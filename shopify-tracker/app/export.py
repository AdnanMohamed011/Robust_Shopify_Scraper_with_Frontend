"""
Exports tracked products as a CSV matching Shopify's official product
import format, so the file can be uploaded directly into a store's
Shopify admin (Products > Import).

Key convention Shopify's importer relies on: rows are grouped by
`Handle`. Only the FIRST row for a given handle should carry the full
product info (Title, Body HTML, Vendor, Tags, Image Src). Every
subsequent row for the same handle should leave those blank and only
carry the option values + variant-specific fields (SKU, price, qty) —
that's what tells Shopify "this is another variant of the same
product" rather than a separate product.
"""
import csv
import io
import math
from .models import Product
from .scraper import looks_like_size


def round_to_99(price: float) -> float:
    """
    Charm-pricing: always rounds UP to the nearest whole number minus a
    cent, e.g. 35.85 -> 35.99, 35.00 -> 35.99, 35.99 -> 35.99 (unchanged).
    Applied after currency conversion (and after any margin -- see
    apply_margin), since the final, fully-adjusted price is what actually
    needs cleaning up into a normal storefront price.
    """
    return math.floor(price) + 0.99


def apply_margin(price: float, margin_percent: float) -> float:
    """
    Applies a flat percentage markup on top of an already FX-converted
    price, e.g. apply_margin(100, 15) -> 115. margin_percent=0 (or None)
    is a no-op -- a direct pass-through of the source price, unchanged
    from before this existed. Always applied BEFORE round_to_99, so the
    charm-price rounding is the last step no matter what margin is set,
    same as it already is for the FX conversion.
    """
    return price * (1 + (margin_percent or 0) / 100)


def resolve_quantity(product: Product) -> int:
    """
    Decides what stock quantity to actually push/export for a product,
    trusting `available` over a literal 0 in `inventory_qty`.

    Some source storefronts DO publicly expose inventory_quantity, but
    always report 0 regardless of real stock -- common when a store has
    "continue selling when out of stock" on, or just doesn't want to show
    exact counts. `available` is the field that's actually reliable there.
    Blindly using inventory_qty whenever it's not None (the previous
    logic) meant a store like that would show as 100% sold out downstream
    even though every product is genuinely purchasable -- available=True
    but inventory_qty=0 should NOT mean "push 0 stock".

    - available=False -> always 0, regardless of any inventory_qty value.
    - available=True + a real positive inventory_qty -> use that exact number.
    - available=True + inventory_qty is None or 0 -> push 1 (in stock,
      exact count unknown/not meaningfully exposed).
    """
    if not product.available:
        return 0
    if product.inventory_qty is not None and product.inventory_qty > 0:
        return product.inventory_qty
    return 1

SHOPIFY_CSV_COLUMNS = [
    "Handle", "Title", "Body (HTML)", "Vendor", "Type", "Tags", "Published",
    "Option1 Name", "Option1 Value",
    "Option2 Name", "Option2 Value",
    "Option3 Name", "Option3 Value",
    "Variant SKU", "Variant Grams", "Variant Inventory Tracker", "Variant Inventory Qty",
    "Variant Inventory Policy", "Variant Fulfillment Service",
    "Variant Price", "Variant Requires Shipping", "Variant Taxable",
    "Image Src", "Image Position", "Status",
]


def _fallback_handle(product: Product) -> str:
    """If a store's JSON didn't expose a handle for some reason, derive
    a stable one from the Shopify product id so rows still group correctly."""
    return product.handle or f"product-{product.shopify_product_id}"


def _has_size_option(p: Product) -> bool:
    return any(
        looks_like_size(v)
        for v in (p.option1_value, p.option2_value, p.option3_value)
    )


def filter_sized_only(products: list) -> list:
    """Keep only products that have a real size variant (ready-to-wear /
    stitched), dropping unstitched/fabric-only listings that don't come
    with sizes. Filtering is per-handle (product), not per-variant: if any
    variant of a product carries a size, all of that product's variant
    rows are kept so the CSV import still groups correctly."""
    handles_with_size = set()
    by_handle = {}
    for p in products:
        handle = _fallback_handle(p)
        by_handle.setdefault(handle, []).append(p)
        if _has_size_option(p):
            handles_with_size.add(handle)

    return [p for p in products if _fallback_handle(p) in handles_with_size]


def build_shopify_csv(products: list, fx_rate: float = 1.0, margin_percent: float = 0.0) -> str:
    """
    fx_rate converts each product's stored price (in the SOURCE store's
    own currency, e.g. PKR) into your Shopify store's currency before
    it's written out, so a Gul Ahmed price in PKR doesn't land in your
    store as if it were the same number in USD/AED/whatever you sell in.
    Pass 1.0 (the default) to export raw, unconverted prices.

    margin_percent adds a flat percentage markup on top of that converted
    price (see apply_margin) before the .99 charm-price rounding. Pass 0.0
    (the default) for a direct price match with no markup.
    """
    # Group variant rows by product handle, preserving first-seen order,
    # so all variants of one product end up adjacent in the file.
    groups = {}
    for p in products:
        groups.setdefault(_fallback_handle(p), []).append(p)

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=SHOPIFY_CSV_COLUMNS)
    writer.writeheader()

    for handle, variants in groups.items():
        first = variants[0]

        base_tags = [t.strip() for t in (first.tags or "").split(",") if t.strip()]
        if first.piece_count and first.piece_count not in base_tags:
            base_tags.append(first.piece_count)
        tags_out = ", ".join(base_tags)

        for i, p in enumerate(variants):
            is_first_row = (i == 0)

            # Inventory: trust `available` over a literal 0 in
            # inventory_qty (see resolve_quantity's docstring -- some
            # storefronts report 0 for every product regardless of real
            # stock). Leave blank (not 0) when available but the exact
            # count genuinely isn't known -- Shopify treats a blank qty as
            # "don't track inventory" for that row, safer than either
            # inventing a fake positive count or wrongly writing 0.
            if not p.available:
                qty = 0
            elif p.inventory_qty is not None and p.inventory_qty > 0:
                qty = p.inventory_qty
            else:
                qty = ""

            writer.writerow({
                "Handle": handle,
                "Title": first.title if is_first_row else "",
                "Body (HTML)": (first.body_html or "") if is_first_row else "",
                "Vendor": (first.vendor or "") if is_first_row else "",
                "Type": (first.product_type or "") if is_first_row else "",
                "Tags": tags_out if is_first_row else "",
                "Published": "TRUE" if is_first_row else "",
                "Option1 Name": p.option1_name or "",
                "Option1 Value": p.option1_value or "",
                "Option2 Name": p.option2_name or "",
                "Option2 Value": p.option2_value or "",
                "Option3 Name": p.option3_name or "",
                "Option3 Value": p.option3_value or "",
                "Variant SKU": p.sku or "",
                "Variant Grams": int(p.weight_grams) if p.weight_grams is not None else "",
                "Variant Inventory Tracker": "shopify" if p.inventory_qty is not None else "",
                "Variant Inventory Qty": qty,
                "Variant Inventory Policy": "deny",
                "Variant Fulfillment Service": "manual",
                "Variant Price": f"{round_to_99(apply_margin(p.price * fx_rate, margin_percent)):.2f}",
                "Variant Requires Shipping": "TRUE",
                "Variant Taxable": "TRUE",
                "Image Src": (first.image_url or "") if is_first_row else "",
                "Image Position": "1" if (is_first_row and first.image_url) else "",
                "Status": "active" if is_first_row else "",
            })

        # Shopify's import format supports multiple images per product by
        # adding extra rows that carry ONLY Handle + Image Src + Image
        # Position, with everything else blank. The first image already
        # went out above (as part of the first variant row) — this adds
        # rows 2, 3, 4... for any additional images the product has.
        extra_images = []
        if first.all_image_urls:
            extra_images = [u.strip() for u in first.all_image_urls.split(",") if u.strip()]
        # Skip the first URL since it's already been written as the primary image.
        for position, image_url in enumerate(extra_images[1:], start=2):
            blank_row = {col: "" for col in SHOPIFY_CSV_COLUMNS}
            blank_row["Handle"] = handle
            blank_row["Image Src"] = image_url
            blank_row["Image Position"] = str(position)
            writer.writerow(blank_row)

    return buffer.getvalue()