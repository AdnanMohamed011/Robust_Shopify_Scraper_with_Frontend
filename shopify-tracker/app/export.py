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
from .models import Product

SHOPIFY_CSV_COLUMNS = [
    "Handle", "Title", "Body (HTML)", "Vendor", "Tags", "Published",
    "Option1 Name", "Option1 Value",
    "Option2 Name", "Option2 Value",
    "Option3 Name", "Option3 Value",
    "Variant SKU", "Variant Inventory Tracker", "Variant Inventory Qty",
    "Variant Inventory Policy", "Variant Fulfillment Service",
    "Variant Price", "Variant Requires Shipping", "Variant Taxable",
    "Image Src", "Image Position", "Status",
]


def _fallback_handle(product: Product) -> str:
    """If a store's JSON didn't expose a handle for some reason, derive
    a stable one from the Shopify product id so rows still group correctly."""
    return product.handle or f"product-{product.shopify_product_id}"


def build_shopify_csv(products: list) -> str:
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
        for i, p in enumerate(variants):
            is_first_row = (i == 0)

            # Inventory: only write a real number if the store actually
            # exposed one. Otherwise leave it blank rather than guessing —
            # Shopify treats a blank qty as "don't track inventory" for
            # that row, which is safer than inventing a fake count.
            qty = p.inventory_qty if p.inventory_qty is not None else (0 if not p.available else "")

            writer.writerow({
                "Handle": handle,
                "Title": first.title if is_first_row else "",
                "Body (HTML)": (first.body_html or "") if is_first_row else "",
                "Vendor": (first.vendor or "") if is_first_row else "",
                "Tags": (first.tags or "") if is_first_row else "",
                "Published": "TRUE" if is_first_row else "",
                "Option1 Name": p.option1_name or "",
                "Option1 Value": p.option1_value or "",
                "Option2 Name": p.option2_name or "",
                "Option2 Value": p.option2_value or "",
                "Option3 Name": p.option3_name or "",
                "Option3 Value": p.option3_value or "",
                "Variant SKU": p.sku or "",
                "Variant Inventory Tracker": "shopify" if p.inventory_qty is not None else "",
                "Variant Inventory Qty": qty,
                "Variant Inventory Policy": "deny",
                "Variant Fulfillment Service": "manual",
                "Variant Price": f"{p.price:.2f}",
                "Variant Requires Shipping": "TRUE",
                "Variant Taxable": "TRUE",
                "Image Src": (first.image_url or "") if is_first_row else "",
                "Image Position": "1" if (is_first_row and first.image_url) else "",
                "Status": "active" if is_first_row else "",
            })

    return buffer.getvalue()
