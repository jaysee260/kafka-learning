"""Stage 2: analytics service. Keeps running revenue totals.

    python -m shop.consumers.analytics

Prints a small report every 10 orders. Like inventory, state is in memory:
stage 4 has you rebuild it from scratch by resetting this group's offsets.
"""

from collections import Counter

from shop.consumer import run_consumer
from shop.events import cents

REPORT_EVERY = 10

order_count = 0
revenue_cents = 0
revenue_by_customer: Counter[str] = Counter()
units_by_sku: Counter[str] = Counter()


def handle_order(order: dict, msg) -> None:
    global order_count, revenue_cents
    order_count += 1
    revenue_cents += order["total_cents"]
    revenue_by_customer[order["customer_id"]] += order["total_cents"]
    for item in order["items"]:
        units_by_sku[item["sku"]] += item["qty"]

    if order_count % REPORT_EVERY == 0:
        top_customer, top_spend = revenue_by_customer.most_common(1)[0]
        top_sku, top_units = units_by_sku.most_common(1)[0]
        print(
            f"--- {order_count} orders | revenue {cents(revenue_cents)} | "
            f"top customer {top_customer} ({cents(top_spend)}) | "
            f"best seller {top_sku} ({top_units} units) ---"
        )


if __name__ == "__main__":
    run_consumer("analytics", handle_order)
