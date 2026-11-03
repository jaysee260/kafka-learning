"""Stage 2: inventory service. Decrements stock for every item ordered.

    python -m shop.consumers.inventory

Stock lives in memory, so it resets when the service restarts. That is
deliberate: in stage 4 you will rebuild state by replaying the topic.
"""

from shop.consumer import run_consumer
from shop.events import CATALOG

STARTING_STOCK = 200
LOW_STOCK = 20

stock = {sku: STARTING_STOCK for sku in CATALOG}


def handle_order(order: dict, msg) -> None:
    for item in order["items"]:
        sku = item["sku"]
        stock[sku] -= item["qty"]
        flag = "  LOW STOCK" if stock[sku] <= LOW_STOCK else ""
        print(f"[p{msg.partition()} @{msg.offset()}] {sku} -{item['qty']} -> {stock[sku]} left{flag}")


if __name__ == "__main__":
    run_consumer("inventory", handle_order)
