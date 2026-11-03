"""Event shapes and a generator for fake orders.

Events are plain dicts serialized as JSON. JSON is easy to read in Redpanda
Console; a later exercise swaps it for a schema (Avro/Protobuf) so producers
and consumers agree on the shape formally.
"""

import json
import random
import uuid
from datetime import datetime, timezone

CUSTOMERS = ["alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"]

# sku -> (name, unit price in cents). Money is kept in integer cents to avoid
# floating point rounding surprises.
CATALOG = {
    "SKU-MUG": ("Coffee mug", 1200),
    "SKU-TEE": ("T-shirt", 2500),
    "SKU-HAT": ("Baseball cap", 1800),
    "SKU-STK": ("Sticker pack", 500),
    "SKU-BAG": ("Tote bag", 3000),
}


def make_order_placed() -> dict:
    """Build one random OrderPlaced event."""
    items = []
    for sku in random.sample(list(CATALOG), k=random.randint(1, 3)):
        name, price = CATALOG[sku]
        items.append({"sku": sku, "name": name, "qty": random.randint(1, 4), "unit_price_cents": price})

    return {
        "type": "OrderPlaced",
        "order_id": str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMERS),
        "items": items,
        "total_cents": sum(i["qty"] * i["unit_price_cents"] for i in items),
        "placed_at": datetime.now(timezone.utc).isoformat(),
    }


def serialize(event: dict) -> bytes:
    """Kafka only stores bytes. We decide how to turn objects into bytes."""
    return json.dumps(event).encode("utf-8")


def deserialize(raw: bytes) -> dict:
    """The inverse of serialize(). Raises ValueError on malformed input."""
    return json.loads(raw.decode("utf-8"))


def cents(amount: int) -> str:
    return f"${amount / 100:,.2f}"
