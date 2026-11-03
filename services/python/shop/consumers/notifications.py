"""Stage 2: notification service. "Emails" each customer a confirmation.

    python -m shop.consumers.notifications

A real version would call an email provider. Think about what happens if
that call succeeds but the service crashes before the offset is committed:
the customer gets the email twice. Stage 4 digs into this.
"""

from shop.consumer import run_consumer
from shop.events import cents


def handle_order(order: dict, msg) -> None:
    lines = ", ".join(f"{i['qty']}x {i['name']}" for i in order["items"])
    print(
        f"To: {order['customer_id']}@example.com | "
        f"Thanks for order {order['order_id'][:8]}: {lines} ({cents(order['total_cents'])})"
    )


if __name__ == "__main__":
    run_consumer("notifications", handle_order)
