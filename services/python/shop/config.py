"""Shared settings: where the broker lives and what the topics are called.

Every service imports from here so topic names never drift apart.
Override the broker address with the BOOTSTRAP_SERVERS environment variable.
"""

import os

# The "external" listener from docker-compose.yml. A client only needs one
# broker address to start; it discovers the rest of the cluster from there.
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")

# Topics used across the stages.
ORDERS_TOPIC = "orders"            # Stage 1+: OrderPlaced events
PAYMENTS_TOPIC = "payments"        # Stage 5: PaymentReceived events
ORDERS_DLQ_TOPIC = "orders.dlq"    # Stage 4: messages we could not process

# How many partitions `orders` gets. More partitions = more consumers can
# share the work in one consumer group (stage 3). 3 is plenty for learning.
ORDERS_PARTITIONS = 3
