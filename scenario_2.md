# Scenario-Based Interview Question #2
## Duplicate Order Creation in Production

### Scenario

You are providing production support for an **Order Management Application**.

A client reports that a single customer order has been created **three times**, even though the customer clicked the **Place Order** button only once.

The issue has affected approximately **200 customers during the last hour**.

Duplicate orders have also caused:

- Duplicate payment processing
- Duplicate shipments

---

## System Architecture

```text
Client
   |
API Gateway
   |
Order Service
   |
Kafka
   |
   +-------------------+-------------------+
   |                   |                   |
Inventory Service  Payment Service   Shipping Service
```

## Interview Question

How would you investigate this production issue, contain the immediate impact, identify the root cause, and design a permanent solution to prevent duplicate orders, payments, and shipments?

---

# Answer

This is primarily an **idempotency and distributed-systems reliability problem**.

I would approach it in three stages:

1. **Immediate production containment**
2. **Root-cause investigation**
3. **Permanent architectural fix**

---

# 1. Immediate Production Containment

The first priority is preventing further customer and financial impact.

I would:

- Temporarily prevent duplicate payment and shipment processing.
- Identify affected orders using:
  - `orderId`
  - `customerId`
  - `requestId`
  - `correlationId`
  - timestamps
- Determine the canonical order for each customer.
- Cancel duplicate orders.
- Reverse/refund duplicate payments where necessary.
- Stop duplicate shipments that haven't been dispatched.
- Preserve application logs and Kafka information for investigation.

---

# 2. Investigate Where Duplication Started

Even if the customer clicked **Place Order** only once, the request may have been processed multiple times.

Possible causes include:

- API Gateway retries
- Client retry after network timeout
- Load balancer retry
- Order Service retry
- Kafka redelivery
- Kafka consumer restart
- Consumer processed event but failed before committing Kafka offset
- Order Service published the same event multiple times

I would trace one affected order end-to-end using:

```text
requestId
correlationId
idempotencyKey
orderId
eventId
paymentId
shipmentId
```

Then investigate:

```text
Did API Gateway retry?
        |
        v
Did Order Service receive POST multiple times?
        |
        v
Were multiple order rows created?
        |
        v
Was ORDER_CREATED published multiple times?
        |
        v
Was the same Kafka event delivered multiple times?
        |
        v
Did Payment Service process the event repeatedly?
        |
        v
Did Shipping Service process the event repeatedly?
```

---

# 3. Make Order Creation Idempotent

The client should generate an **Idempotency Key** for every logical Place Order operation.

Example:

```http
POST /orders

Idempotency-Key: 7ad63f80-0b17-4ea1-98f2-82306e893612
```

If the client retries the same request, it must send the **same idempotency key**.

The Order Service stores this key.

Example:

```sql
CREATE TABLE order_request (
    idempotency_key VARCHAR(100) NOT NULL,
    order_id UUID NOT NULL,
    status VARCHAR(30) NOT NULL,

    CONSTRAINT uq_order_idempotency_key
        UNIQUE (idempotency_key)
);
```

---

# 4. Do Not Depend Only on Application-Level Checks

This is unsafe:

```java
if (!repository.existsByIdempotencyKey(key)) {
    repository.save(order);
}
```

Because two requests could arrive simultaneously.

```text
Request A                      Request B

exists(key)? NO                exists(key)? NO
      |                              |
      v                              v
save(order)                    save(order)
```

Both requests observed that the record didn't exist.

This is a classic **check-then-act race condition**.

Instead, enforce:

```text
UNIQUE(idempotency_key)
```

at the database level.

If two requests arrive simultaneously:

```text
Request A
    |
    +---- INSERT abc123 ---- SUCCESS

Request B
    |
    +---- INSERT abc123 ---- UNIQUE CONSTRAINT FAILURE
```

Request B can then retrieve and return the already-created order.

The **database unique constraint is the final concurrency guard**.

---

# 5. Kafka Consumers Must Be Idempotent

Fixing only the Order Service isn't enough.

With an at-least-once messaging model, consumers must assume:

> The same message may be delivered more than once.

Every event should have a unique `eventId`.

Example:

```json
{
  "eventId": "evt-123",
  "orderId": "order-456",
  "type": "ORDER_CREATED"
}
```

Each consumer can maintain a processed-event table.

```sql
CREATE TABLE processed_event (
    event_id VARCHAR(100) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL
);
```

Consumers atomically claim an event before performing the business operation.

Example:

```sql
INSERT INTO processed_event(
    event_id,
    processed_at
)
VALUES (
    ?,
    CURRENT_TIMESTAMP
)
ON CONFLICT DO NOTHING;
```

Only the consumer invocation that successfully claims the event should continue.

This should independently exist in:

```text
Inventory Service
Payment Service
Shipping Service
```

---

# 6. Payment Requires Additional Protection

Payments are financially sensitive, so they should have another idempotency layer.

For example:

```text
payment:{orderId}
```

or:

```sql
UNIQUE(order_id, payment_type)
```

Therefore, even if the same Kafka event is processed multiple times:

```text
ORDER_CREATED
ORDER_CREATED
ORDER_CREATED
```

only:

```text
1 Payment
```

is created.

If the external payment provider supports idempotency keys, the Payment Service should also send the same payment idempotency key when retrying the payment request.

---

# 7. Shipping Should Also Be Idempotent

Shipping should have business-level protection as well.

For example:

```sql
UNIQUE(order_id)
```

if the business rule allows only one shipment per order.

Therefore:

```text
Same event delivered 3 times
        |
        v
Shipping Service
        |
        v
1 shipment
```

---

# 8. Use the Transactional Outbox Pattern

Another potential issue exists between:

```text
Order DB Transaction
        +
Kafka Publish
```

Naive implementation:

```java
orderRepository.save(order);

kafkaTemplate.send(
    "orders",
    event
);
```

These are two separate operations.

Failures can create inconsistent states.

For example:

```text
Order saved
    |
Application crashes
    |
Kafka event never published
```

or retry logic could cause the event to be published multiple times.

A better solution is the **Transactional Outbox Pattern**.

Inside one database transaction:

```text
BEGIN

INSERT INTO orders(...);

INSERT INTO outbox_events(
    event_id,
    aggregate_id,
    event_type,
    payload
);

COMMIT
```

Conceptually:

```java
@Transactional
public Order createOrder(CreateOrderCommand command) {

    Order order = orderRepository.save(
        Order.create(command)
    );

    OutboxEvent event = OutboxEvent.of(
        UUID.randomUUID(),
        order.getId(),
        "ORDER_CREATED",
        serializer.serialize(order)
    );

    outboxRepository.save(event);

    return order;
}
```

Now:

```text
Order persisted
       AND
Event scheduled for publication
```

occur in the **same database transaction**.

A separate publisher reads the outbox table and publishes events to Kafka.

---

# 9. Consumers Still Need Idempotency

The Transactional Outbox Pattern does **not** mean duplicate delivery becomes impossible.

The publisher itself may retry publishing.

Therefore:

```text
Transactional Outbox
        |
        v
Kafka
        |
        | evt-123
        | evt-123
        | evt-123
        v
Consumer
```

The consumer must still recognize:

```text
evt-123 already processed
```

and ignore the duplicate.

---

# 10. Final Architecture

```text
                    Idempotency-Key
Client -----------------------------------+
                                          |
                                          v
                                    API Gateway
                                          |
                                          v
                                +------------------+
                                |  Order Service   |
                                +------------------+
                                          |
                                   DB Transaction
                                    /          \
                                   v            v
                                Order         Outbox
                                                |
                                                v
                                              Kafka
                                                |
                   +----------------------------+----------------------------+
                   |                            |                            |
                   v                            v                            v
              Inventory                     Payment                      Shipping
              Consumer                      Consumer                     Consumer
                   |                            |                            |
                   v                            v                            v
             Event Dedup                  Event Dedup                  Event Dedup
                   |                            |                            |
                   v                            v                            v
            DB Constraint                DB Constraint                DB Constraint
```

---

# 11. Example Production Failure

Suppose logs show:

```text
10:01:01 POST /orders
10:01:31 Request timed out

10:01:32 POST /orders
10:02:02 Request timed out

10:02:03 POST /orders
```

Without idempotency:

```text
Request 1 -> Order A
Request 2 -> Order B
Request 3 -> Order C
```

Result:

```text
3 Orders
3 Payments
3 Shipments
```

With idempotency, all requests contain:

```text
Idempotency-Key: abc123
```

Therefore:

```text
Request 1
   |
   +---- abc123 ----> Create Order

Request 2
   |
   +---- abc123 ----> Existing Order

Request 3
   |
   +---- abc123 ----> Existing Order
```

Result:

```text
1 Order
1 Payment
1 Shipment
```

---

# Key Interview Statement

> Retries are normal in distributed systems. We should not try to eliminate every retry. Instead, we should design operations to be idempotent so that retrying the same logical operation produces the same business result.

---

# Final Solution Summary

```text
API Layer
    |
    +-- Idempotency key
    +-- Same key reused during retries

Order Service
    |
    +-- Database unique constraint
    +-- Atomic order creation
    +-- Transactional Outbox

Kafka
    |
    +-- Assume duplicate delivery is possible
    +-- Unique eventId

Consumers
    |
    +-- Event deduplication
    +-- Atomic event claiming

Payment
    |
    +-- Payment idempotency key
    +-- Business-level unique constraint

Shipping
    |
    +-- Business-level unique constraint

Observability
    |
    +-- correlationId
    +-- requestId
    +-- orderId
    +-- eventId
```

---

# Important Follow-Up Question

## What if two requests with the same idempotency key arrive at exactly the same time?

The database unique constraint acts as the final concurrency guard.

One transaction successfully creates the record.

The other transaction receives a uniqueness conflict and retrieves the already-created order.

Therefore:

```text
Application-level exists() check
              !=
Concurrency protection
```

The database constraint provides the actual guarantee.