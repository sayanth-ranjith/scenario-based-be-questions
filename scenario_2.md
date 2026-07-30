# Scenario-Based Interview Question #2  
## Duplicate Order Creation in Production

## Scenario

You are providing production support for an order management application.

A client reports that a single customer order has been created three times, even though the customer clicked the **Place Order** button only once.

The issue has affected approximately 200 customers during the last hour.

Duplicate orders have also caused duplicate payment processing and duplicate shipments.

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
Inventory Service
   |
Payment Service
   |
Shipping Service
