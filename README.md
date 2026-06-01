# PayStream

Event-driven payment processing built on Apache Kafka.

Four independent microservices communicate exclusively through Kafka topics —
no shared code, no shared build, no direct calls between services.

## Topic flow

```
payment-gateway
      │
      └──▶  payment.requested
                    │
                    └──▶  fraud-detector
                                │
                                ├──▶  payment.authorized
                                │           │
                                │           └──▶  ledger
                                │                   │
                                │                   ├──▶  ledger.updated
                                │                   │           │
                                │                   │           └──▶  notification
                                │                   │
                                │                   └──▶  account.balance
                                │
                                └──▶  payment.flagged
```

## Services

| Service | Role | Consumes | Produces |
|---|---|---|---|
| `payment-gateway` | Entry point — accepts payment requests | — | `payment.requested` |
| `fraud-detector` | Evaluates each payment for fraud | `payment.requested` | `payment.authorized`, `payment.flagged` |
| `ledger` | Writes authorised transactions and updates balances | `payment.authorized` | `ledger.updated`, `account.balance` |
| `notification` | Dispatches notifications on completed transactions | `ledger.updated` | — |

## Prerequisites

- Docker & Docker Compose
- JDK 21
- Maven 3.9+

## Quick start

### 1 — Start Kafka and Kafka UI

```bash
# from the repo root
docker compose up -d

# broker is healthy when this returns without error
docker compose ps
```

Kafka UI is available at **http://localhost:8080**

To stop everything:

```bash
docker compose down
```

### 2 — Build a service (fat jar)

Each service is a fully self-contained Maven project. Run from inside its directory:

```bash
cd payment-gateway
mvn package -DskipTests
# fat jar → target/app.jar
```

Repeat for `fraud-detector`, `ledger`, `notification`.

### 3 — Run a service on the host

```bash
# payment-gateway
java -jar payment-gateway/target/app.jar

# fraud-detector
java -jar fraud-detector/target/app.jar

# ledger
java -jar ledger/target/app.jar

# notification
java -jar notification/target/app.jar
```

Host services connect to Kafka at `localhost:9092`.

### 4 — Build and run with Docker

Build each image from its own directory (images are independent):

```bash
docker build -t paystream/payment-gateway   payment-gateway/
docker build -t paystream/fraud-detector    fraud-detector/
docker build -t paystream/ledger            ledger/
docker build -t paystream/notification      notification/
```

Run a service inside the same Docker network as the broker:

```bash
docker run --rm --network paystream_default \
  -e BOOTSTRAP_SERVERS=kafka:29092 \
  paystream/payment-gateway
```

> **Note:** the compose network name is `paystream_default` by default (folder name + `_default`).
> Containerised services should connect to `kafka:29092` (internal listener).

## Project layout

```
paystream/
├── docker-compose.yml          # Kafka (KRaft) + Kafka UI
├── payment-gateway/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/paystream/gateway/PaymentGatewayApp.java
├── fraud-detector/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/paystream/fraud/FraudDetectorApp.java
├── ledger/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/paystream/ledger/LedgerApp.java
└── notification/
    ├── pom.xml
    ├── Dockerfile
    └── src/main/java/com/paystream/notification/NotificationApp.java
```

## Stack

| Component | Version |
|---|---|
| Apache Kafka | 4.0.0 (KRaft, no ZooKeeper) |
| kafka-clients | 4.0.0 |
| Jackson | 2.18.3 |
| SLF4J Simple | 2.0.17 |
| Java | 21 |
| Maven | 3.9 |
