# 💳 CorePay — MSA 기반 결제 시스템

> **Java 21 · Spring Boot 4 · Kafka · Redis · MySQL · Docker**
> 실무 수준의 마이크로서비스 아키텍처를 직접 설계하고 구현한 백엔드 포트폴리오 프로젝트입니다.

---

## 📌 프로젝트 개요

CorePay는 MSA(Microservices Architecture) 패턴을 적용한 결제 시스템입니다.
사용자 인증, 상품 관리, 주문, 결제 등 실제 커머스 도메인을 독립된 서비스로 분리하여 구성하였으며,
서비스 간 통신은 **Apache Kafka** 이벤트 스트리밍을 중심으로 사용합니다.

> 💡 **[v2 아키텍처 변경]**
> - **Auth Service → User Service 통합**: 회원 관리와 JWT 인증을 단일 서비스에서 처리. `UserRegisterUseCase`가 `@Transactional` 하나로 User + Auth DB를 동시 저장하고 Kafka 이벤트 발행을 제거.
> - **Order Service OpenFeign 제거**: Product Service 동기 호출 대신 **Kafka 이벤트로 동기화된 ProductSnapshot(Redis 캐시 + 로컬 DB)**을 조회하는 방식으로 대체.

> 💡 **[v3 아키텍처 변경]**
> - **Outbox 패턴 도입**: Kafka 발행과 DB 저장을 동일 트랜잭션으로 묶어 이벤트 유실 원천 차단. 즉시 발행 성공 시 `SENT` 처리, 실패 시 `OutboxScheduler`가 30초마다 `PENDING` 이벤트를 재발행하는 이중 안전망 구조.
> - **MDC 분산 트레이싱**: `X-Trace-Id` 헤더를 HTTP → Kafka → 비동기 스레드까지 전파하여 서비스 간 요청 흐름을 단일 `traceId`로 추적.
> - **corepay-common 1.1.1**: `OutboxEvent`, `OutboxScheduler`, `OutboxEventPublisher`, `KafkaMdcHelper`, `MdcLoggingFilter`, `MdcTaskDecorator` 공통 라이브러리로 제공.

| 항목 | 내용 |
|---|---|
| 언어 | Java 21 (Virtual Threads 지원) |
| 프레임워크 | Spring Boot 4.0.3 / Spring Cloud 2025.1.1 |
| 메시지 브로커 | Apache Kafka |
| 캐시 | Redis |
| DB | MySQL (Flyway 마이그레이션 관리) |
| 모니터링 | Spring Actuator + Micrometer + Prometheus / Grafana |
| 공통 라이브러리 | corepay-common 1.1.1 (내부 Maven 배포) |
| 회복 탄력성 | Resilience4j (Circuit Breaker) |
| 이벤트 안정성 | Outbox Pattern (즉시 발행 + 스케줄러 안전망) |
| 분산 추적 | MDC + X-Trace-Id 전파 (HTTP / Kafka / 비동기) |

---

## 🏗️ 시스템 아키텍처

```mermaid
flowchart TD
    Client(["🌐 Client\nBrowser / Mobile"])

    subgraph GW["🚪 API Gateway\n(Spring Cloud Gateway · WebFlux · Spring Boot 3.3.5)"]
        FILTER["JWT 인증 필터\nGlobalFilter"]
        ROUTER["라우팅\n/user/** /product/** /order/**"]
        FILTER --> ROUTER
    end

    Client -->|"HTTP Request"| FILTER

    subgraph USER["👤 User Service (Auth 통합)"]
        U1["회원가입\nPOST /api/users/signup\nUserRegisterUseCase @Transactional"]
        U2["UserService.creat()\n회원 DB 저장 (BCrypt)"]
        U3["AuthService.signup()\n인증용 AuthUser DB 저장"]
        U4["로그인\nPOST /api/auth/login\nJWT Access Token 발급"]
        U5["비밀번호 변경\nPOST /api/auth/update_password\n직접 DB 업데이트 (이벤트 없음)"]
        U6[("MySQL\n회원 DB + 인증 DB")]
        U1 --> U2
        U1 --> U3
        U2 --> U6
        U3 --> U6
        U4 --> U6
        U5 --> U6
    end

    subgraph PRODUCT["🛍️ Product Service"]
        P1["상품 등록 · 조회\nRedis Cache"]
        P2["재고 관리\nKafka Consumer/Producer"]
        P3["상품 생성 이벤트\nOutbox → product-created-topic"]
        P4[("MySQL\n상품 DB")]
        P5[("⚡ Redis\n상품 캐시\nSTOCK_TTL / DUPLICATE_TTL")]
        P6["Resilience4j\nCircuit Breaker"]
        POUT[("MySQL\nOutbox DB")]
        P1 --> P4
        P1 <--> P5
        P2 --> P4
        P3 --> POUT
        P6 --> P2
    end

    subgraph ORDER["📋 Order Service"]
        O1["주문 생성\nPOST /api/orders"]
        O2["ProductSnapshotService\nRedis 요약본 조회\n미스 시 ProductSnapshot DB"]
        O3["주문 이벤트\nOutbox → order-created-topic"]
        O4["Kafka Consumer\npayment-completed/failed/cancelled-topic"]
        O5[("MySQL\n주문 DB")]
        O6[("MySQL\nProductSnapshot DB")]
        O7[("⚡ Redis\nProductSnapshot 캐시")]
        OOUT[("MySQL\nOutbox DB")]
        O1 --> O2
        O2 <--> O7
        O2 --> O6
        O1 --> O3
        O3 --> OOUT
        O4 --> O5
        O1 --> O5
    end

    subgraph PAYMENT["💰 Payment Service"]
        PA1["결제 처리\nKafka Consumer"]
        PA2["결제 결과 이벤트\nOutbox → payment-completed/failed/cancelled-topic"]
        PA3[("MySQL\n결제 DB")]
        PA4[("⚡ Redis\n멱등성 처리\n중복 결제 방지")]
        PA5["Resilience4j\nCircuit Breaker"]
        PAOUT[("MySQL\nOutbox DB")]
        PA1 --> PA3
        PA1 <--> PA4
        PA1 --> PA2
        PA2 --> PAOUT
        PA5 --> PA1
    end

    subgraph OUTBOX["📤 Outbox 안전망 (corepay-common)"]
        PUB["OutboxEventPublisher\n즉시 발행 시도 → SENT\n실패 시 PENDING 유지"]
        SCHED["OutboxScheduler\n30초마다 PENDING 재발행"]
        SCHED --> PUB
    end

    subgraph KAFKA["📨 Apache Kafka"]
        K1["product-created-topic\n상품 생성 시 Order에 ProductSnapshot 동기화"]
        K3["order-created-topic"]
        K5["payment-completed-topic"]
        K6["payment-failed-topic"]
        K7["payment-cancelled-topic"]
        K8["stock-increase-topic\n재고 복구 보상 트랜잭션"]
        K9["order-cancel-topic"]
    end

    subgraph INFRA["📊 Observability"]
        MON["Prometheus + Grafana\n메트릭 수집 · 대시보드"]
        TRACE["X-Trace-Id 헤더 전파\nMDC 로깅 · KafkaMdcHelper\nHTTP / Kafka / 비동기 스레드"]
    end

    %% Gateway 라우팅
    ROUTER -->|"/user/**"| USER
    ROUTER -->|"/product/**"| PRODUCT
    ROUTER -->|"/order/**"| ORDER

    %% Outbox → Kafka 발행
    POUT -->|"즉시 발행 or 재발행"| PUB
    OOUT -->|"즉시 발행 or 재발행"| PUB
    PAOUT -->|"즉시 발행 or 재발행"| PUB
    PUB -->|"publish"| K1
    PUB -->|"publish"| K3
    PUB -->|"publish"| K5
    PUB -->|"publish"| K6
    PUB -->|"publish"| K7

    %% 상품 생성 → ProductSnapshot 동기화
    K1 -->|"consume 요약본 저장"| O2

    %% 주문 → 결제 플로우
    K3 -->|"consume"| PA1
    K3 -->|"consume 재고 차감"| P2

    %% 보상 트랜잭션 Saga
    K5 -->|"consume 주문 완료"| O4
    K6 -->|"consume 주문 취소"| O4
    K7 -->|"consume 주문 환불"| O4
    O4 -->|"publish 재고 복구"| K8
    P2 -->|"재고 부족 시 publish"| K9
    K9 -->|"consume 주문 취소"| O4
    K8 -->|"consume 재고 복구"| P2

    %% 모니터링
    USER & PRODUCT & ORDER & PAYMENT -.->|"메트릭 노출"| MON
    USER & PRODUCT & ORDER & PAYMENT -.->|"Trace ID 전파"| TRACE
```

---

## 📦 서비스 목록

### 👤 User Service (Auth 통합)
> **역할**: 회원 가입/정보 관리, JWT 발급·검증, Spring Security 기반 인증 처리

- **[Auth 통합]** `UserRegisterUseCase.register()`가 `@Transactional` 하나로 `UserService.creat()` + `AuthService.signup()`을 순차 호출 → 회원 DB와 인증 DB를 동시 저장, **Kafka 발행 없음**
- Spring Security + JWT (`jjwt 0.12.x`): `POST /api/auth/login` 로그인 시 Access Token 직접 발급
- 비밀번호 변경(`POST /api/auth/update_password`)은 Auth DB를 직접 업데이트, Kafka 미사용
- Flyway로 DB 스키마 버전 관리

> 💡 **통합 이유**: 이전에는 Auth Service가 별도 서비스로 존재하여 Kafka `user-created-topic`을 통해 인증 데이터를 동기화해야 했습니다. 이벤트 유실이나 순서 역전 시 로그인 불가 상태가 되는 리스크가 있었습니다. User Service에 통합함으로써 회원가입과 인증 데이터가 **동일 트랜잭션**으로 처리되어 데이터 정합성이 보장되고 운영 복잡도가 줄었습니다.

🔗 **[corepay_user 저장소 바로가기](https://github.com/jihoon-68/corepay_user)**

---

### 🛍️ Product Service
> **역할**: 상품 등록/조회/재고 관리, Redis 캐싱으로 조회 성능 최적화

- Redis Cache로 상품 목록/상세 캐싱 (`STOCK_TTL`, `DUPLICATE_TTL` 상수 분리)
- 상품 등록 시 **Outbox 패턴**으로 `product-created-topic` 이벤트 저장 → Order Service가 ProductSnapshot 동기화
- Kafka Consumer/Producer: 재고 차감·복원 이벤트 처리
- Resilience4j Circuit Breaker 적용
- Prometheus 메트릭 노출 (재고 변동 모니터링)
- corepay-common **1.1.1** 적용

🔗 **[corepay_product 저장소 바로가기](https://github.com/jihoon-68/corepay_product)**

---

### 📋 Order Service
> **역할**: 주문 생성 및 상태 관리, ProductSnapshot 기반 상품 정보 조회

- **[OpenFeign 제거]** Product Service 직접 호출 없음 → `ProductSnapshotService`로 **Redis 캐시 조회 → DB Fallback** 방식으로 상품 정보 조회
- Kafka Consumer: `product-created-topic` 수신 → ProductSnapshot 로컬 저장/업데이트
- **Outbox 패턴**: 주문 생성 이벤트를 DB에 먼저 저장 후 즉시 발행 시도 → 실패 시 스케줄러가 재발행
- Kafka Consumer: 결제 완료/실패/취소 이벤트 수신 후 주문 상태 업데이트
- `OrderCancelledEvent`에 `paymentConfirmed` 필드 추가 (결제 확인 여부 구분)
- corepay-common **1.1.1** 적용

🔗 **[corepay_order 저장소 바로가기](https://github.com/jihoon-68/corepay_order)**

---

### 💰 Payment Service
> **역할**: 결제 처리 및 트랜잭션 관리, 결제 이력 저장

- Kafka Consumer: 주문 이벤트 수신 후 결제 로직 실행
- **Outbox 패턴**: 결제 성공/실패/취소 이벤트를 DB에 먼저 저장 후 즉시 발행 시도 → 실패 시 스케줄러가 재발행
- Redis로 멱등성(idempotency) 처리 (중복 결제 방지)
- 결제 실패 응답은 HTTP 200 + 바디로 통일 (402 응답 제거)
- Resilience4j Circuit Breaker로 외부 PG 장애 격리
- Prometheus + Grafana로 결제 성공률 및 처리 시간 모니터링
- corepay-common **1.1.1** 적용

🔗 **[corepay_payment 저장소 바로가기](https://github.com/jihoon-68/corepay_payment)**

---

### 🚪 API Gateway
> **역할**: 단일 진입점(Single Entry Point), JWT 인증 필터, 서비스 라우팅

- Spring Cloud Gateway (Reactive / WebFlux 기반)
- 요청 수신 → JWT 토큰 검증 → 각 마이크로서비스로 라우팅
- Spring Boot 3.3.5 (Gateway 전용 안정화 버전 사용)
- `/auth/**` 라우팅 제거 → 인증 엔드포인트가 `/user/**` 경로로 통합됨

🔗 **[corepay_api_geteway 저장소 바로가기](https://github.com/jihoon-68/corepay_api_geteway)**

---

### 📦 corepay-common (공통 라이브러리 · v1.1.1)
> **역할**: 서비스 간 중복 코드 제거, 공통 인프라 로직 제공

- **Outbox 패턴**: `OutboxEvent`, `OutboxRepository`, `OutboxEventPublisher`, `OutboxScheduler`, `OutboxStatus`
- **MDC 트레이싱**: `KafkaMdcHelper`, `MdcLoggingFilter`, `MdcTaskDecorator`
- Flyway 공통 마이그레이션 SQL (`outbox_events` 테이블 생성 포함)
- 내부 Maven 저장소로 배포하여 각 서비스가 의존성으로 사용

🔗 **[corepay-common 저장소 바로가기](https://github.com/jihoon-68/corepay-common)**

---

## 🔁 주요 흐름 1: 회원가입 → 로그인

```
[회원가입]
1. 클라이언트 → API Gateway → User Service  POST /api/users/signup
2. UserRegisterUseCase.register() 실행 [@Transactional 단일 트랜잭션]
   2-1. UserService.creat()   → 회원 정보 MySQL 저장 (BCrypt 해싱)
   2-2. AuthService.signup()  → 인증용 AuthUser MySQL 저장
   (두 저장이 한 트랜잭션 내 실행 — Kafka 발행 없음)
3. User Service → 클라이언트  201 Created 응답

[로그인]
4. 클라이언트 → API Gateway → User Service  POST /api/auth/login
5. AuthService.login()  → AuthUser 조회 → BCrypt 비밀번호 검증 → JWT 발급
6. User Service → 클라이언트  {"accessToken": "Bearer ..."}
```

> 💡 **설계 포인트**: 통합 전 Auth Service는 `user-created-topic` Kafka 이벤트를 소비하여 인증 데이터를 동기화했습니다. 이벤트 유실 또는 순서 역전 시 회원가입 직후 로그인이 되지 않는 위험이 있었습니다. 통합 후는 동일 트랜잭션으로 두 DB가 함께 커밋/롤백되어 **데이터 정합성이 트랜잭션 수준으로 보장**됩니다.

---

## 🔁 주요 흐름 2: 주문 → 결제 이벤트 플로우 (Outbox 패턴 적용)

```
1. 클라이언트 → API Gateway (JWT 검증) → Order Service  POST /api/orders
2. ProductSnapshotService.getProductInfos()
   2-1. Redis에서 product:snapshot:{id} 일괄 조회
   2-2. 캐시 미스 시 ProductSnapshot DB IN 쿼리로 Fallback
   (OpenFeign 호출 없음, 동기 서비스 간 의존 없음)
3. Order 저장 + OutboxEvent(order-created-topic) 저장 [같은 트랜잭션]
   3-1. 즉시 Kafka 발행 시도 → 성공 시 SENT
   3-2. 실패 시 PENDING 유지 → OutboxScheduler가 30초 후 재발행
4. Payment Service → Kafka 소비 → 결제 처리
5. Product Service → Kafka 소비 → 재고 차감
6. Payment Service → OutboxEvent(payment-completed/failed-topic) 저장 후 발행
7. Order Service → Kafka 소비 → 주문 상태 업데이트
```

> 💡 **설계 포인트 (Outbox 패턴)**: 기존 방식은 DB 저장 후 Kafka 발행 중 장애가 발생하면 이벤트가 유실되는 문제가 있었습니다. `OutboxEvent`를 비즈니스 엔티티와 **같은 트랜잭션**에 저장함으로써 발행 보장을 확보했습니다. 즉시 발행 성공 시 `SENT`로 즉시 업데이트하여 스케줄러 부하를 최소화하고, 실패 시에만 30초 후 재발행하는 이중 안전망 구조입니다.

---

## 🔁 주요 흐름 3: MDC 분산 트레이싱

```
[HTTP 요청]
1. MdcLoggingFilter → X-Trace-Id 헤더 추출 (없으면 UUID 생성) → MDC 저장
2. 각 서비스 로그에 traceId 자동 포함

[Kafka 이벤트]
3. OutboxEventPublisher → KafkaMdcHelper.injectMdc() → Kafka 헤더에 traceId 주입
4. Kafka Consumer → KafkaMdcHelper.extractMdc() → MDC 복원

[비동기 스레드]
5. MdcTaskDecorator → @Async / Virtual Thread 실행 시에도 MDC 컨텍스트 전달
```

> 💡 **설계 포인트**: 분산 환경에서 하나의 요청이 여러 서비스를 거칠 때 로그를 `traceId` 하나로 연결하여 장애 추적을 단순화합니다. HTTP 헤더 → Kafka 헤더 → 비동기 스레드까지 MDC 컨텍스트가 끊기지 않고 전파됩니다.

---

## 🛠️ 기술적 의사결정 포인트

| 기술 | 도입 이유 |
|---|---|
| Kafka (비동기 통신) | 서비스 간 강결합 방지, 장애 격리, 재처리 용이 |
| **Auth + User 통합** | **Kafka 동기화 제거 → 회원가입+인증을 단일 @Transactional로 보장, 운영 복잡도 감소** |
| **OpenFeign 제거** | **Product Service 직접 호출 제거 → ProductSnapshot 조회로 의존성 차단** |
| **Outbox 패턴** | **DB 저장과 Kafka 발행을 동일 트랜잭션으로 묶어 이벤트 유실 원천 차단. 즉시 발행 + 30초 스케줄러 재발행 이중 안전망** |
| **MDC 분산 트레이싱** | **X-Trace-Id를 HTTP/Kafka/비동기까지 전파하여 서비스 간 요청 흐름을 단일 traceId로 추적** |
| ProductSnapshot (Redis + DB) | 상품 정보를 Order Service 내 로컬에 캐싱하여 빠른 주문 생성 지원 |
| Redis Cache | 상품 조회 응답 속도 개선, ProductSnapshot 캐시 레이어 |
| Resilience4j | 외부 서비스 장애 시 Cascade Failure 방지 |
| Flyway | DB 스키마 변경 이력 추적 및 환경 간 일관성 확보 |
| Virtual Threads (Java 21) | I/O 바운드 처리 성능 향상, 스레드 비용 절감 |
| corepay-common 1.1.1 | 중복 코드 제거, Outbox·MDC 포함 서비스 간 공통 로직 통일 |
| Prometheus + Grafana | 실시간 메트릭 수집 및 대시보드 시각화 |
