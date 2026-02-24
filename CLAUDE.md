# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Loopers Spring Kotlin Template - A multi-module monolithic e-commerce application built with Kotlin and Spring Boot.
This project implements an e-commerce system with products, orders, points, likes, coupons, payments, and rankings.

## Essential Commands

```bash
make init                                    # Setup git hooks for pre-commit ktlint checks

# Infrastructure
docker-compose -f ./docker/infra-compose.yml up        # PostgreSQL, Redis, Kafka
docker-compose -f ./docker/monitoring-compose.yml up    # Prometheus + Grafana (localhost:3000, admin/admin)

# Build and Test
./gradlew build                             # Build all modules
./gradlew test                              # Run all tests
./gradlew :apps:commerce-api:test           # Run tests for specific module
./gradlew :apps:commerce-api:test --tests "com.loopers.domain.coupon.*Test"
./gradlew :apps:commerce-api:cleanTest :apps:commerce-api:test --tests "com.loopers.domain.coupon.*Test"  # Force re-run
./gradlew jacocoTestReport                  # Test coverage report

# Run applications
./gradlew :apps:commerce-api:bootRun        # Main REST API (port 8080)
./gradlew :apps:commerce-streamer:bootRun   # Kafka streaming app
./gradlew :apps:pg-simulator:bootRun        # PG simulator (port 8082)

# Code quality
./gradlew ktlintCheck                       # Check Kotlin code style
./gradlew ktlintFormat                      # Auto-format Kotlin code
```

## Architecture Overview

### Multi-Module Structure

```
Root
├── apps/
│   ├── commerce-api        # Main REST API (HTTP endpoints, business logic)
│   ├── commerce-streamer   # Kafka consumer (event-driven stat/ranking updates)
│   ├── commerce-batch      # Spring Batch (ranking score aggregation, non-web, exits after job)
│   └── pg-simulator        # Payment Gateway simulator (standalone, local/dev only)
├── modules/
│   ├── jpa                 # DataSource, JPA/Hibernate, QueryDSL, BaseEntity, MySQL Testcontainer fixture
│   ├── redis               # Redis master/replica config, CacheTemplate, Redis Testcontainer fixture
│   ├── kafka               # Kafka producer/consumer config with DLT, Kafka Testcontainer fixture
│   └── event-schema        # Shared CloudEventEnvelope data class (Kafka message schema)
└── supports/
    ├── jackson             # ObjectMapper config (NON_NULL, module discovery)
    ├── logging             # Logback config (JSON/plain console, Slack appender per-profile)
    └── monitoring          # Micrometer + Prometheus via actuator
```

Module dependency rules: `apps` → `modules` + `supports`. `modules` must be domain-agnostic. No circular dependencies.

### Layered Architecture (commerce-api)

```
com.loopers
├── domain/                     # Domain Layer - entities, value objects, repository interfaces, Service classes
│   ├── user, product, order, point, like, coupon, payment, ranking
│   └── common/                 # CoreException, ErrorType, DomainEvent interface, Money value object
├── application/                # Application Layer - Facade classes for cross-domain orchestration
│   └── {domain}/               # *Criteria (input), *Info (output), Cached* (Redis DTOs), *CacheKeys
├── infrastructure/             # Infrastructure Layer - repository implementations, Feign clients, outbox
│   └── {domain}/               # *JpaRepository + *RdbRepository pairs
└── interfaces/                 # Interface Layer
    ├── api/{domain}/           # *V1Controller + *V1ApiSpec (Swagger interface), *V1Request/Response
    └── event/                  # @TransactionalEventListener / @EventListener handlers
```

### Key Architectural Patterns

**Service vs Facade:**
- **Service**: Single-domain business logic (`UserService`, `ProductService`, `OrderService`)
- **Facade**: Cross-domain orchestration (`OrderFacade` coordinates order, product, point, coupon, payment)
- **Rule**: No horizontal dependencies between Services or Facades — only Facade calls multiple Services

**Two-Layer Repository Pattern:**
- `*JpaRepository`: extends `JpaRepository<Entity, Long>`, Spring Data JPA interface (+ QueryDSL when needed)
- `*RdbRepository`: implements domain `*Repository` interface, wraps JpaRepository, declares `@Transactional`

**Transactional Outbox Pattern:**
- Domain entities collect `DomainEvent`s in a `@Transient` list, drained via `pollEvents()`
- `OutboxEventListener` (`@TransactionalEventListener(BEFORE_COMMIT)`) saves events to `Outbox` table in same TX
- `OutboxRelayService`: cursor-based sequential relay to Kafka with HOL-blocking, retry, DLQ (`OutboxFailed`)
- `CloudEventEnvelopeFactory` maps `DomainEvent` subtypes to `CloudEventEnvelope` with type naming: `loopers.{domain}.{action}.v{version}`

**Two-Phase Event Processing:**
- Synchronous (same TX, `BEFORE_COMMIT`): stock decrease, outbox save
- Async (`@Async` + `AFTER_COMMIT`): PG payment request, like count updates, data platform notifications

**Controller API Conventions:**
- `*V1ApiSpec` interface holds Swagger annotations, `*V1Controller` implements it with Spring MVC annotations
- All responses wrapped in `ApiResponse<T>` with `meta` (result, errorCode, message) and `data`
- User context via `X-USER-ID` header
- `ApiControllerAdvice`: `CoreException` → mapped HTTP status, Spring exceptions → 400, catch-all → 500

**Ranking Data Flow (3-tier):**
commerce-api publishes events → Outbox → Kafka → commerce-streamer writes `ProductStatistic`/`ProductHourlyMetric` → commerce-batch aggregates into Redis ZSETs (hourly/daily) and RDB MV tables (weekly/monthly) → commerce-api reads via `ProductRankingProxyReader`

**PG Gateway Integration:**
- `PgGateway` with Resilience4j: `@CircuitBreaker` (order 1) + `@Retry` (order 2)
- Feign clients: `PgPaymentFeignClient`, `PgQueryFeignClient`
- `PgExceptionClassifier`: classifies Feign errors into `PgRequestNotReachedException` / `PgResponseUncertainException`

### Key Domain Classes

- **`BaseEntity`** (`modules/jpa`): `@MappedSuperclass` with `id` (IDENTITY), `createdAt`, `updatedAt`, `deletedAt`, soft delete via `delete()`/`restore()`, `guard()` for validation
- **`Money`**: `@Embeddable`, wraps `BigDecimal` (scale=2, HALF_UP), operator overloads (+, -, *, /), `applyPercentage()`, equality by `compareTo`
- **`CoreException`**: `errorType: ErrorType` (INTERNAL_ERROR/BAD_REQUEST/NOT_FOUND/CONFLICT), optional `customMessage`
- **`DomainEvent`** interface: `occurredAt: Instant` — entities hold and poll events
- **`CacheKey`** interface: `key: String`, `traceKey: String`, `ttl: Duration` — implemented as sealed classes per domain

### commerce-streamer

- Kafka batch consumers (`concurrency=3`, manual ACK) for `like-events`, `order-events`, `product-events`, `ranking-events`
- **Idempotency**: `EventHandled` entity with unique `idempotency_key` (`{consumerGroup}:{eventId}`), INSERT-only dedup
- Updates `ProductStatistic` and `ProductHourlyMetric` for downstream batch aggregation

### commerce-batch

- Non-web app (`web-application-type: none`), exits after job completion
- Jobs: `hourlyRankingJob`, `todayDailyRollupJob`, `yesterdayReconciliationJob`, `dailyRankingJob`, `weeklyRankingJob`, `monthlyRankingJob`
- Chunk-oriented (size 1000), retry on pessimistic lock failures, skip on data integrity errors
- Triggered by `RankingJobScheduler` (`@Scheduled`) or manually via `BatchRankingController`

## Testing

### Test Suffixes

- `*Test`: pure unit tests (no Spring context)
- `*IntegrationTest`: Spring context with Testcontainers
- `*E2ETest`: `@SpringBootTest(RANDOM_PORT)` + `TestRestTemplate`
- `*ConcurrencyTest`: `@SpringBootTest` with `CountDownLatch` + thread pool

### Test Infrastructure

- **Testcontainers** (shared via `testFixtures`): MySQL 8.0 (`modules/jpa`), Redis (`modules/redis`), Kafka (`modules/kafka`)
- **`DatabaseCleanUp`**: discovers `@Entity` classes via JPA metamodel, `truncateAllTables()` with foreign key checks disabled, used in `@AfterEach`
- **`KSelect`**: Kotlin bridge to Instancio's `Select.field()` using `KProperty1`
- Tests run with `spring.profiles.active=test`, timezone `Asia/Seoul`, `maxParallelForks=1`

## Configuration

- **Profiles**: `local`, `test`, `dev`, `qa`, `prd`
- Module configs imported via `spring.config.import`: `jpa.yml`, `redis.yml`, `kafka.yml`, `logging.yml`, `monitoring.yml`
- JPA: `open-in-view: false`, `ddl-auto: create` (local/test), `default_batch_fetch_size: 100`, timezone `NORMALIZE_UTC`
- Kafka producer: idempotent (`acks: all`), StringSerializer, retries: 3
- Kafka consumer: manual ACK, DLQ via `DeadLetterPublishingRecoverer`
- Schedulers disabled in `local` and `test` profiles
- `AsyncConfig`: ThreadPoolTaskExecutor (core=2, max=10, queue=1000, prefix `event-async-`)
- Injectable `Clock` bean via `ClockConfig` (enables mocking in tests)

## Tech Stack

- Kotlin 2.0.20, Java 21 toolchain, Spring Boot 3.4.4, Spring Cloud 2024.0.1
- QueryDSL, Resilience4j 2.2.0, springdoc-openapi 2.7.0
- Test: JUnit 5, SpringMockK 4.0.2, Mockito-Kotlin, Instancio 5.0.2, WireMock
- ktlint enforced via pre-commit hooks (`./gradlew ktlintCheck`)

## Tool Usage

### Serena MCP

Prefer Serena for semantic code operations:

- Understanding codebase structure (`get_symbols_overview`)
- Finding symbols and their relationships (`find_symbol`, `find_referencing_symbols`)
- Navigating complex code paths

### Context7

Use for external library documentation lookup before implementing integrations.
