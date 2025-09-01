# Production Scenarios for PyRQG

This directory contains business scenarios with realistic schemas and workloads for comprehensive database testing.

## Scenarios Overview

1. **E-Commerce Platform** - Online retail with products, orders, inventory
2. **Banking System** - Accounts, transactions, fraud detection
3. **Healthcare Records** - Patients, appointments, medical history
4. **Social Media Network** - Users, posts, interactions
5. **Logistics & Shipping** - Packages, routes, tracking
6. **Hotel Reservation System** - Rooms, bookings, guests
7. **Educational Platform** - Courses, students, grades
8. **Real Estate Management** - Properties, listings, contracts
9. **Manufacturing ERP** - Production, inventory, quality control
10. **Food Delivery Service** - Restaurants, orders, drivers
11. **Subscription SaaS** - Plans, billing, usage tracking
12. **Event Ticketing** - Events, venues, ticket sales
13. **IoT Sensor Network** - Devices, readings, alerts
14. **Gaming Platform** - Players, matches, leaderboards
15. **Content Management** - Articles, media, publishing
16. **HR Management** - Employees, payroll, performance
17. **Inventory Warehouse** - Stock, movements, audits
18. **Customer Support** - Tickets, agents, SLAs
19. **Financial Trading** - Orders, positions, market data
20. **Analytics Platform** - Events, metrics, dashboards

## Directory Structure

```
production_scenarios/
├── schemas/          # Legacy: DDL per scenario
├── workloads/        # Legacy: workload generators (queries only)
├── scenarios/        # Single-file scenarios (schema + queries)
├── configs/          # Configuration files
└── reports/          # Test results and metrics
```

## Usage

Each scenario includes:
- Schema definition (DDL)
- Realistic workload patterns

## Single-file Scenarios (recommended)

Keep schema and queries together in one Python file, or reference external schema files under `schem.files/` at the repo root.

Single-file module must export:
- `grammar` (or `g`): a `pyrqg.dsl.core.Grammar` with `rule("query", ...)`
- either `schema_sql: str` or `schema_files: list[str]` (filenames relative to `schem.files/`)

Example: `production_scenarios/scenarios/ecommerce_scenario.py` with `schema_files = ["ecommerce.sql"]`.

Run via runner:
```
python -m pyrqg.runner production \
  --production-scenario production_scenarios/scenarios/ecommerce_scenario.py \
  --count 200
```
