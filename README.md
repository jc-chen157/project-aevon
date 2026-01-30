# Aevon (🛡️, ❄️)

**The High-Precision Usage & State Ledger for Modern Infrastructure.**

[![License: FSL-1.1](https://img.shields.io/badge/License-FSL--1.1-blue.svg)](https://fsl.software/)
[![Standard: Event-Sourced](https://img.shields.io/badge/Standard-Event--Sourced-orange.svg)](#)

Aevon is an event-sourced usage engine and state tracker designed for developers who need 100% precision in high-scale environments. It decouples **usage logic** (how you track and aggregate state) from **billing** (how you charge for it), acting as the "Source of Truth" for your application's state.

---

## 🏗️ The Engineering Core: The Iceberg Model

Aevon is built on the principle that state is not a static number, but a **materialized view of history**.

1.  **The Deep (Event Store):** An immutable, append-only log of every usage event. This "Iceberg" allows for **retroactive state corrections**. If an event from "3 hours ago" arrives now, Aevon re-calculates history to ensure the current state is correct.
2.  **The Surface (Projections):** High-speed, customizable aggregation functions that turn raw events into "State." Whether it’s token counts, active seats, or credit balances, Aevon provides the real-time view.

---

## 🛠️ Key Capabilities

* **⌛ Retroactive Precision:** Built-in handling for late-arriving data. Aevon automatically emits "Correction Events" when historical data modifies current state.
* **🔧 Customizable Aggregations:** Define state logic via flexible functions (Sum, Max, Gauge, Distinct Count) that run over the event stream in real-time.
* **🛡️ Atomic Reservations:** A `permit -> execute -> settle` flow. Perfect for AI Agents, high-performance computing, or any service that needs to lock budget/credits before execution.
* **⚡ Agentic Self-Optimization:** A dedicated API allows autonomous agents or services to query their own usage patterns and cost-efficiency to optimize their performance on the fly.
* **🔌 Headless Integration:** Aevon is "Billing Provider Agnostic." Use it as a sidecar to Stripe, Orb, or your own internal systems. We handle the math; they handle the invoices.

---

## 🛡️ The "Agentic" Layer

Aevon is "Agent-Ready" out of the box. By providing a low-latency state API, it solves the primary fear of the agentic economy: **Runaway Costs**. 

Agents can use Aevon to:
* **Check Balance:** Verify budget before initiating expensive inference loops.
* **Lock Credits:** Ensure funds aren't double-spent across parallel workflows.
* **Analyze ROI:** Programmatically audit their own "Success-per-Dollar" metrics.

---

## 🚀 Getting Started

### 1. Run via Docker Compose
Start the Aevon engine with a single command:

```bash
docker-compose up -d