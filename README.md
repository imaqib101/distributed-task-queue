# Distributed Task Queue ⚡

[![TypeScript](https://img.shields.io/badge/TypeScript-Strict-007ACC?style=for-the-badge&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-20+-43853D?style=for-the-badge&logo=node.js&logoColor=white)](https://nodejs.org/)
[![Vitest](https://img.shields.io/badge/Vitest-Tested-729B1B?style=for-the-badge&logo=vitest&logoColor=white)](https://vitest.dev/)
[![DLQ Observability](https://img.shields.io/badge/DLQ-Observability%20%26%20Redrive-informational?style=for-the-badge)](https://en.wikipedia.org/wiki/Dead_letter_queue)
[![CI](https://github.com/imaqib101/distributed-task-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/imaqib101/distributed-task-queue/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge)](LICENSE)

A high-performance, resilient **Asynchronous Task Queue & Worker Pool** built in TypeScript. Features strict priority scheduling (`CRITICAL` ➔ `LOW`), bounded worker concurrency, exponential retry backoff with jitter, dead-letter queue (DLQ) categorization, and redrive capabilities.

---

## 🏗️ Architecture

```text
Producer Enqueues Task
          │
          ▼
┌───────────────────────────────────────────────┐
│              Priority Task Queue              │
│       CRITICAL > HIGH > NORMAL > LOW          │
└───────────────────────┬───────────────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────┐
│     Worker Pool (Max Active Concurrency)      │
└───────┬───────────────────────────────┬───────┘
        │                               │
        ▼ (Success)                     ▼ (Failure)
  [ Record Metrics ]         [ Exponential Backoff + Jitter ]
  (p50, p95, p99, Throughput)           │
                                        ├─ Attempts < Max: Reschedule to Queue
                                        │
                                        ▼ (Attempts >= Max)
                             ┌───────────────────────┐
                             │   Dead Letter Queue   │
                             │ (Error History & DLQ) │
                             └──────────┬────────────┘
                                        │
                                        ▼
                             [ DLQ Replay / Redrive ]
```

---

## ✨ Core Capabilities

- 🎯 **Strict Priority Scheduling**: Enforces task execution order across `CRITICAL`, `HIGH`, `NORMAL`, and `LOW` priorities with FIFO fairness within identical priority bands.
- ⚡ **Worker Pool Concurrency**: Limits concurrent asynchronous executions to prevent thread/memory saturation and downstream rate-limiting.
- 🔁 **Exponential Backoff with Jitter**: Configurable delay curve with random jitter (20%) to disperse retry traffic and protect downstream dependencies.
- 📬 **Full DLQ Observability**: Tracks detailed per-attempt error history, stack messages, timestamps, and aggregates failure reason categorization.
- 🔄 **DLQ Redrive Engine**: Replay single tasks by ID or redrive all dead-lettered workloads back into the primary queue after downstream service recovery.
- 📊 **Telemetry Percentiles**: Real-time computation of `p50`, `p95`, `p99`, and average execution latency.
- 🛑 **Graceful Draining**: Safe shutdown protocol (`drain()`) awaiting completion of in-flight workloads with deadline timeouts.

---

## 🚀 Quick Start

```typescript
import { TaskQueue } from "distributed-task-queue";

// 1. Initialize Worker Pool with max 5 concurrent workers
const queue = new TaskQueue({ concurrency: 5, drainTimeoutMs: 10000 });

// 2. Enqueue Critical Job
queue.enqueue({
  id: "job_export_9281",
  payload: { reportType: "quarterly_financials" },
  priority: "CRITICAL",
  retries: 0,
  maxRetries: 3,
  initialDelayMs: 100
});

// 3. Process with Worker Handler
await queue.processAll(async (task) => {
  console.log(`Processing task ${task.id} with priority ${task.priority}`);
  await generateReport(task.payload);
});

// 4. Inspect Performance & DLQ Observability
const metrics = queue.getPerformanceMetrics();
console.log(`p95 Latency: ${metrics.latencyPercentiles.p95}ms`);

const dlqMetrics = queue.dlq.getMetrics();
console.log(`Dead Letters: ${dlqMetrics.totalDead}`);
```

---

## 📦 Installation

Requires Node.js 20 or newer. The package is not published to npm yet; install from source:

```bash
git clone https://github.com/imaqib101/distributed-task-queue.git
cd distributed-task-queue
npm ci
npm run build
```

---

## 📚 API

Everything below is exported from the package entry point.

| Export                   | Description                                                                                                                                                               |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TaskQueue`              | Priority queue and worker pool. Options: `concurrency` (default 5), `drainTimeoutMs` (default 5000), `adaptive` (min/max concurrency, backlog and error-rate thresholds). |
| `DeadLetterQueue`        | Held by `queue.dlq`. `push`, `get`, `getMetrics`, `replay`, `replayAll`, `replayBatch`, `pruneOlderThan`, `clear`.                                                        |
| `categorizeError`        | Classifies an error message as `"transient"` or `"permanent"`.                                                                                                            |
| `DLQMonitor`             | Standalone message-oriented DLQ monitor with alert threshold, per-queue metrics and `replayMessage`.                                                                      |
| `WorkerPoolStatsTracker` | Sliding-window latency percentiles, throughput and error rate snapshots, plus `getHealth` threshold checks.                                                               |

`TaskQueue` methods: `enqueue`, `enqueueBatch`, `processNext`, `processAll`, `drain`, `redriveDLQ`, `redriveTransientDLQ`, `getPerformanceMetrics`, and the getters `queueLength`, `activeWorkersCount`, `scheduledRetriesCount`.

---

## 🛠️ Development

| Script                  | Purpose                 |
| ----------------------- | ----------------------- |
| `npm run build`         | Compile to `dist/`      |
| `npm run typecheck`     | Type-check without emit |
| `npm run lint`          | ESLint                  |
| `npm run format:check`  | Prettier check          |
| `npm test`              | Run the Vitest suite    |
| `npm run test:coverage` | Run tests with coverage |

See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution workflow.

---

## 👤 Author

**Mohammed Aqib**

- GitHub: [@imaqib101](https://github.com/imaqib101)
- Location: Bengaluru, India
