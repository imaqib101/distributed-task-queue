import { describe, it, expect } from "vitest";
import { TaskQueue } from "../src/workerPool";

describe("Adaptive Worker Pool & DLQ Optimizations", () => {
  it("scales concurrency up when backlog exceeds threshold", () => {
    const queue = new TaskQueue({
      concurrency: 3,
      adaptive: {
        minConcurrency: 2,
        maxConcurrency: 8,
        backlogScaleThreshold: 5,
        errorRateThreshold: 0.2
      }
    });

    expect(queue.concurrency).toBe(3);

    // Enqueue 10 tasks to create backlog
    for (let i = 0; i < 10; i++) {
      queue.enqueue({
        id: `task_${i}`,
        payload: { index: i },
        retries: 0,
        maxRetries: 2
      });
    }

    // Adaptive controller should have scaled concurrency up
    expect(queue.concurrency).toBeGreaterThan(3);
  });

  it("categorizes DLQ errors into transient and permanent with selective redrive", () => {
    const queue = new TaskQueue({ concurrency: 2 });

    // Enqueue a task that will encounter transient timeout
    queue.enqueue({
      id: "t_timeout",
      payload: { action: "apiCall" },
      retries: 2,
      maxRetries: 2
    });

    // Enqueue a task that will encounter permanent validation error
    queue.enqueue({
      id: "t_validation",
      payload: { action: "badInput" },
      retries: 2,
      maxRetries: 2
    });

    // Manually push to DLQ to test categorization and metrics
    queue.dlq.push(
      { id: "t_timeout", payload: {}, retries: 2, maxRetries: 2 },
      new Error("Gateway timeout 504"),
      []
    );
    queue.dlq.push(
      { id: "t_validation", payload: {}, retries: 2, maxRetries: 2 },
      new Error("Invalid schema payload: field missing"),
      []
    );

    const metrics = queue.dlq.getMetrics();
    expect(metrics.totalDead).toBe(2);
    expect(metrics.transientCount).toBe(1);
    expect(metrics.permanentCount).toBe(1);

    // Redrive transient tasks only
    const count = queue.redriveTransientDLQ();
    expect(count).toBe(1);
    expect(queue.dlq.getMetrics().totalDead).toBe(1);
    expect(queue.queueLength).toBe(3); // 2 original + 1 re-enqueued transient
  });

  it("prunes expired dead letter entries based on retention policy", () => {
    const queue = new TaskQueue();

    const entry = queue.dlq.push(
      { id: "old_task", payload: {}, retries: 3, maxRetries: 3 },
      new Error("Expired job"),
      []
    );

    // Artificially age the entry
    entry.deadAt = Date.now() - 10000;

    const pruned = queue.dlq.pruneOlderThan(5000);
    expect(pruned).toBe(1);
    expect(queue.dlq.getMetrics().totalDead).toBe(0);
  });
});
