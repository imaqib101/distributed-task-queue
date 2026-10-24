import { describe, it, expect } from "vitest";
import { TaskQueue } from "../src/workerPool";

describe("DeadLetterQueue (DLQ), Metrics & Redrive", () => {
  it("routes exhausted tasks to DLQ with detailed error history", async () => {
    const queue = new TaskQueue({ concurrency: 1 });

    queue.enqueue({
      id: "payment_failure_task",
      payload: { accountId: "acc_992" },
      retries: 0,
      maxRetries: 2,
      initialDelayMs: 10
    });

    let attempts = 0;
    await queue.processAll(async () => {
      attempts++;
      throw new Error("Stripe Gateway Timeout 504");
    });

    expect(attempts).toBe(2);
    const metrics = queue.dlq.getMetrics();
    expect(metrics.totalDead).toBe(1);
    expect(metrics.failureReasons["Stripe Gateway Timeout 504"]).toBe(1);
    expect(metrics.retryDistribution[2]).toBe(1);

    const deadTask = queue.dlq.get("payment_failure_task");
    expect(deadTask).toBeDefined();
    expect(deadTask?.errorHistory).toHaveLength(2);
    expect(deadTask?.errorHistory[0].message).toBe("Stripe Gateway Timeout 504");
  });

  it("calculates accurate performance and latency percentiles", async () => {
    const queue = new TaskQueue({ concurrency: 2 });

    for (let i = 0; i < 10; i++) {
      queue.enqueue({
        id: `t_${i}`,
        payload: { i },
        retries: 0,
        maxRetries: 1
      });
    }

    await queue.processAll(async (task) => {
      // Simulate varied task processing durations
      const ms = task.payload.i * 5;
      await new Promise((r) => setTimeout(r, ms));
    });

    const perf = queue.getPerformanceMetrics();
    expect(perf.totalProcessed).toBe(10);
    expect(perf.totalSucceeded).toBe(10);
    expect(perf.totalFailed).toBe(0);
    expect(perf.latencyPercentiles.p50).toBeGreaterThanOrEqual(0);
    expect(perf.latencyPercentiles.p95).toBeGreaterThanOrEqual(perf.latencyPercentiles.p50);
  });

  it("redrives dead-letter tasks back into the active queue", async () => {
    const queue = new TaskQueue({ concurrency: 1 });

    queue.enqueue({
      id: "retry_task",
      payload: { sendEmail: true },
      retries: 0,
      maxRetries: 1
    });

    // Cause failure
    await queue.processAll(async () => {
      throw new Error("SMTP connection lost");
    });

    expect(queue.dlq.getMetrics().totalDead).toBe(1);

    // Redrive DLQ
    const redrivenCount = queue.redriveDLQ();
    expect(redrivenCount).toBe(1);
    expect(queue.dlq.getMetrics().totalDead).toBe(0);
    expect(queue.queueLength).toBe(1);

    // Successful second run
    await queue.processAll(async (task) => {
      expect(task.id).toBe("retry_task");
      expect(task.retries).toBe(0); // Reset for clean retry
    });

    expect(queue.queueLength).toBe(0);
  });
});
