import { describe, it, expect, vi, afterEach } from "vitest";
import { TaskQueue } from "../src/workerPool";

describe("TaskQueue.getPerformanceMetrics", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("reports zeros before any task has run", () => {
    const m = new TaskQueue().getPerformanceMetrics();
    expect(m.totalProcessed).toBe(0);
    expect(m.latencyPercentiles).toEqual({ p50: 0, p95: 0, p99: 0, avg: 0 });
  });

  it("uses nearest-rank percentiles over known latencies", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000_000);
    const queue = new TaskQueue({ concurrency: 1 });
    for (let latency = 1; latency <= 100; latency++) {
      queue.enqueue({
        id: `t${latency}`,
        payload: null,
        retries: 0,
        maxRetries: 1,
        enqueuedAt: Date.now() - latency
      });
    }
    while (queue.queueLength > 0) {
      await queue.processNext(async () => {});
    }
    const { latencyPercentiles: p, totalSucceeded } = queue.getPerformanceMetrics();
    expect(totalSucceeded).toBe(100);
    expect(p.p50).toBe(50);
    expect(p.p95).toBe(95);
    expect(p.p99).toBe(99);
    expect(p.avg).toBe(50.5);
  });
});
