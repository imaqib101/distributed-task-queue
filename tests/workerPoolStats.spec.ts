import { describe, it, expect, beforeEach } from "vitest";
import { WorkerPoolStatsTracker, PoolSnapshot } from "../src/workerPoolStats";

describe("WorkerPoolStatsTracker", () => {
  let tracker: WorkerPoolStatsTracker;

  beforeEach(() => {
    tracker = new WorkerPoolStatsTracker(10);
  });

  it("recordTask updates latency window and counts", () => {
    tracker.recordTask(50, true);
    tracker.recordTask(100, false);
    const snap = tracker.snapshot(2, 5, 1);
    expect(snap.p50LatencyMs).toBe(50);
    expect(snap.errorRate).toBe(0.5);
    expect(snap.activeWorkers).toBe(2);
    expect(snap.queueDepth).toBe(5);
    expect(snap.dlqDepth).toBe(1);
  });

  it("getPercentile returns correct p50/p95/p99 on known data", () => {
    tracker = new WorkerPoolStatsTracker(100);
    for (let i = 1; i <= 100; i++) {
      tracker.recordTask(i, true);
    }
    expect(tracker.getPercentile(50)).toBe(50);
    expect(tracker.getPercentile(95)).toBe(95);
    expect(tracker.getPercentile(99)).toBe(99);
  });

  it("evicts the oldest samples once the window is full", () => {
    for (let i = 1; i <= 100; i++) {
      tracker.recordTask(i, true);
    }
    // window size is 10, so only samples 91..100 remain
    expect(tracker.getPercentile(0)).toBe(91);
    expect(tracker.getPercentile(100)).toBe(100);
  });

  it("snapshot returns correct PoolSnapshot shape", () => {
    tracker.recordTask(20, true);
    const snap = tracker.snapshot(1, 0, 0);
    expect(snap).toHaveProperty("timestamp");
    expect(snap).toHaveProperty("activeWorkers", 1);
    expect(snap).toHaveProperty("queueDepth", 0);
    expect(snap).toHaveProperty("throughputPerSec");
    expect(snap).toHaveProperty("p50LatencyMs", 20);
    expect(snap).toHaveProperty("p95LatencyMs", 20);
    expect(snap).toHaveProperty("p99LatencyMs", 20);
    expect(snap).toHaveProperty("errorRate", 0);
    expect(snap).toHaveProperty("dlqDepth", 0);
  });

  it("getHealth flags unhealthy when errorRate exceeds threshold", () => {
    tracker.recordTask(10, false);
    tracker.recordTask(10, false);
    const snap = tracker.snapshot(1, 0, 2);
    const health = tracker.getHealth(snap, { maxErrorRate: 0.1 });
    expect(health.healthy).toBe(false);
    expect(health.warnings.length).toBeGreaterThan(0);
    expect(health.warnings[0]).toContain("Error rate");
  });

  it("getHealth flags warning when queueDepth or p95 is too high", () => {
    tracker.recordTask(1500, true);
    const snap = tracker.snapshot(2, 600, 0);
    const health = tracker.getHealth(snap, { maxP95Ms: 1000, maxQueueDepth: 500 });
    expect(health.healthy).toBe(false);
    expect(health.warnings).toHaveLength(2);
    expect(health.warnings.some(w => w.includes("P95 latency"))).toBe(true);
    expect(health.warnings.some(w => w.includes("Queue depth"))).toBe(true);
  });

  it("reset clears state completely", () => {
    tracker.recordTask(100, true);
    tracker.snapshot(1, 1, 1);
    expect(tracker.getSnapshots()).toHaveLength(1);

    tracker.reset();
    expect(tracker.getSnapshots()).toHaveLength(0);
    expect(tracker.getPercentile(50)).toBe(0);
  });

  it("handles empty latency window safely", () => {
    expect(tracker.getPercentile(50)).toBe(0);
    const snap = tracker.snapshot(0, 0, 0);
    expect(snap.p50LatencyMs).toBe(0);
    expect(snap.errorRate).toBe(0);
  });
});
