import { describe, it, expect, beforeEach } from "vitest";
import { DLQMonitor, DLQMessage } from "../src/dlqMetrics";

describe("DLQMonitor & Metrics / Replay", () => {
  let monitor: DLQMonitor;

  beforeEach(() => {
    monitor = new DLQMonitor(5);
  });

  it("pushes messages and detects alert thresholds", () => {
    const alert1 = monitor.push({ id: "1", queueName: "q1", error: "ERR_TIMEOUT", attempts: 3 });
    expect(alert1).toBe(false);

    const alert2 = monitor.push({ id: "2", queueName: "q1", error: "ERR_TIMEOUT", attempts: 3 });
    expect(alert2).toBe(false);

    const alert3 = monitor.push({ id: "3", queueName: "q2", error: "ERR_DB", attempts: 3 });
    expect(alert3).toBe(false);

    const alert4 = monitor.push({ id: "4", queueName: "q1", error: "ERR_TIMEOUT", attempts: 3 });
    expect(alert4).toBe(false);

    const alert5 = monitor.push({ id: "5", queueName: "q2", error: "ERR_DB", attempts: 3 });
    expect(alert5).toBe(true); // threshold 5 reached
  });

  it("retrieves failed messages by queue name or all", () => {
    monitor.push({ id: "1", queueName: "emails", error: "SMTP_AUTH", attempts: 2 });
    monitor.push({ id: "2", queueName: "payments", error: "STRIPE_ERR", attempts: 1 });

    expect(monitor.getFailedMessages()).toHaveLength(2);
    expect(monitor.getFailedMessages("emails")).toHaveLength(1);
    expect(monitor.getFailedMessages("emails")[0].id).toBe("1");
  });

  it("clears queue globally or by specific queue name", () => {
    monitor.push({ id: "1", queueName: "emails", error: "SMTP_AUTH", attempts: 2 });
    monitor.push({ id: "2", queueName: "payments", error: "STRIPE_ERR", attempts: 1 });

    monitor.clearQueue("emails");
    expect(monitor.getFailedMessages()).toHaveLength(1);
    expect(monitor.getFailedMessages("payments")[0].id).toBe("2");

    monitor.clearQueue();
    expect(monitor.getFailedMessages()).toHaveLength(0);
  });

  it("computes accurate DLQMetricsSummary including total, byQueue, and topErrors", () => {
    monitor.push({ id: "1", queueName: "q1", error: "TIMEOUT", attempts: 3 });
    monitor.push({ id: "2", queueName: "q1", error: "TIMEOUT", attempts: 3 });
    monitor.push({ id: "3", queueName: "q2", error: "DB_ERROR", attempts: 3 });

    const metrics = monitor.getMetrics();
    expect(metrics.total).toBe(3);
    expect(metrics.byQueue).toEqual({ q1: 2, q2: 1 });
    expect(metrics.topErrors).toEqual([
      { error: "TIMEOUT", count: 2 },
      { error: "DB_ERROR", count: 1 }
    ]);
    expect(metrics.oldestFailedAt).toBeDefined();

    const q1Metrics = monitor.getMetrics("q1");
    expect(q1Metrics.total).toBe(2);
    expect(q1Metrics.topErrors).toHaveLength(1);
  });

  it("replays message successfully: removes from DLQ and calls handler", async () => {
    monitor.push({ id: "task_abc", queueName: "q1", error: "TIMEOUT", attempts: 3 });
    expect(monitor.getFailedMessages()).toHaveLength(1);

    let replayedMsg: DLQMessage | null = null;
    const success = await monitor.replayMessage("task_abc", async (msg) => {
      replayedMsg = msg;
    });

    expect(success).toBe(true);
    expect(replayedMsg).not.toBeNull();
    expect(replayedMsg?.id).toBe("task_abc");
    expect(monitor.getFailedMessages()).toHaveLength(0);
  });

  it("returns false and does nothing when replaying non-existent message id", async () => {
    monitor.push({ id: "task_abc", queueName: "q1", error: "TIMEOUT", attempts: 3 });

    let handlerCalled = false;
    const success = await monitor.replayMessage("non_existent", async () => {
      handlerCalled = true;
    });

    expect(success).toBe(false);
    expect(handlerCalled).toBe(false);
    expect(monitor.getFailedMessages()).toHaveLength(1);
  });
});
