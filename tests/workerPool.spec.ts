import { describe, it, expect } from "vitest";
import { TaskQueue } from "../src/workerPool";

describe("TaskQueue & WorkerPool Concurrency", () => {
  it("strictly honors concurrency limits under heavy load", async () => {
    const queue = new TaskQueue({ concurrency: 3 });
    let maxObservedActive = 0;

    for (let i = 0; i < 15; i++) {
      queue.enqueue({
        id: `task_${i}`,
        payload: { val: i },
        retries: 0,
        maxRetries: 1
      });
    }

    await queue.processAll(async () => {
      maxObservedActive = Math.max(maxObservedActive, queue.activeWorkersCount);
      await new Promise((res) => setTimeout(res, 20));
      maxObservedActive = Math.max(maxObservedActive, queue.activeWorkersCount);
    });

    expect(maxObservedActive).toBeLessThanOrEqual(3);
    expect(queue.queueLength).toBe(0);
  });

  it("prioritizes tasks according to CRITICAL > HIGH > NORMAL > LOW", async () => {
    const queue = new TaskQueue({ concurrency: 1 });
    const executedOrder: string[] = [];

    // Enqueue in non-ordered priority sequence
    queue.enqueue({ id: "low_1", payload: {}, priority: "LOW", retries: 0, maxRetries: 1 });
    queue.enqueue({ id: "norm_1", payload: {}, priority: "NORMAL", retries: 0, maxRetries: 1 });
    queue.enqueue({ id: "crit_1", payload: {}, priority: "CRITICAL", retries: 0, maxRetries: 1 });
    queue.enqueue({ id: "high_1", payload: {}, priority: "HIGH", retries: 0, maxRetries: 1 });

    await queue.processAll(async (task) => {
      executedOrder.push(task.id);
    });

    expect(executedOrder).toEqual(["crit_1", "high_1", "norm_1", "low_1"]);
  });

  it("handles graceful shutdown draining properly", async () => {
    const queue = new TaskQueue({ concurrency: 2 });
    queue.enqueue({ id: "t1", payload: {}, retries: 0, maxRetries: 1 });

    const drainPromise = queue.drain();
    expect(() => {
      queue.enqueue({ id: "t2", payload: {}, retries: 0, maxRetries: 1 });
    }).toThrow("Cannot enqueue task: WorkerPool is currently draining");

    await drainPromise;
  });
});
