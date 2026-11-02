import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";
import { DeadLetterQueue, categorizeError } from "../src/dlq";
import { Task } from "../src/types";

describe("DeadLetterQueue & error categorization unit tests", () => {
  let dlq: DeadLetterQueue<string>;

  beforeEach(() => {
    dlq = new DeadLetterQueue<string>();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  const createTask = (id: string, retries = 3): Task<string> => ({
    id,
    type: "email-job",
    payload: `payload-${id}`,
    retries,
    maxRetries: 3,
    enqueuedAt: Date.now() - 5000,
    startedAt: Date.now() - 1000
  });

  describe("categorizeError", () => {
    it("identifies transient errors correctly", () => {
      expect(categorizeError("Gateway timeout 504")).toBe("transient");
      expect(categorizeError("Rate limit exceeded")).toBe("transient");
      expect(categorizeError("ECONNRESET by peer")).toBe("transient");
      expect(categorizeError("ECONNREFUSED 127.0.0.1")).toBe("transient");
      expect(categorizeError("Service temporarily unavailable 503")).toBe("transient");
      expect(categorizeError("Worker pool overloaded")).toBe("transient");
      expect(categorizeError("network socket closed")).toBe("transient");
      expect(categorizeError("Bad Gateway 502")).toBe("transient");
    });

    it("identifies permanent errors correctly", () => {
      expect(categorizeError("Invalid schema payload")).toBe("permanent");
      expect(categorizeError("NullPointerException in user code")).toBe("permanent");
      expect(categorizeError("Authorization failed 401")).toBe("permanent");
      expect(categorizeError("File not found on disk")).toBe("permanent");
    });
  });

  describe("DeadLetterQueue operations", () => {
    it("pushes tasks and tracks detailed entry information", () => {
      const task = createTask("t1", 3);
      const errHistory = [
        { timestamp: Date.now() - 2000, attempt: 1, message: "timeout" },
        { timestamp: Date.now() - 1000, attempt: 2, message: "rate limit" }
      ];

      const entry = dlq.push(task, new Error("timeout after 5000ms"), errHistory);

      expect(entry.task.id).toBe("t1");
      expect(entry.totalAttempts).toBe(3);
      expect(entry.errorCategory).toBe("transient");
      expect(entry.lastError).toBe("timeout after 5000ms");
      expect(entry.errorHistory).toHaveLength(2);

      const retrieved = dlq.get("t1");
      expect(retrieved).toBeDefined();
      expect(retrieved?.task.id).toBe("t1");
    });

    it("aggregates comprehensive metrics across categories, reasons and retries", () => {
      dlq.push(createTask("t1", 2), new Error("network timeout"), []);
      dlq.push(createTask("t2", 3), new Error("network timeout"), []);
      dlq.push(createTask("t3", 3), new Error("Invalid schema syntax"), []);

      const metrics = dlq.getMetrics();
      expect(metrics.totalDead).toBe(3);
      expect(metrics.transientCount).toBe(2);
      expect(metrics.permanentCount).toBe(1);
      expect(metrics.failureReasons["network timeout"]).toBe(2);
      expect(metrics.failureReasons["Invalid schema syntax"]).toBe(1);
      expect(metrics.retryDistribution[2]).toBe(1);
      expect(metrics.retryDistribution[3]).toBe(2);
    });

    it("replays single task by id, resetting retries and timestamps", () => {
      const task = createTask("t-replay", 3);
      dlq.push(task, new Error("transient network 503"), []);

      const replayed = dlq.replay("t-replay");
      expect(replayed).toBeDefined();
      expect(replayed?.id).toBe("t-replay");
      expect(replayed?.retries).toBe(0);
      expect(replayed?.startedAt).toBeUndefined();

      // Dead letter queue should no longer hold the replayed task
      expect(dlq.get("t-replay")).toBeUndefined();
      expect(dlq.getMetrics().totalDead).toBe(0);
    });

    it("replays all tasks or conditionally via predicate", () => {
      dlq.push(createTask("t-transient-1"), new Error("503 timeout"), []);
      dlq.push(createTask("t-perm-1"), new Error("SyntaxError"), []);
      dlq.push(createTask("t-transient-2"), new Error("overloaded"), []);

      // Filtered replay: only transient errors
      const transientReplayed = dlq.replayBatch((entry) => entry.errorCategory === "transient");
      expect(transientReplayed).toHaveLength(2);
      expect(transientReplayed.map((t) => t.id)).toEqual(["t-transient-1", "t-transient-2"]);

      // Only permanent remains in DLQ
      expect(dlq.getMetrics().totalDead).toBe(1);
      expect(dlq.get("t-perm-1")).toBeDefined();

      // Replay all remaining
      const remainingReplayed = dlq.replayAll();
      expect(remainingReplayed).toHaveLength(1);
      expect(remainingReplayed[0].id).toBe("t-perm-1");
      expect(dlq.getMetrics().totalDead).toBe(0);
    });

    it("prunes expired entries older than maxAgeMs and supports clear", () => {
      dlq.push(createTask("old-1"), new Error("err1"), []);
      
      // Advance virtual clock by 60 seconds
      vi.advanceTimersByTime(60000);

      dlq.push(createTask("new-1"), new Error("err2"), []);

      // Prune anything older than 30s -> should prune old-1
      const pruned = dlq.pruneOlderThan(30000);
      expect(pruned).toBe(1);
      expect(dlq.get("old-1")).toBeUndefined();
      expect(dlq.get("new-1")).toBeDefined();

      // Clear all
      dlq.clear();
      expect(dlq.getMetrics().totalDead).toBe(0);
      expect(Object.keys(dlq.getMetrics().failureReasons)).toHaveLength(0);
    });
  });
});
