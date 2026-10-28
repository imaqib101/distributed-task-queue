import { describe, it, expect, vi, afterEach } from "vitest";
import { DeadLetterQueue, categorizeError } from "../src/dlq";
import type { Task } from "../src/types";

const task = (id: string, retries = 3): Task => ({ id, payload: { id }, retries, maxRetries: 3 });
const history = [{ timestamp: 1, attempt: 1, message: "boom" }];

describe("categorizeError", () => {
  it.each([
    "Request timeout",
    "Rate limit hit",
    "ECONNRESET",
    "HTTP 503",
    "temporarily unavailable"
  ])("treats %s as transient", (msg) => expect(categorizeError(msg)).toBe("transient"));

  it("treats unrecognised errors as permanent", () => {
    expect(categorizeError("invalid payload")).toBe("permanent");
  });
});

describe("DeadLetterQueue", () => {
  afterEach(() => vi.useRealTimers());

  it("records entries and aggregates metrics", () => {
    const dlq = new DeadLetterQueue();
    dlq.push(task("a"), new Error("timeout"), history);
    dlq.push(task("b", 2), new Error("bad input"), history);
    dlq.push(task("c"), new Error("timeout"), history);

    const m = dlq.getMetrics();
    expect(m.totalDead).toBe(3);
    expect(m.transientCount).toBe(2);
    expect(m.permanentCount).toBe(1);
    expect(m.failureReasons).toEqual({ timeout: 2, "bad input": 1 });
    expect(m.retryDistribution).toEqual({ 2: 1, 3: 2 });
    expect(dlq.get("a")?.errorCategory).toBe("transient");
    expect(dlq.get("missing")).toBeUndefined();
  });

  it("replay removes the entry and resets retries", () => {
    const dlq = new DeadLetterQueue();
    dlq.push(task("a"), new Error("x"), history);
    const replayed = dlq.replay("a");
    expect(replayed?.retries).toBe(0);
    expect(replayed?.startedAt).toBeUndefined();
    expect(dlq.get("a")).toBeUndefined();
    expect(dlq.replay("a")).toBeNull();
  });

  it("replayAll and replayBatch drain the matching entries", () => {
    const dlq = new DeadLetterQueue();
    dlq.push(task("a"), new Error("timeout"), history);
    dlq.push(task("b"), new Error("nope"), history);
    const transient = dlq.replayBatch((e) => e.errorCategory === "transient");
    expect(transient.map((t) => t.id)).toEqual(["a"]);
    expect(dlq.getMetrics().totalDead).toBe(1);
    expect(dlq.replayAll().map((t) => t.id)).toEqual(["b"]);
    expect(dlq.getMetrics().totalDead).toBe(0);
  });

  it("pruneOlderThan drops only expired entries", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const dlq = new DeadLetterQueue();
    dlq.push(task("old"), new Error("x"), history);
    vi.setSystemTime(10_000);
    dlq.push(task("new"), new Error("x"), history);
    expect(dlq.pruneOlderThan(5_000)).toBe(1);
    expect(dlq.get("old")).toBeUndefined();
    expect(dlq.get("new")).toBeDefined();
  });

  it("clear empties entries and counters", () => {
    const dlq = new DeadLetterQueue();
    dlq.push(task("a"), new Error("x"), history);
    dlq.clear();
    expect(dlq.getMetrics()).toMatchObject({
      totalDead: 0,
      failureReasons: {},
      retryDistribution: {}
    });
  });
});
