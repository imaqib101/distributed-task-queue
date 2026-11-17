import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  CircuitBreaker,
  CircuitBreakerOpenError,
} from "../src/circuitBreaker";

// ── helpers ───────────────────────────────────────────────────────────────

const ok = () => Promise.resolve("ok");
const fail = (msg = "downstream failure") =>
  Promise.reject(new Error(msg));

// ── suite ─────────────────────────────────────────────────────────────────

describe("CircuitBreaker", () => {
  let cb: CircuitBreaker;

  beforeEach(() => {
    cb = new CircuitBreaker({
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 500,
      halfOpenMaxConcurrent: 1,
    });
  });

  // ── CLOSED state ─────────────────────────────────────────────────────────

  it("starts CLOSED and passes calls through", async () => {
    const result = await cb.execute(ok);
    expect(result).toBe("ok");
    expect(cb.currentState).toBe("CLOSED");
  });

  it("increments totalCalls and totalFailures correctly", async () => {
    await cb.execute(ok);
    await cb.execute(ok).catch(() => {});
    await cb.execute(fail).catch(() => {});

    const m = cb.getMetrics();
    expect(m.totalCalls).toBe(3);
    expect(m.totalFailures).toBe(1);
  });

  it("resets consecutive failure count on success (CLOSED)", async () => {
    await cb.execute(fail).catch(() => {});
    await cb.execute(fail).catch(() => {});
    await cb.execute(ok); // resets streak
    await cb.execute(fail).catch(() => {});
    await cb.execute(fail).catch(() => {});

    // Still only 2 consecutive failures after the reset — should stay CLOSED
    expect(cb.currentState).toBe("CLOSED");
    expect(cb.getMetrics().failureCount).toBe(2);
  });

  // ── OPEN state ────────────────────────────────────────────────────────────

  it("trips to OPEN after failureThreshold consecutive failures", async () => {
    for (let i = 0; i < 3; i++) {
      await cb.execute(fail).catch(() => {});
    }
    expect(cb.currentState).toBe("OPEN");
  });

  it("rejects immediately with CircuitBreakerOpenError when OPEN", async () => {
    for (let i = 0; i < 3; i++) {
      await cb.execute(fail).catch(() => {});
    }

    await expect(cb.execute(ok)).rejects.toThrow(CircuitBreakerOpenError);
  });

  it("does not count CircuitBreakerOpenError rejections as additional failures", async () => {
    for (let i = 0; i < 3; i++) {
      await cb.execute(fail).catch(() => {});
    }
    // These are short-circuit rejections, totalFailures should stay at 3
    await cb.execute(ok).catch(() => {});
    await cb.execute(ok).catch(() => {});
    expect(cb.getMetrics().totalFailures).toBe(3);
  });

  // ── HALF_OPEN probe state ─────────────────────────────────────────────────

  it("transitions to HALF_OPEN after resetTimeoutMs elapses", async () => {
    vi.useFakeTimers();
    const cbFake = new CircuitBreaker({
      failureThreshold: 2,
      successThreshold: 2,
      resetTimeoutMs: 1_000,
    });

    for (let i = 0; i < 2; i++) {
      await cbFake.execute(fail).catch(() => {});
    }
    expect(cbFake.currentState).toBe("OPEN");

    // Trip the internal clock past resetTimeoutMs
    vi.advanceTimersByTime(1_001);

    // Next call triggers transitionIfNeeded → HALF_OPEN → success → counts
    let resolved = false;
    cbFake.execute(ok).then(() => { resolved = true; });

    // Allow microtask queue to flush
    await Promise.resolve();
    await Promise.resolve();

    expect(cbFake.currentState).toBe("HALF_OPEN");
    vi.useRealTimers();
  });

  it("closes the circuit after successThreshold successes in HALF_OPEN", async () => {
    // Trip to OPEN
    for (let i = 0; i < 3; i++) {
      await cb.execute(fail).catch(() => {});
    }

    // Manually inject HALF_OPEN by calling reset() then probing
    // (reset() + two successes mirrors the real path post-timeout)
    cb.reset();
    expect(cb.currentState).toBe("CLOSED");

    // Now use a separate instance already in HALF_OPEN to test that path directly
    const cbHalf = new CircuitBreaker({
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 50,
    });
    for (let i = 0; i < 3; i++) {
      await cbHalf.execute(fail).catch(() => {});
    }
    expect(cbHalf.currentState).toBe("OPEN");

    // Sleep past resetTimeout
    await new Promise((r) => setTimeout(r, 60));

    // First probe — still one success short
    await cbHalf.execute(ok);
    expect(cbHalf.currentState).toBe("HALF_OPEN");

    // Second probe — meets successThreshold → CLOSED
    await cbHalf.execute(ok);
    expect(cbHalf.currentState).toBe("CLOSED");
  });

  it("reopens immediately on any failure in HALF_OPEN", async () => {
    const cbHalf = new CircuitBreaker({
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 50,
    });
    for (let i = 0; i < 3; i++) {
      await cbHalf.execute(fail).catch(() => {});
    }
    await new Promise((r) => setTimeout(r, 60));

    // Probe fails → back to OPEN
    await cbHalf.execute(fail).catch(() => {});
    expect(cbHalf.currentState).toBe("OPEN");
  });

  it("enforces halfOpenMaxConcurrent limit and rejects extra probes", async () => {
    const cbHalf = new CircuitBreaker({
      failureThreshold: 2,
      successThreshold: 2,
      resetTimeoutMs: 50,
      halfOpenMaxConcurrent: 1,
    });
    for (let i = 0; i < 2; i++) {
      await cbHalf.execute(fail).catch(() => {});
    }
    await new Promise((r) => setTimeout(r, 60));

    // Launch two concurrent probes; the second should be rejected
    let slow: (() => void) | null = null;
    const slowFn = (): Promise<string> =>
      new Promise<string>((res) => { slow = () => res("done"); });

    const probe1 = cbHalf.execute(slowFn); // occupies the slot
    const probe2 = cbHalf.execute(ok);     // should be rejected

    await expect(probe2).rejects.toThrow(CircuitBreakerOpenError);

    // Resolve the first probe
    slow!();
    await probe1;
  });

  // ── reset() ───────────────────────────────────────────────────────────────

  it("reset() returns circuit to CLOSED and clears counters", async () => {
    for (let i = 0; i < 3; i++) {
      await cb.execute(fail).catch(() => {});
    }
    expect(cb.currentState).toBe("OPEN");

    cb.reset();
    expect(cb.currentState).toBe("CLOSED");
    expect(cb.getMetrics().failureCount).toBe(0);
  });

  it("after reset(), circuit trips again on failureThreshold hits", async () => {
    for (let i = 0; i < 3; i++) {
      await cb.execute(fail).catch(() => {});
    }
    cb.reset();

    for (let i = 0; i < 3; i++) {
      await cb.execute(fail).catch(() => {});
    }
    expect(cb.currentState).toBe("OPEN");
  });

  // ── getMetrics() ──────────────────────────────────────────────────────────

  it("getMetrics returns correct lastFailureAt timestamp", async () => {
    const before = Date.now();
    await cb.execute(fail).catch(() => {});
    const after = Date.now();

    const { lastFailureAt } = cb.getMetrics();
    expect(lastFailureAt).not.toBeNull();
    expect(lastFailureAt!).toBeGreaterThanOrEqual(before);
    expect(lastFailureAt!).toBeLessThanOrEqual(after);
  });

  it("getMetrics.lastFailureAt is null when no failures occurred", () => {
    expect(cb.getMetrics().lastFailureAt).toBeNull();
  });

  // ── propagation ───────────────────────────────────────────────────────────

  it("propagates the original error from fn (not wrapped)", async () => {
    const originalErr = new TypeError("network timeout");
    await expect(
      cb.execute(() => Promise.reject(originalErr))
    ).rejects.toThrow("network timeout");
  });

  it("returns the resolved value when fn succeeds", async () => {
    const value = await cb.execute(() => Promise.resolve(42));
    expect(value).toBe(42);
  });
});
