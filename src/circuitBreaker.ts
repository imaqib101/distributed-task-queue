/**
 * Circuit Breaker for distributed task queues
 *
 * Implements the classic three-state circuit breaker pattern:
 *
 *   CLOSED     — Normal operation. Failures are counted.
 *   OPEN       — Tripped. All calls are rejected immediately with
 *                CircuitBreakerOpenError, protecting downstream systems.
 *   HALF_OPEN  — Probe state. A limited number of trial calls are allowed
 *                through. If they succeed the circuit closes; if any fails
 *                it reopens and the timeout resets.
 *
 * Usage with TaskQueue:
 *
 *   const cb = new CircuitBreaker({ failureThreshold: 5, resetTimeoutMs: 10_000 });
 *   const queue = new TaskQueue({ concurrency: 4 });
 *
 *   // Wrap per-task execution:
 *   await queue.processAll(async (task) => {
 *     await cb.execute(() => myHandler(task));
 *   });
 */

export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

export interface CircuitBreakerOptions {
  /** Number of consecutive failures before tripping to OPEN (default: 5) */
  failureThreshold?: number;
  /** How many consecutive successes in HALF_OPEN close the circuit (default: 2) */
  successThreshold?: number;
  /** Milliseconds to wait in OPEN state before probing via HALF_OPEN (default: 30 000) */
  resetTimeoutMs?: number;
  /** Maximum concurrent calls allowed in HALF_OPEN probe state (default: 1) */
  halfOpenMaxConcurrent?: number;
}

export interface CircuitBreakerMetrics {
  state: CircuitState;
  failureCount: number;
  successCount: number;
  totalCalls: number;
  totalFailures: number;
  lastFailureAt: number | null;
  lastStateChangeAt: number;
  halfOpenAttempts: number;
}

export class CircuitBreakerOpenError extends Error {
  readonly state: CircuitState;
  constructor(message = "Circuit breaker is OPEN — calls temporarily rejected") {
    super(message);
    this.name = "CircuitBreakerOpenError";
    this.state = "OPEN";
  }
}

export class CircuitBreaker {
  private state: CircuitState = "CLOSED";

  /** Consecutive failures in CLOSED state */
  private failureCount = 0;
  /** Consecutive successes in HALF_OPEN state */
  private halfOpenSuccesses = 0;
  /** Active concurrent calls in HALF_OPEN state */
  private halfOpenActive = 0;

  /** Absolute metrics (never reset) */
  private totalCalls = 0;
  private totalFailures = 0;
  private lastFailureAt: number | null = null;
  private lastStateChangeAt: number = Date.now();
  private openedAt: number | null = null;

  private readonly failureThreshold: number;
  private readonly successThreshold: number;
  private readonly resetTimeoutMs: number;
  private readonly halfOpenMaxConcurrent: number;

  constructor(options: CircuitBreakerOptions = {}) {
    this.failureThreshold = options.failureThreshold ?? 5;
    this.successThreshold = options.successThreshold ?? 2;
    this.resetTimeoutMs = options.resetTimeoutMs ?? 30_000;
    this.halfOpenMaxConcurrent = options.halfOpenMaxConcurrent ?? 1;
  }

  // ── Public API ────────────────────────────────────────────────────────────

  /**
   * Execute `fn` protected by the circuit breaker.
   * Throws `CircuitBreakerOpenError` if the circuit is open and the reset
   * timeout has not elapsed. Otherwise propagates `fn`'s own errors after
   * recording the failure.
   */
  async execute<T>(fn: () => Promise<T>): Promise<T> {
    this.transitionIfNeeded();

    if (this.state === "OPEN") {
      throw new CircuitBreakerOpenError();
    }

    if (this.state === "HALF_OPEN") {
      if (this.halfOpenActive >= this.halfOpenMaxConcurrent) {
        throw new CircuitBreakerOpenError(
          "Circuit breaker is HALF_OPEN — probe slot unavailable"
        );
      }
      this.halfOpenActive++;
    }

    this.totalCalls++;

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (err) {
      this.onFailure();
      throw err;
    } finally {
      if (this.state === "HALF_OPEN") {
        this.halfOpenActive = Math.max(0, this.halfOpenActive - 1);
      }
    }
  }

  /** Inspect current metrics without side-effects */
  getMetrics(): CircuitBreakerMetrics {
    return {
      state: this.state,
      failureCount: this.failureCount,
      successCount: this.halfOpenSuccesses,
      totalCalls: this.totalCalls,
      totalFailures: this.totalFailures,
      lastFailureAt: this.lastFailureAt,
      lastStateChangeAt: this.lastStateChangeAt,
      halfOpenAttempts: this.halfOpenActive,
    };
  }

  /** Manually force the circuit closed (e.g. after operator inspection) */
  reset(): void {
    this.transitionTo("CLOSED");
    this.failureCount = 0;
    this.halfOpenSuccesses = 0;
    this.halfOpenActive = 0;
    this.openedAt = null;
  }

  /** Current state as a string */
  get currentState(): CircuitState {
    return this.state;
  }

  // ── Internal transitions ──────────────────────────────────────────────────

  private transitionIfNeeded(): void {
    if (
      this.state === "OPEN" &&
      this.openedAt !== null &&
      Date.now() - this.openedAt >= this.resetTimeoutMs
    ) {
      this.transitionTo("HALF_OPEN");
    }
  }

  private onSuccess(): void {
    if (this.state === "HALF_OPEN") {
      this.halfOpenSuccesses++;
      if (this.halfOpenSuccesses >= this.successThreshold) {
        this.transitionTo("CLOSED");
        this.failureCount = 0;
        this.halfOpenSuccesses = 0;
      }
    } else {
      // CLOSED — reset consecutive failure streak
      this.failureCount = 0;
    }
  }

  private onFailure(): void {
    this.totalFailures++;
    this.lastFailureAt = Date.now();

    if (this.state === "HALF_OPEN") {
      // Any failure in HALF_OPEN immediately reopens
      this.transitionTo("OPEN");
      this.openedAt = Date.now();
      this.halfOpenSuccesses = 0;
    } else {
      // CLOSED
      this.failureCount++;
      if (this.failureCount >= this.failureThreshold) {
        this.transitionTo("OPEN");
        this.openedAt = Date.now();
      }
    }
  }

  private transitionTo(next: CircuitState): void {
    if (this.state !== next) {
      this.state = next;
      this.lastStateChangeAt = Date.now();
    }
  }
}
