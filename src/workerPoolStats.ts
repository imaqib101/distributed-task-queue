export interface PoolSnapshot {
  timestamp: number;
  activeWorkers: number;
  queueDepth: number;
  throughputPerSec: number;
  p50LatencyMs: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
  errorRate: number; // 0-1
  dlqDepth: number;
}

export interface PoolHealthStatus {
  healthy: boolean;
  warnings: string[];
  snapshot: PoolSnapshot;
}

export class WorkerPoolStatsTracker {
  private snapshots: PoolSnapshot[] = [];
  private latencyWindow: number[] = [];
  private readonly windowSize: number;
  private completedInWindow: number = 0;
  private errorsInWindow: number = 0;
  private windowStart: number = Date.now();

  constructor(windowSize: number = 100) {
    this.windowSize = windowSize;
  }

  recordTask(latencyMs: number, success: boolean): void {
    this.latencyWindow.push(latencyMs);
    if (this.latencyWindow.length > this.windowSize) {
      this.latencyWindow.shift();
    }
    this.completedInWindow++;
    if (!success) {
      this.errorsInWindow++;
    }
  }

  getPercentile(p: number): number {
    if (this.latencyWindow.length === 0) return 0;
    const sorted = [...this.latencyWindow].sort((a, b) => a - b);
    const index = Math.ceil((p / 100) * sorted.length) - 1;
    const clampedIndex = Math.max(0, Math.min(index, sorted.length - 1));
    return sorted[clampedIndex] ?? 0;
  }

  snapshot(activeWorkers: number, queueDepth: number, dlqDepth: number): PoolSnapshot {
    const now = Date.now();
    const durationSec = Math.max(1, (now - this.windowStart) / 1000);
    const throughputPerSec = this.completedInWindow / durationSec;
    const totalInWindow = this.completedInWindow;
    const errorRate = totalInWindow > 0 ? this.errorsInWindow / totalInWindow : 0;

    const snap: PoolSnapshot = {
      timestamp: now,
      activeWorkers,
      queueDepth,
      throughputPerSec,
      p50LatencyMs: this.getPercentile(50),
      p95LatencyMs: this.getPercentile(95),
      p99LatencyMs: this.getPercentile(99),
      errorRate,
      dlqDepth
    };

    this.snapshots.push(snap);
    // Reset window counters
    this.completedInWindow = 0;
    this.errorsInWindow = 0;
    this.windowStart = now;

    return snap;
  }

  getHealth(
    s: PoolSnapshot,
    opts?: { maxErrorRate?: number; maxP95Ms?: number; maxQueueDepth?: number }
  ): PoolHealthStatus {
    const maxErrorRate = opts?.maxErrorRate ?? 0.05;
    const maxP95Ms = opts?.maxP95Ms ?? 1000;
    const maxQueueDepth = opts?.maxQueueDepth ?? 500;

    const warnings: string[] = [];
    if (s.errorRate > maxErrorRate) {
      warnings.push(
        `Error rate ${(s.errorRate * 100).toFixed(1)}% exceeds threshold ${(maxErrorRate * 100).toFixed(1)}%`
      );
    }
    if (s.p95LatencyMs > maxP95Ms) {
      warnings.push(`P95 latency ${s.p95LatencyMs}ms exceeds threshold ${maxP95Ms}ms`);
    }
    if (s.queueDepth > maxQueueDepth) {
      warnings.push(`Queue depth ${s.queueDepth} exceeds threshold ${maxQueueDepth}`);
    }

    return {
      healthy: warnings.length === 0,
      warnings,
      snapshot: s
    };
  }

  getSnapshots(): PoolSnapshot[] {
    return [...this.snapshots];
  }

  reset(): void {
    this.snapshots = [];
    this.latencyWindow = [];
    this.completedInWindow = 0;
    this.errorsInWindow = 0;
    this.windowStart = Date.now();
  }
}
