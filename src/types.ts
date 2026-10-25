export type TaskPriority = "CRITICAL" | "HIGH" | "NORMAL" | "LOW";

export interface Task<T = any> {
  id: string;
  payload: T;
  priority?: TaskPriority;
  retries: number;
  maxRetries: number;
  initialDelayMs?: number;
  backoffFactor?: number;
  enqueuedAt?: number;
  startedAt?: number;
}

export interface ErrorHistoryEntry {
  timestamp: number;
  attempt: number;
  message: string;
}

export interface DeadLetterEntry<T = any> {
  task: Task<T>;
  deadAt: number;
  totalAttempts: number;
  lastError: string;
  errorHistory: ErrorHistoryEntry[];
  errorCategory?: "transient" | "permanent";
}

export interface DLQMetrics {
  totalDead: number;
  transientCount: number;
  permanentCount: number;
  failureReasons: Record<string, number>;
  retryDistribution: Record<number, number>;
  tasks: DeadLetterEntry[];
}

export interface QueuePerformanceMetrics {
  totalProcessed: number;
  totalSucceeded: number;
  totalFailed: number;
  utilizationRate: number;
  activeWorkers: number;
  currentConcurrency: number;
  latencyPercentiles: {
    p50: number;
    p95: number;
    p99: number;
    avg: number;
  };
}

export interface AdaptiveConcurrencyOptions {
  minConcurrency?: number;
  maxConcurrency?: number;
  backlogScaleThreshold?: number; // queue depth to trigger concurrency increase
  errorRateThreshold?: number; // 0.0 - 1.0 (e.g. 0.20 = 20% errors triggers scale-down)
}
