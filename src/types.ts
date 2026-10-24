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
}

export interface DLQMetrics {
  totalDead: number;
  failureReasons: Record<string, number>;
  retryDistribution: Record<number, number>;
  tasks: DeadLetterEntry[];
}

export interface QueuePerformanceMetrics {
  totalProcessed: number;
  totalSucceeded: number;
  totalFailed: number;
  latencyPercentiles: {
    p50: number;
    p95: number;
    p99: number;
    avg: number;
  };
}
