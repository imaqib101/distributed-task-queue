import { DeadLetterQueue } from "./dlq";
import { Task, TaskPriority, QueuePerformanceMetrics, ErrorHistoryEntry } from "./types";

export interface WorkerPoolOptions {
  concurrency?: number;
  drainTimeoutMs?: number;
}

const PRIORITY_WEIGHTS: Record<TaskPriority, number> = {
  CRITICAL: 0,
  HIGH: 1,
  NORMAL: 2,
  LOW: 3
};

export class TaskQueue<T = any> {
  private queue: Task<T>[] = [];
  private activeWorkers = 0;
  private scheduledRetries = 0;
  private isDraining = false;
  private latencies: number[] = [];
  private taskErrorHistories = new Map<string, ErrorHistoryEntry[]>();

  private totalProcessed = 0;
  private totalSucceeded = 0;
  private totalFailed = 0;

  public readonly dlq = new DeadLetterQueue<T>();
  public readonly concurrency: number;
  public readonly drainTimeoutMs: number;

  constructor(options: WorkerPoolOptions = {}) {
    this.concurrency = options.concurrency ?? 5;
    this.drainTimeoutMs = options.drainTimeoutMs ?? 5000;
  }

  enqueue(task: Task<T>): void {
    if (this.isDraining) {
      throw new Error("Cannot enqueue task: WorkerPool is currently draining for shutdown");
    }

    task.enqueuedAt = task.enqueuedAt ?? Date.now();
    task.priority = task.priority ?? "NORMAL";

    this.queue.push(task);
    this.sortQueueByPriority();
  }

  private sortQueueByPriority(): void {
    this.queue.sort((a, b) => {
      const pA = PRIORITY_WEIGHTS[a.priority ?? "NORMAL"];
      const pB = PRIORITY_WEIGHTS[b.priority ?? "NORMAL"];
      if (pA !== pB) return pA - pB;
      return (a.enqueuedAt ?? 0) - (b.enqueuedAt ?? 0);
    });
  }

  get queueLength(): number {
    return this.queue.length;
  }

  get activeWorkersCount(): number {
    return this.activeWorkers;
  }

  get scheduledRetriesCount(): number {
    return this.scheduledRetries;
  }

  async processNext(handler: (task: Task<T>) => Promise<void>): Promise<boolean> {
    if (this.queue.length === 0) return false;
    if (this.activeWorkers >= this.concurrency) return false;

    const task = this.queue.shift()!;
    this.activeWorkers++;
    const startTime = Date.now();

    try {
      await handler(task);

      const durationMs = Date.now() - startTime;
      this.latencies.push(durationMs);
      this.totalProcessed++;
      this.totalSucceeded++;
      this.taskErrorHistories.delete(task.id);
      return true;
    } catch (err: any) {
      const errorMsg = err?.message || "Task execution failed";
      task.retries++;

      let history = this.taskErrorHistories.get(task.id);
      if (!history) {
        history = [];
        this.taskErrorHistories.set(task.id, history);
      }
      history.push({
        timestamp: Date.now(),
        attempt: task.retries,
        message: errorMsg
      });

      if (task.retries >= task.maxRetries) {
        this.dlq.push(task, err instanceof Error ? err : new Error(errorMsg), history);
        this.taskErrorHistories.delete(task.id);
        this.totalProcessed++;
        this.totalFailed++;
      } else {
        // Calculate exponential backoff with jitter
        const initialDelay = task.initialDelayMs ?? 50;
        const factor = task.backoffFactor ?? 2;
        const delay = initialDelay * Math.pow(factor, task.retries - 1);
        const jitter = Math.floor(Math.random() * delay * 0.2);
        const waitTime = Math.max(1, delay + jitter);

        this.scheduledRetries++;
        setTimeout(() => {
          this.queue.push(task);
          this.sortQueueByPriority();
          this.scheduledRetries--;
        }, waitTime);
      }
      return false;
    } finally {
      this.activeWorkers--;
    }
  }

  async processAll(handler: (task: Task<T>) => Promise<void>): Promise<void> {
    while (this.queue.length > 0 || this.activeWorkers > 0 || this.scheduledRetries > 0) {
      while (this.queue.length > 0 && this.activeWorkers < this.concurrency) {
        this.processNext(handler);
      }
      await new Promise((res) => setTimeout(res, 10));
    }
  }

  redriveDLQ(): number {
    const replayed = this.dlq.replayAll();
    for (const t of replayed) {
      this.enqueue(t);
    }
    return replayed.length;
  }

  getPerformanceMetrics(): QueuePerformanceMetrics {
    const sorted = [...this.latencies].sort((a, b) => a - b);
    const count = sorted.length;

    const p50 = count > 0 ? sorted[Math.floor(count * 0.5)] : 0;
    const p95 = count > 0 ? sorted[Math.floor(count * 0.95)] : 0;
    const p99 = count > 0 ? sorted[Math.floor(count * 0.99)] : 0;
    const avg = count > 0 ? Math.round(sorted.reduce((a, b) => a + b, 0) / count) : 0;

    return {
      totalProcessed: this.totalProcessed,
      totalSucceeded: this.totalSucceeded,
      totalFailed: this.totalFailed,
      latencyPercentiles: { p50, p95, p99, avg }
    };
  }

  async drain(): Promise<void> {
    this.isDraining = true;
    const deadline = Date.now() + this.drainTimeoutMs;

    while (this.activeWorkers > 0 || this.scheduledRetries > 0) {
      if (Date.now() > deadline) {
        throw new Error(`Drain timeout reached with ${this.activeWorkers} active workers still running`);
      }
      await new Promise((r) => setTimeout(r, 20));
    }
  }
}
