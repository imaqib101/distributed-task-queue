import { DeadLetterEntry, DLQMetrics, Task } from "./types";

const TRANSIENT_PATTERNS = [
  /timeout/i,
  /rate limit/i,
  /econnreset/i,
  /econnrefused/i,
  /network/i,
  /503/,
  /502/,
  /504/,
  /temporar/i,
  /overloaded/i
];

export function categorizeError(errorMsg: string): "transient" | "permanent" {
  for (const pattern of TRANSIENT_PATTERNS) {
    if (pattern.test(errorMsg)) {
      return "transient";
    }
  }
  return "permanent";
}

export class DeadLetterQueue<T = any> {
  private deadLetters = new Map<string, DeadLetterEntry<T>>();
  private failureReasonCounts = new Map<string, number>();
  private retryHistogram = new Map<number, number>();

  push(
    task: Task<T>,
    error: Error,
    errorHistory: { timestamp: number; attempt: number; message: string }[]
  ): DeadLetterEntry<T> {
    const reason = error.message || "Unknown error";
    const category = categorizeError(reason);

    const entry: DeadLetterEntry<T> = {
      task,
      deadAt: Date.now(),
      totalAttempts: task.retries,
      lastError: reason,
      errorHistory: [...errorHistory],
      errorCategory: category
    };

    this.deadLetters.set(task.id, entry);

    // Track failure reasons
    const currentReasonCount = this.failureReasonCounts.get(reason) ?? 0;
    this.failureReasonCounts.set(reason, currentReasonCount + 1);

    // Track retry histogram
    const currentRetryCount = this.retryHistogram.get(task.retries) ?? 0;
    this.retryHistogram.set(task.retries, currentRetryCount + 1);

    return entry;
  }

  get(taskId: string): DeadLetterEntry<T> | undefined {
    return this.deadLetters.get(taskId);
  }

  getMetrics(): DLQMetrics {
    const failureReasons: Record<string, number> = {};
    for (const [reason, count] of this.failureReasonCounts.entries()) {
      failureReasons[reason] = count;
    }

    const retryDistribution: Record<number, number> = {};
    for (const [retries, count] of this.retryHistogram.entries()) {
      retryDistribution[retries] = count;
    }

    let transientCount = 0;
    let permanentCount = 0;

    for (const entry of this.deadLetters.values()) {
      if (entry.errorCategory === "transient") {
        transientCount++;
      } else {
        permanentCount++;
      }
    }

    return {
      totalDead: this.deadLetters.size,
      transientCount,
      permanentCount,
      failureReasons,
      retryDistribution,
      tasks: Array.from(this.deadLetters.values())
    };
  }

  replay(taskId: string): Task<T> | null {
    const entry = this.deadLetters.get(taskId);
    if (!entry) return null;

    this.deadLetters.delete(taskId);
    return {
      ...entry.task,
      retries: 0,
      enqueuedAt: Date.now(),
      startedAt: undefined
    };
  }

  replayAll(): Task<T>[] {
    const replayed: Task<T>[] = [];
    for (const entry of this.deadLetters.values()) {
      replayed.push({
        ...entry.task,
        retries: 0,
        enqueuedAt: Date.now(),
        startedAt: undefined
      });
    }
    this.deadLetters.clear();
    return replayed;
  }

  replayBatch(predicate: (entry: DeadLetterEntry<T>) => boolean): Task<T>[] {
    const replayed: Task<T>[] = [];
    for (const [id, entry] of this.deadLetters.entries()) {
      if (predicate(entry)) {
        replayed.push({
          ...entry.task,
          retries: 0,
          enqueuedAt: Date.now(),
          startedAt: undefined
        });
        this.deadLetters.delete(id);
      }
    }
    return replayed;
  }

  pruneOlderThan(maxAgeMs: number): number {
    const cutoff = Date.now() - maxAgeMs;
    let pruned = 0;
    for (const [id, entry] of this.deadLetters.entries()) {
      if (entry.deadAt < cutoff) {
        this.deadLetters.delete(id);
        pruned++;
      }
    }
    return pruned;
  }

  clear(): void {
    this.deadLetters.clear();
    this.failureReasonCounts.clear();
    this.retryHistogram.clear();
  }
}
