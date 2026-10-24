import { DeadLetterEntry, DLQMetrics, Task } from "./types";

export class DeadLetterQueue<T = any> {
  private deadLetters = new Map<string, DeadLetterEntry<T>>();
  private failureReasonCounts = new Map<string, number>();
  private retryHistogram = new Map<number, number>();

  push(task: Task<T>, error: Error, errorHistory: { timestamp: number; attempt: number; message: string }[]): DeadLetterEntry<T> {
    const reason = error.message || "Unknown error";
    const entry: DeadLetterEntry<T> = {
      task,
      deadAt: Date.now(),
      totalAttempts: task.retries,
      lastError: reason,
      errorHistory: [...errorHistory]
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

    return {
      totalDead: this.deadLetters.size,
      failureReasons,
      retryDistribution,
      tasks: Array.from(this.deadLetters.values())
    };
  }

  replay(taskId: string): Task<T> | null {
    const entry = this.deadLetters.get(taskId);
    if (!entry) return null;

    this.deadLetters.delete(taskId);
    // Return fresh task ready for re-enqueuing
    return {
      ...entry.task,
      retries: 0
    };
  }

  replayAll(): Task<T>[] {
    const tasks: Task<T>[] = [];
    for (const entry of this.deadLetters.values()) {
      tasks.push({
        ...entry.task,
        retries: 0
      });
    }
    this.deadLetters.clear();
    return tasks;
  }

  purge(): void {
    this.deadLetters.clear();
    this.failureReasonCounts.clear();
    this.retryHistogram.clear();
  }
}
