export interface DLQMessage {
  id: string;
  queueName: string;
  error: string;
  failedAt: string;
  attempts: number;
}

export interface DLQMetricsSummary {
  total: number;
  byQueue: Record<string, number>;
  topErrors: Array<{ error: string; count: number }>;
  oldestFailedAt: string | null;
}

export class DLQMonitor {
  private deadLetterQueue: DLQMessage[] = [];
  private alertThreshold: number;

  constructor(alertThreshold = 10) {
    this.alertThreshold = alertThreshold;
  }

  push(msg: Omit<DLQMessage, "failedAt">): boolean {
    this.deadLetterQueue.push({
      ...msg,
      failedAt: new Date().toISOString()
    });
    return this.deadLetterQueue.length >= this.alertThreshold;
  }

  getFailedMessages(queueName?: string): DLQMessage[] {
    if (!queueName) return [...this.deadLetterQueue];
    return this.deadLetterQueue.filter((m) => m.queueName === queueName);
  }

  clearQueue(queueName?: string): void {
    if (!queueName) {
      this.deadLetterQueue = [];
    } else {
      this.deadLetterQueue = this.deadLetterQueue.filter((m) => m.queueName !== queueName);
    }
  }

  getMetrics(queueName?: string): DLQMetricsSummary {
    const messages = this.getFailedMessages(queueName);
    const total = messages.length;
    const byQueue: Record<string, number> = {};
    const errorCounts: Record<string, number> = {};
    let oldestFailedAt: string | null = null;

    for (const msg of messages) {
      byQueue[msg.queueName] = (byQueue[msg.queueName] || 0) + 1;
      errorCounts[msg.error] = (errorCounts[msg.error] || 0) + 1;

      if (!oldestFailedAt || msg.failedAt < oldestFailedAt) {
        oldestFailedAt = msg.failedAt;
      }
    }

    const topErrors = Object.entries(errorCounts)
      .map(([error, count]) => ({ error, count }))
      .sort((a, b) => b.count - a.count);

    return {
      total,
      byQueue,
      topErrors,
      oldestFailedAt
    };
  }

  async replayMessage(id: string, handler: (msg: DLQMessage) => Promise<void>): Promise<boolean> {
    const index = this.deadLetterQueue.findIndex((m) => m.id === id);
    if (index === -1) return false;

    const msg = this.deadLetterQueue.splice(index, 1)[0]!;
    await handler(msg);
    return true;
  }
}
