/**
 * Dead Letter Queue (DLQ) Metrics & Alerting Telemetry
 */
export interface DLQMessage {
  id: string;
  queueName: string;
  error: string;
  failedAt: string;
  attempts: number;
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
      failedAt: new Date().toISOString(),
    });
    return this.deadLetterQueue.length >= this.alertThreshold;
  }

  getFailedMessages(queueName?: string): DLQMessage[] {
    if (!queueName) return [...this.deadLetterQueue];
    return this.deadLetterQueue.filter(m => m.queueName === queueName);
  }

  clearQueue(queueName?: string): void {
    if (!queueName) {
      this.deadLetterQueue = [];
    } else {
      this.deadLetterQueue = this.deadLetterQueue.filter(m => m.queueName !== queueName);
    }
  }
}
