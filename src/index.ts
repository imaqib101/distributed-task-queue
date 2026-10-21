export interface Task<T = any> {
  id: string;
  payload: T;
  retries: number;
  maxRetries: number;
}

export class DeadLetterQueue<T = any> {
  private dlq: Task<T>[] = [];

  push(task: Task<T>) {
    this.dlq.push(task);
  }

  getMetrics() {
    return {
      totalDead: this.dlq.length,
      tasks: [...this.dlq]
    };
  }
}

export class TaskQueue<T = any> {
  private queue: Task<T>[] = [];
  public dlq = new DeadLetterQueue<T>();

  enqueue(task: Task<T>) {
    this.queue.push(task);
  }

  async processNext(handler: (task: Task<T>) => Promise<void>) {
    const task = this.queue.shift();
    if (!task) return false;

    try {
      await handler(task);
    } catch (err) {
      task.retries++;
      if (task.retries >= task.maxRetries) {
        this.dlq.push(task);
      } else {
        this.queue.push(task);
      }
    }
    return true;
  }
}
