/**
 * In-memory Redis Streams-style Consumer Group
 *
 * Models the core Redis Streams primitives:
 *   xadd        – append a message to the stream
 *   xreadgroup  – claim pending messages for a consumer
 *   xack        – acknowledge processed messages
 *   xpending    – inspect unacknowledged messages per consumer
 *   xclaim      – steal idle messages from another consumer (rebalance)
 *   xlen        – current stream length
 *
 * Intentionally avoids I/O so the whole unit is synchronous and testable
 * without a real Redis instance.
 */

export interface StreamMessage<T = Record<string, unknown>> {
  id: string;       // "<ms>-<seq>" e.g. "1700000000000-0"
  data: T;
  insertedAt: number;
}

export interface PendingEntry {
  messageId: string;
  consumer: string;
  deliveryCount: number;
  lastDeliveredAt: number;
}

export interface XPendingResult {
  total: number;
  entries: PendingEntry[];
}

export interface XReadResult<T> {
  messages: StreamMessage<T>[];
}

let _seqCounter = 0;

function makeId(): string {
  const ms = Date.now();
  return `${ms}-${_seqCounter++}`;
}

export class StreamConsumerGroup<T = Record<string, unknown>> {
  /** All messages ever added to the stream */
  private readonly stream: StreamMessage<T>[] = [];

  /**
   * Index of the last message delivered to this group (delivery cursor).
   * -1 means "deliver from the beginning".
   * null (only when startId="$") means "deliver from the stream length
   * at the time of the FIRST xreadgroup call".
   */
  private lastDeliveredIndex: number | null;

  /**
   * Pending Entry List (PEL) — messages delivered but not yet ACK'd.
   * key: messageId
   */
  private pending = new Map<string, PendingEntry>();

  constructor(
    public readonly groupName: string,
    /** Starting position: "0" = from-beginning, "$" = new-only */
    startId: "0" | "$" = "0"
  ) {
    // null is the sentinel for "defer cursor init to first xreadgroup call"
    this.lastDeliveredIndex = startId === "$" ? null : -1;
  }

  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Append a new message to the stream.
   * Returns the generated message ID.
   */
  xadd(data: T): string {
    const id = makeId();
    this.stream.push({ id, data, insertedAt: Date.now() });
    return id;
  }

  /**
   * Deliver up to `count` undelivered messages to `consumer`.
   * Messages enter the PEL until xack'd.
   *
   * For "$" groups, the first xreadgroup call anchors the cursor to the
   * current stream tail — only messages added after this point are delivered.
   */
  xreadgroup(consumer: string, count = 1): XReadResult<T> {
    // Lazily anchor the cursor for "$" groups on their first xreadgroup call
    if (this.lastDeliveredIndex === null) {
      this.lastDeliveredIndex = this.stream.length - 1;
    }

    const messages: StreamMessage<T>[] = [];
    let cursor = this.lastDeliveredIndex;

    while (messages.length < count && cursor < this.stream.length - 1) {
      cursor++;
      const msg = this.stream[cursor];
      messages.push(msg);
      this.pending.set(msg.id, {
        messageId: msg.id,
        consumer,
        deliveryCount: 1,
        lastDeliveredAt: Date.now(),
      });
    }

    if (cursor > this.lastDeliveredIndex) {
      this.lastDeliveredIndex = cursor;
    }

    return { messages };
  }

  /**
   * Acknowledge that `consumer` has finished processing `messageId`.
   * Returns true if the message was in the PEL, false otherwise.
   */
  xack(consumer: string, messageId: string): boolean {
    const entry = this.pending.get(messageId);
    if (!entry || entry.consumer !== consumer) return false;
    this.pending.delete(messageId);
    return true;
  }

  /**
   * Steal messages that have been idle for at least `minIdleMs` ms from
   * any consumer other than `newConsumer` (rebalancing / dead-worker takeover).
   * Returns the re-assigned messages.
   */
  xclaim(newConsumer: string, minIdleMs: number): StreamMessage<T>[] {
    const now = Date.now();
    const claimed: StreamMessage<T>[] = [];

    for (const [msgId, entry] of this.pending.entries()) {
      if (
        entry.consumer !== newConsumer &&
        now - entry.lastDeliveredAt >= minIdleMs
      ) {
        // Re-assign ownership
        this.pending.set(msgId, {
          ...entry,
          consumer: newConsumer,
          deliveryCount: entry.deliveryCount + 1,
          lastDeliveredAt: now,
        });
        const msg = this.stream.find((m) => m.id === msgId);
        if (msg) claimed.push(msg);
      }
    }

    return claimed;
  }

  /**
   * Return the pending (unacknowledged) messages for the group or a specific
   * consumer.
   */
  xpending(consumer?: string): XPendingResult {
    const entries: PendingEntry[] = [];
    for (const entry of this.pending.values()) {
      if (!consumer || entry.consumer === consumer) {
        entries.push({ ...entry });
      }
    }
    return { total: entries.length, entries };
  }

  /** Total number of messages ever added to the stream */
  xlen(): number {
    return this.stream.length;
  }

  /** Read-only snapshot of the raw stream (useful for introspection/tests) */
  snapshot(): ReadonlyArray<StreamMessage<T>> {
    return this.stream;
  }

  /**
   * Reset delivery cursor to beginning ('0') or to the end of the current stream ('$').
   */
  resetCursor(startId: "0" | "$" = "0"): void {
    if (startId === "0") {
      this.lastDeliveredIndex = -1;
    } else {
      this.lastDeliveredIndex = this.stream.length - 1;
    }
  }

  /**
   * Return list of active consumers currently holding pending messages.
   */
  getConsumers(): string[] {
    const consumers = new Set<string>();
    for (const entry of this.pending.values()) {
      consumers.add(entry.consumer);
    }
    return Array.from(consumers);
  }

  /**
   * Return the stream lag (number of messages produced but not yet delivered to this consumer group).
   */
  getLag(): number {
    if (this.lastDeliveredIndex === null) {
      return 0;
    }
    return Math.max(0, this.stream.length - 1 - this.lastDeliveredIndex);
  }

  /**
   * Trim older messages from head of stream to limit stream length (like Redis XTRIM MAXLEN).
   * Automatically adjusts lastDeliveredIndex cursor.
   * Returns number of trimmed messages.
   */
  xtrim(maxLen: number): number {
    const cap = Math.max(0, maxLen);
    if (this.stream.length <= cap) {
      return 0;
    }
    const toTrim = this.stream.length - cap;
    this.stream.splice(0, toTrim);
    if (this.lastDeliveredIndex !== null) {
      this.lastDeliveredIndex = Math.max(-1, this.lastDeliveredIndex - toTrim);
    }
    return toTrim;
  }

  /**
   * Delete a message from the stream and from PEL if present (like Redis XDEL).
   */
  xdel(messageId: string): boolean {
    const idx = this.stream.findIndex((m) => m.id === messageId);
    if (idx === -1) {
      return false;
    }
    this.stream.splice(idx, 1);
    if (this.lastDeliveredIndex !== null && idx <= this.lastDeliveredIndex) {
      this.lastDeliveredIndex = Math.max(-1, this.lastDeliveredIndex - 1);
    }
    this.pending.delete(messageId);
    return true;
  }

  /**
   * Automatically claim idle pending messages from other consumers (like Redis 6.2+ XAUTOCLAIM).
   */
  xautoclaim(
    newConsumer: string,
    minIdleMs: number,
    count = 100
  ): { messages: StreamMessage<T>[]; claimedCount: number } {
    const now = Date.now();
    const claimed: StreamMessage<T>[] = [];

    for (const [msgId, entry] of this.pending.entries()) {
      if (claimed.length >= count) break;
      if (entry.consumer !== newConsumer && now - entry.lastDeliveredAt >= minIdleMs) {
        this.pending.set(msgId, {
          ...entry,
          consumer: newConsumer,
          deliveryCount: entry.deliveryCount + 1,
          lastDeliveredAt: now,
        });
        const msg = this.stream.find((m) => m.id === msgId);
        if (msg) claimed.push(msg);
      }
    }

    return { messages: claimed, claimedCount: claimed.length };
  }
}
