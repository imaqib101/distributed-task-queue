import { describe, it, expect, beforeEach } from "vitest";
import { StreamConsumerGroup } from "../src/streamConsumerGroup";

describe("StreamConsumerGroup — Redis Streams consumer group semantics", () => {
  let group: StreamConsumerGroup<{ value: number }>;

  beforeEach(() => {
    group = new StreamConsumerGroup("workers", "0");
  });

  // ── xadd / xlen ────────────────────────────────────────────────────────

  it("xadd appends messages and xlen reflects the stream length", () => {
    expect(group.xlen()).toBe(0);
    group.xadd({ value: 1 });
    group.xadd({ value: 2 });
    group.xadd({ value: 3 });
    expect(group.xlen()).toBe(3);
  });

  it("xadd returns unique monotonically ordered IDs", () => {
    const ids = [
      group.xadd({ value: 1 }),
      group.xadd({ value: 2 }),
      group.xadd({ value: 3 }),
    ];
    const numericIds = ids.map((id) => parseInt(id.split("-")[1], 10));
    expect(numericIds[0]).toBeLessThan(numericIds[1]);
    expect(numericIds[1]).toBeLessThan(numericIds[2]);
  });

  // ── xreadgroup ─────────────────────────────────────────────────────────

  it("xreadgroup delivers messages to a consumer and respects count", () => {
    group.xadd({ value: 10 });
    group.xadd({ value: 20 });
    group.xadd({ value: 30 });

    const { messages } = group.xreadgroup("consumer-A", 2);
    expect(messages).toHaveLength(2);
    expect(messages[0].data.value).toBe(10);
    expect(messages[1].data.value).toBe(20);
  });

  it("xreadgroup advances the delivery cursor so messages are not double-delivered", () => {
    group.xadd({ value: 1 });
    group.xadd({ value: 2 });

    const first = group.xreadgroup("consumer-A", 1);
    const second = group.xreadgroup("consumer-B", 1);

    expect(first.messages[0].data.value).toBe(1);
    expect(second.messages[0].data.value).toBe(2);
  });

  it("xreadgroup returns empty when all messages have been delivered", () => {
    group.xadd({ value: 1 });
    group.xreadgroup("consumer-A", 5);

    const { messages } = group.xreadgroup("consumer-B", 5);
    expect(messages).toHaveLength(0);
  });

  // ── xack ───────────────────────────────────────────────────────────────

  it("xack removes a message from the PEL and returns true", () => {
    group.xadd({ value: 42 });
    const { messages } = group.xreadgroup("worker-1", 1);
    const msgId = messages[0].id;

    expect(group.xpending("worker-1").total).toBe(1);
    const acked = group.xack("worker-1", msgId);
    expect(acked).toBe(true);
    expect(group.xpending("worker-1").total).toBe(0);
  });

  it("xack returns false when consumer does not own the message", () => {
    group.xadd({ value: 7 });
    const { messages } = group.xreadgroup("worker-A", 1);
    const msgId = messages[0].id;

    // worker-B did not receive this message
    const result = group.xack("worker-B", msgId);
    expect(result).toBe(false);
    // message is still pending for worker-A
    expect(group.xpending("worker-A").total).toBe(1);
  });

  it("xack returns false for a non-existent message ID", () => {
    expect(group.xack("worker-1", "9999999999999-0")).toBe(false);
  });

  // ── xpending ───────────────────────────────────────────────────────────

  it("xpending reports all unacknowledged messages across consumers", () => {
    group.xadd({ value: 1 });
    group.xadd({ value: 2 });
    group.xadd({ value: 3 });

    group.xreadgroup("consumer-A", 2);
    group.xreadgroup("consumer-B", 1);

    const all = group.xpending();
    expect(all.total).toBe(3);

    const forA = group.xpending("consumer-A");
    expect(forA.total).toBe(2);

    const forB = group.xpending("consumer-B");
    expect(forB.total).toBe(1);
  });

  it("xpending entry includes correct deliveryCount and consumer", () => {
    group.xadd({ value: 99 });
    const { messages } = group.xreadgroup("worker-Z", 1);
    const { entries } = group.xpending("worker-Z");

    expect(entries[0].consumer).toBe("worker-Z");
    expect(entries[0].messageId).toBe(messages[0].id);
    expect(entries[0].deliveryCount).toBe(1);
  });

  // ── xclaim ─────────────────────────────────────────────────────────────

  it("xclaim steals idle messages from a dead consumer and increments deliveryCount", async () => {
    group.xadd({ value: 55 });
    const { messages } = group.xreadgroup("dead-worker", 1);
    const msgId = messages[0].id;

    // Simulate idle time by backdating the PEL entry
    // We test with minIdleMs=0 to avoid real waits in unit tests
    const stolen = group.xclaim("rescue-worker", 0);

    expect(stolen).toHaveLength(1);
    expect(stolen[0].id).toBe(msgId);

    const { entries } = group.xpending("rescue-worker");
    expect(entries[0].consumer).toBe("rescue-worker");
    expect(entries[0].deliveryCount).toBe(2);
  });

  it("xclaim does not steal messages owned by the claiming consumer", () => {
    group.xadd({ value: 1 });
    group.xreadgroup("worker-A", 1);

    // worker-A claims its own — should get nothing
    const stolen = group.xclaim("worker-A", 0);
    expect(stolen).toHaveLength(0);
  });

  it("xclaim does not steal recently-delivered messages (minIdleMs guard)", async () => {
    group.xadd({ value: 1 });
    group.xreadgroup("busy-worker", 1);

    // minIdleMs=60000 → message delivered <60 s ago should not be stolen
    const stolen = group.xclaim("rescue-worker", 60_000);
    expect(stolen).toHaveLength(0);
  });

  // ── End-to-end consumer lifecycle ──────────────────────────────────────

  it("full lifecycle: produce → consume → ack clears PEL", () => {
    for (let i = 0; i < 5; i++) group.xadd({ value: i });

    const { messages: batch1 } = group.xreadgroup("w1", 3);
    const { messages: batch2 } = group.xreadgroup("w2", 2);

    expect(group.xpending().total).toBe(5);

    for (const m of batch1) group.xack("w1", m.id);
    for (const m of batch2) group.xack("w2", m.id);

    expect(group.xpending().total).toBe(0);
    expect(group.xlen()).toBe(5); // stream length unchanged
  });

  it("$ start position: messages added before first xreadgroup are skipped; only post-xreadgroup messages are delivered", () => {
    const lateGroup = new StreamConsumerGroup<{ value: number }>("late", "$");

    // These exist before the group's first xreadgroup call — should be skipped
    lateGroup.xadd({ value: 100 });
    lateGroup.xadd({ value: 200 });

    // First xreadgroup anchors the cursor to the current tail → delivers nothing
    const first = lateGroup.xreadgroup("consumer", 10);
    expect(first.messages).toHaveLength(0);

    // Messages added AFTER the first xreadgroup call are delivered
    lateGroup.xadd({ value: 300 });
    lateGroup.xadd({ value: 400 });

    const second = lateGroup.xreadgroup("consumer", 10);
    expect(second.messages).toHaveLength(2);
    expect(second.messages[0].data.value).toBe(300);
    expect(second.messages[1].data.value).toBe(400);
  });
});
