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

  // ── resetCursor & getConsumers ──────────────────────────────────────────

  it("getConsumers returns active consumers with pending unacked messages", () => {
    group.xadd({ value: 1 });
    group.xadd({ value: 2 });
    group.xadd({ value: 3 });

    group.xreadgroup("worker-alpha", 1);
    group.xreadgroup("worker-beta", 1);

    expect(group.getConsumers().sort()).toEqual(["worker-alpha", "worker-beta"]);

    const pending = group.xpending("worker-alpha");
    group.xack("worker-alpha", pending.entries[0].messageId);

    expect(group.getConsumers()).toEqual(["worker-beta"]);
  });

  it("resetCursor resets delivery cursor to allow replay from beginning", () => {
    group.xadd({ value: 10 });
    group.xadd({ value: 20 });

    const first = group.xreadgroup("w1", 2);
    expect(first.messages).toHaveLength(2);

    const empty = group.xreadgroup("w1", 2);
    expect(empty.messages).toHaveLength(0);

    group.resetCursor("0");
    const replayed = group.xreadgroup("w2", 2);
    expect(replayed.messages).toHaveLength(2);
    expect(replayed.messages[0].data.value).toBe(10);
  });

  // ── getLag ──────────────────────────────────────────────────────────────

  it("getLag accurately tracks undelivered message backlog", () => {
    expect(group.getLag()).toBe(0);

    group.xadd({ value: 1 });
    group.xadd({ value: 2 });
    group.xadd({ value: 3 });
    expect(group.getLag()).toBe(3);

    group.xreadgroup("w1", 1);
    expect(group.getLag()).toBe(2);

    group.xreadgroup("w1", 2);
    expect(group.getLag()).toBe(0);

    group.xadd({ value: 4 });
    expect(group.getLag()).toBe(1);
  });

  // ── xtrim ───────────────────────────────────────────────────────────────

  it("xtrim limits stream length and adjusts cursor", () => {
    for (let i = 1; i <= 5; i++) {
      group.xadd({ value: i });
    }
    expect(group.xlen()).toBe(5);

    // Read 2 messages (cursor at index 1)
    group.xreadgroup("w1", 2);

    // Trim to 3 messages (drops index 0 and 1)
    const trimmed = group.xtrim(3);
    expect(trimmed).toBe(2);
    expect(group.xlen()).toBe(3);

    // Reading next should deliver value 3
    const next = group.xreadgroup("w1", 1);
    expect(next.messages).toHaveLength(1);
    expect(next.messages[0].data.value).toBe(3);
  });

  // ── xdel ────────────────────────────────────────────────────────────────

  it("xdel removes message from stream and cleans up PEL", () => {
    const id1 = group.xadd({ value: 100 });
    const id2 = group.xadd({ value: 200 });

    group.xreadgroup("w1", 1);
    expect(group.xpending("w1").total).toBe(1);

    const deleted = group.xdel(id1);
    expect(deleted).toBe(true);
    expect(group.xlen()).toBe(1);
    expect(group.xpending("w1").total).toBe(0);

    expect(group.xdel("non-existent-id")).toBe(false);
  });

  // ── xautoclaim ──────────────────────────────────────────────────────────

  it("xautoclaim automatically reassigns idle pending messages with count limit", () => {
    group.xadd({ value: 1 });
    group.xadd({ value: 2 });
    group.xadd({ value: 3 });

    group.xreadgroup("stalled-worker", 3);
    expect(group.xpending("stalled-worker").total).toBe(3);

    // Auto-claim up to 2 idle messages
    const { messages, claimedCount } = group.xautoclaim("recovering-worker", 0, 2);
    expect(claimedCount).toBe(2);
    expect(messages).toHaveLength(2);
    expect(group.xpending("recovering-worker").total).toBe(2);
    expect(group.xpending("stalled-worker").total).toBe(1);
  });

  // ── xrange / xrevrange ──────────────────────────────────────────────────

  it("xrange returns all messages with '-' and '+'", () => {
    const id1 = group.xadd({ value: 100 });
    const id2 = group.xadd({ value: 200 });
    const id3 = group.xadd({ value: 300 });

    const all = group.xrange();
    expect(all).toHaveLength(3);
    expect(all[0].id).toBe(id1);
    expect(all[2].id).toBe(id3);
  });

  it("xrange filters messages within range and respects count limit", () => {
    const id1 = group.xadd({ value: 10 });
    const id2 = group.xadd({ value: 20 });
    const id3 = group.xadd({ value: 30 });
    const id4 = group.xadd({ value: 40 });

    const sliced = group.xrange(id2, id3);
    expect(sliced).toHaveLength(2);
    expect(sliced[0].id).toBe(id2);
    expect(sliced[1].id).toBe(id3);

    const limited = group.xrange("-", "+", 2);
    expect(limited).toHaveLength(2);
    expect(limited[0].id).toBe(id1);
    expect(limited[1].id).toBe(id2);
  });

  it("xrevrange returns messages in reverse order with optional count", () => {
    const id1 = group.xadd({ value: 1 });
    const id2 = group.xadd({ value: 2 });
    const id3 = group.xadd({ value: 3 });

    const reversed = group.xrevrange();
    expect(reversed).toHaveLength(3);
    expect(reversed[0].id).toBe(id3);
    expect(reversed[1].id).toBe(id2);
    expect(reversed[2].id).toBe(id1);

    const revLimited = group.xrevrange(id3, id2, 1);
    expect(revLimited).toHaveLength(1);
    expect(revLimited[0].id).toBe(id3);
  });

  // ── xinfoStream / xinfoConsumers ────────────────────────────────────────

  it("xinfoStream reports correct stream stats, boundaries, lag, and counts", () => {
    const emptyInfo = group.xinfoStream();
    expect(emptyInfo.length).toBe(0);
    expect(emptyInfo.firstEntry).toBeNull();
    expect(emptyInfo.lastEntry).toBeNull();
    expect(emptyInfo.pendingCount).toBe(0);
    expect(emptyInfo.lag).toBe(0);

    const id1 = group.xadd({ value: 11 });
    const id2 = group.xadd({ value: 22 });
    const id3 = group.xadd({ value: 33 });

    group.xreadgroup("worker-1", 1);

    const info = group.xinfoStream();
    expect(info.length).toBe(3);
    expect(info.firstEntry?.id).toBe(id1);
    expect(info.lastEntry?.id).toBe(id3);
    expect(info.pendingCount).toBe(1);
    expect(info.consumerCount).toBe(1);
    expect(info.lag).toBe(2);
  });

  it("xinfoConsumers reports per-consumer pending count and idle time", () => {
    group.xadd({ value: 1 });
    group.xadd({ value: 2 });
    group.xadd({ value: 3 });

    group.xreadgroup("worker-a", 2);
    group.xreadgroup("worker-b", 1);

    const consumers = group.xinfoConsumers();
    expect(consumers).toHaveLength(2);

    const cA = consumers.find((c) => c.name === "worker-a");
    const cB = consumers.find((c) => c.name === "worker-b");

    expect(cA?.pendingCount).toBe(2);
    expect(cA?.idleMs).toBeGreaterThanOrEqual(0);
    expect(cB?.pendingCount).toBe(1);
    expect(cB?.idleMs).toBeGreaterThanOrEqual(0);
  });
});
