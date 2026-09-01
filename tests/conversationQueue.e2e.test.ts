import { describe, expect, test } from "vitest";
import { createInMemoryQueueApi } from "../src";

describe("conversation queue fixture", () => {
  test("coalesces newer input while preserving monotonic conversation versions", async () => {
    const queue = createInMemoryQueueApi();
    const input = {
      botId: "ao",
      userId: "user-1",
      channelId: "channel-1",
      mentionsBot: true,
      dueAt: new Date("2026-09-01T00:00:00.000Z"),
    };

    const first = await queue.enqueueMention({ ...input, text: "first" });
    const processing = await queue.dequeueReady(input.dueAt);
    const second = await queue.enqueueMention({
      ...input,
      text: "second",
      mentionsBot: false,
    });
    const third = await queue.enqueueMention({
      ...input,
      text: "third",
      mentionsBot: false,
    });

    expect(first.conversationVersion).toBe(1);
    expect(processing?.conversationVersion).toBe(1);
    expect(second.id).toBe(third.id);
    expect(third.conversationVersion).toBe(3);
    expect(third.text).toContain("first");
    expect(third.text).toContain("second");
    expect(third.text).toContain("third");
    expect(await queue.getLatestConversationVersion(third.targetThreadId)).toBe(3);
  });
});
