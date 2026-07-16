import { FileQueueStore } from "./fileQueueStore";
import { AgentInputQueueTask, MentionQueueTask, QueueApi } from "./types";

interface QueueBackend extends Pick<FileQueueStore, "enqueueTask" | "dequeueReady" | "ack" | "release" | "getStatus"> {}

export const buildConversationThreadId = (
  channelId: string,
  userId: string,
): string => `${channelId}:${userId}`;

export const buildScheduledThreadId = (channelId: string): string =>
  `${channelId}:scheduled`;

export const parseConversationThreadId = (
  threadId: string,
): { channelId: string; userId: string } | null => {
  const separator = threadId.lastIndexOf(":");
  if (separator <= 0 || separator === threadId.length - 1) {
    return null;
  }
  const channelId = threadId.slice(0, separator);
  const userId = threadId.slice(separator + 1);
  if (userId === "scheduled") {
    return null;
  }
  return { channelId, userId };
};

export const createQueueApi = (store: QueueBackend): QueueApi => ({
  enqueueMention: async (input): Promise<MentionQueueTask> =>
    store.enqueueTask({
      type: "user",
      action: "mention",
      text: input.text,
      channelId: input.channelId,
      authorId: input.userId,
      mentionsBot: input.mentionsBot,
      targetThreadId: buildConversationThreadId(input.channelId, input.userId),
      dueAt: (input.dueAt ?? new Date()).toISOString(),
    }) as Promise<MentionQueueTask>,
  enqueueConversationInput: async (input): Promise<AgentInputQueueTask> =>
    store.enqueueTask({
      type: input.intervalMinutes ? "scheduled_recurring" : "scheduled_once",
      action: "agent_input",
      text: input.text,
      channelId: input.channelId,
      targetThreadId: buildConversationThreadId(input.channelId, input.userId),
      dueAt: (input.dueAt ?? new Date()).toISOString(),
      ...(input.intervalMinutes ? { intervalMinutes: input.intervalMinutes } : {}),
    }) as Promise<AgentInputQueueTask>,
  enqueueScheduledInput: async (input): Promise<AgentInputQueueTask> =>
    store.enqueueTask({
      type: input.intervalMinutes ? "scheduled_recurring" : "scheduled_once",
      action: "agent_input",
      text: input.text,
      channelId: input.channelId,
      targetThreadId: buildScheduledThreadId(input.channelId),
      dueAt: (input.dueAt ?? new Date()).toISOString(),
      ...(input.intervalMinutes ? { intervalMinutes: input.intervalMinutes } : {}),
    }) as Promise<AgentInputQueueTask>,
  dequeueReady: (now) => store.dequeueReady(now),
  ack: (taskId) => store.ack(taskId),
  release: (taskId, nextDueAt) => store.release(taskId, nextDueAt),
  getStatus: (now, limit) => store.getStatus(now, limit),
});

export const createInMemoryQueueApi = (): QueueApi => {
  const items: Array<MentionQueueTask | AgentInputQueueTask> = [];
  return createQueueApi({
    enqueueTask: async (input) => {
      const task = {
        id: `inline_${Date.now()}_${Math.floor(Math.random() * 1000)}`,
        ...input,
        createdAt: new Date().toISOString(),
        locked: false,
      } as MentionQueueTask | AgentInputQueueTask;
      items.push(task);
      return task;
    },
    dequeueReady: async (now) => {
      const idx = items.findIndex(
        (it) => !it.locked && new Date(it.dueAt).getTime() <= now.getTime(),
      );
      if (idx < 0) {
        return null;
      }
      const current = items[idx];
      if (!current) {
        return null;
      }
      items[idx] = { ...current, locked: true } as MentionQueueTask | AgentInputQueueTask;
      return items[idx] ?? null;
    },
    ack: async (id) => {
      const idx = items.findIndex((it) => it.id === id);
      if (idx >= 0) {
        items.splice(idx, 1);
      }
    },
    release: async (id) => {
      const idx = items.findIndex((it) => it.id === id);
      if (idx >= 0) {
        const current = items[idx];
        if (current) {
          items[idx] = { ...current, locked: false } as MentionQueueTask | AgentInputQueueTask;
        }
      }
    },
    getStatus: async (now = new Date(), limit = 5) => {
      const byType = {
        user: 0,
        scheduled_recurring: 0,
        scheduled_once: 0,
      } as Record<"user" | "scheduled_recurring" | "scheduled_once", number>;
      const readyByType = {
        user: 0,
        scheduled_recurring: 0,
        scheduled_once: 0,
      } as Record<"user" | "scheduled_recurring" | "scheduled_once", number>;
      let locked = 0;
      for (const item of items) {
        byType[item.type] += 1;
        if (item.locked) {
          locked += 1;
        }
        if (!item.locked && new Date(item.dueAt).getTime() <= now.getTime()) {
          readyByType[item.type] += 1;
        }
      }
      return {
        now: now.toISOString(),
        counts: {
          total: items.length,
          locked,
          byType,
          readyByType,
        },
        next: items
          .filter((item) => !item.locked)
          .slice(0, Math.max(0, limit))
          .map((item) => ({
            id: item.id,
            type: item.type,
            action: item.action,
            dueAt: item.dueAt,
            locked: item.locked,
            targetThreadId: item.targetThreadId,
            textPreview: item.text,
          })),
      };
    },
  });
};
