import { FileQueueStore } from "./fileQueueStore";
import { AgentInputQueueTask, MentionQueueTask, QueueApi } from "./types";

type QueueBackend = Pick<
  FileQueueStore,
  | "enqueueMentionTask"
  | "enqueueTask"
  | "dequeueReady"
  | "ack"
  | "release"
  | "getStatus"
  | "getLatestConversationVersion"
>;

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
    store.enqueueMentionTask({
      type: "user",
      action: "mention",
      text: input.text,
      channelId: input.channelId,
      userId: input.userId,
      authorId: input.userId,
      mentionsBot: input.mentionsBot,
      targetThreadId: buildConversationThreadId(input.channelId, input.userId),
      source: "user",
      dueAt: (input.dueAt ?? new Date()).toISOString(),
    }) as Promise<MentionQueueTask>,
  enqueueConversationInput: async (input): Promise<AgentInputQueueTask> => {
    const targetThreadId = buildConversationThreadId(
      input.channelId,
      input.userId,
    );
    return store.enqueueTask({
      type: input.intervalMinutes ? "scheduled_recurring" : "scheduled_once",
      action: "agent_input",
      text: input.text,
      channelId: input.channelId,
      userId: input.userId,
      targetThreadId,
      conversationVersion:
        await store.getLatestConversationVersion(targetThreadId),
      source: input.source ?? "scheduled",
      sourceInteractionId: input.sourceInteractionId,
      dueAt: (input.dueAt ?? new Date()).toISOString(),
      ...(input.intervalMinutes ? { intervalMinutes: input.intervalMinutes } : {}),
    }) as Promise<AgentInputQueueTask>;
  },
  enqueueScheduledInput: async (input): Promise<AgentInputQueueTask> => {
    const targetThreadId = buildConversationThreadId(
      input.channelId,
      input.userId,
    );
    return store.enqueueTask({
      type: input.intervalMinutes ? "scheduled_recurring" : "scheduled_once",
      action: "agent_input",
      text: input.text,
      channelId: input.channelId,
      userId: input.userId,
      targetThreadId,
      conversationVersion:
        await store.getLatestConversationVersion(targetThreadId),
      source: "scheduled",
      dueAt: (input.dueAt ?? new Date()).toISOString(),
      ...(input.intervalMinutes ? { intervalMinutes: input.intervalMinutes } : {}),
    }) as Promise<AgentInputQueueTask>;
  },
  dequeueReady: (now) => store.dequeueReady(now),
  ack: (taskId) => store.ack(taskId),
  release: (taskId, nextDueAt) => store.release(taskId, nextDueAt),
  getStatus: (now, limit) => store.getStatus(now, limit),
  getLatestConversationVersion: (threadId) =>
    store.getLatestConversationVersion(threadId),
});

export const createInMemoryQueueApi = (): QueueApi => {
  const items: Array<MentionQueueTask | AgentInputQueueTask> = [];
  const conversationVersions = new Map<string, number>();
  return createQueueApi({
    enqueueMentionTask: async (input) => {
      const nextVersion =
        (conversationVersions.get(input.targetThreadId) ?? 0) + 1;
      conversationVersions.set(input.targetThreadId, nextVersion);
      const pending = items.find(
        (item): item is MentionQueueTask =>
          item.action === "mention" &&
          item.targetThreadId === input.targetThreadId &&
          !item.locked,
      );
      const processing = items.find(
        (item): item is MentionQueueTask =>
          item.action === "mention" &&
          item.targetThreadId === input.targetThreadId &&
          item.locked,
      );
      if (pending) {
        pending.text = mergeUserInput(pending.text, input.text);
        pending.mentionsBot = pending.mentionsBot || input.mentionsBot;
        pending.conversationVersion = nextVersion;
        pending.dueAt = input.dueAt;
        return pending;
      }
      const task = {
        id: `inline_${Date.now()}_${Math.floor(Math.random() * 1000)}`,
        ...input,
        mentionsBot: processing?.mentionsBot || input.mentionsBot,
        text: processing
          ? mergeUserInput(processing.text, input.text)
          : input.text,
        conversationVersion: nextVersion,
        createdAt: new Date().toISOString(),
        locked: false,
      } as MentionQueueTask;
      items.push(task);
      return task;
    },
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
        const current = items[idx];
        if (
          current?.type === "scheduled_recurring" &&
          current.intervalMinutes
        ) {
          items[idx] = {
            ...current,
            conversationVersion:
              conversationVersions.get(current.targetThreadId) ?? 0,
            dueAt: new Date(
              Date.now() + current.intervalMinutes * 60 * 1000,
            ).toISOString(),
            locked: false,
          };
        } else {
          items.splice(idx, 1);
        }
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
            conversationVersion: item.conversationVersion,
            textPreview: item.text,
          })),
      };
    },
    getLatestConversationVersion: async (threadId) =>
      conversationVersions.get(threadId) ?? 0,
  });
};

const mergeUserInput = (previous: string, next: string): string =>
  `${previous}\n\nAdditional user message:\n${next}`;
