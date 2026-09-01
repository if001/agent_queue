import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { AgentInputQueueTask, MentionQueueTask, QueueStatus, QueueTask } from "./types";

type MentionQueueTaskInput = Omit<
  MentionQueueTask,
  "id" | "createdAt" | "locked" | "conversationVersion"
>;
type PersistedQueueTaskInput = Omit<
  AgentInputQueueTask,
  "id" | "createdAt" | "locked"
>;

interface QueueState {
  tasks: QueueTask[];
  conversationVersions: Record<string, number>;
}

export class FileQueueStore {
  private mutationQueue: Promise<void> = Promise.resolve();

  constructor(private readonly filePath: string) {}

  enqueueMentionTask(input: MentionQueueTaskInput): Promise<MentionQueueTask> {
    return this.mutate(() => this.enqueueMentionTaskUnlocked(input));
  }

  private async enqueueMentionTaskUnlocked(input: MentionQueueTaskInput): Promise<MentionQueueTask> {
    const state = await this.readState();
    const conversationVersion =
      (state.conversationVersions[input.targetThreadId] ?? 0) + 1;
    state.conversationVersions[input.targetThreadId] = conversationVersion;
    const pending = state.tasks.find(
      (item): item is MentionQueueTask =>
        item.action === "mention" &&
        item.targetThreadId === input.targetThreadId &&
        !item.locked,
    );
    const processing = state.tasks.find(
      (item): item is MentionQueueTask =>
        item.action === "mention" &&
        item.targetThreadId === input.targetThreadId &&
        item.locked,
    );
    if (pending) {
      pending.text = mergeUserInput(pending.text, input.text);
      pending.mentionsBot = pending.mentionsBot || input.mentionsBot;
      pending.conversationVersion = conversationVersion;
      pending.dueAt = input.dueAt;
      await this.writeState(state);
      return pending;
    }
    const task: MentionQueueTask = {
      id: buildTaskId(),
      ...input,
      mentionsBot: processing?.mentionsBot || input.mentionsBot,
      text: processing
        ? mergeUserInput(processing.text, input.text)
        : input.text,
      conversationVersion,
      createdAt: new Date().toISOString(),
      locked: false,
    };
    state.tasks.push(task);
    await this.writeState(state);
    return task;
  }

  enqueueTask(input: PersistedQueueTaskInput): Promise<QueueTask> {
    return this.mutate(() => this.enqueueTaskUnlocked(input));
  }

  private async enqueueTaskUnlocked(input: PersistedQueueTaskInput): Promise<QueueTask> {
    const state = await this.readState();
    const task: QueueTask = {
      id: buildTaskId(),
      ...input,
      createdAt: new Date().toISOString(),
      locked: false,
    };
    state.tasks.push(task);
    await this.writeState(state);
    return task;
  }

  dequeueReady(now: Date): Promise<QueueTask | null> {
    return this.mutate(() => this.dequeueReadyUnlocked(now));
  }

  private async dequeueReadyUnlocked(now: Date): Promise<QueueTask | null> {
    const state = await this.readState();
    const items = state.tasks;
    const candidates = items
      .filter((item) => !item.locked && new Date(item.dueAt).getTime() <= now.getTime())
      .sort(comparePriority);
    const next = candidates[0];
    if (!next) {
      return null;
    }
    const idx = items.findIndex((item) => item.id === next.id);
    if (idx < 0) {
      return null;
    }
    const current = items[idx];
    if (!current) {
      return null;
    }
    items[idx] = { ...current, locked: true };
    await this.writeState(state);
    return items[idx] ?? null;
  }

  ack(taskId: string): Promise<void> {
    return this.mutate(() => this.ackUnlocked(taskId));
  }

  private async ackUnlocked(taskId: string): Promise<void> {
    const state = await this.readState();
    const items = state.tasks;
    const index = items.findIndex((item) => item.id === taskId);
    if (index < 0) {
      return;
    }
    const target = items[index];
    if (!target) {
      return;
    }
    if (target.type === "scheduled_recurring" && target.intervalMinutes) {
      const nextDueAt = new Date(Date.now() + target.intervalMinutes * 60 * 1000).toISOString();
      items[index] = {
        ...target,
        conversationVersion:
          state.conversationVersions[target.targetThreadId] ?? 0,
        dueAt: nextDueAt,
        locked: false,
      };
    } else {
      items.splice(index, 1);
    }
    await this.writeState(state);
  }

  release(taskId: string, nextDueAt?: Date): Promise<void> {
    return this.mutate(() => this.releaseUnlocked(taskId, nextDueAt));
  }

  private async releaseUnlocked(taskId: string, nextDueAt?: Date): Promise<void> {
    const state = await this.readState();
    const items = state.tasks;
    const index = items.findIndex((item) => item.id === taskId);
    if (index < 0) {
      return;
    }
    const target = items[index];
    if (!target) {
      return;
    }
    items[index] = {
      ...target,
      dueAt: (nextDueAt ?? new Date(Date.now() + 30_000)).toISOString(),
      locked: false,
    };
    await this.writeState(state);
  }

  async getStatus(now: Date = new Date(), limit: number = 5): Promise<QueueStatus> {
    await this.mutationQueue;
    const items = (await this.readState()).tasks;
    const byType: Record<QueueTask["type"], number> = {
      user: 0,
      scheduled_recurring: 0,
      scheduled_once: 0,
    };
    const readyByType: Record<QueueTask["type"], number> = {
      user: 0,
      scheduled_recurring: 0,
      scheduled_once: 0,
    };

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

    const next = items
      .filter((item) => !item.locked)
      .sort(comparePriority)
      .slice(0, Math.max(0, limit))
      .map((item) => ({
        id: item.id,
        type: item.type,
        action: item.action,
        dueAt: item.dueAt,
        locked: item.locked,
        targetThreadId: item.targetThreadId,
        conversationVersion: item.conversationVersion,
        textPreview: toPreview(item.text),
      }));

    return {
      now: now.toISOString(),
      counts: {
        total: items.length,
        locked,
        byType,
        readyByType,
      },
      next,
    };
  }

  async getLatestConversationVersion(threadId: string): Promise<number> {
    await this.mutationQueue;
    const state = await this.readState();
    return state.conversationVersions[threadId] ?? 0;
  }

  private async readState(): Promise<QueueState> {
    try {
      const body = await readFile(this.filePath, "utf8");
      const parsed = JSON.parse(body) as QueueState;
      if (
        !parsed ||
        !Array.isArray(parsed.tasks) ||
        typeof parsed.conversationVersions !== "object"
      ) {
        return emptyQueueState();
      }
      return parsed;
    } catch {
      return emptyQueueState();
    }
  }

  private async writeState(state: QueueState): Promise<void> {
    await mkdir(dirname(this.filePath), { recursive: true });
    await writeFile(this.filePath, JSON.stringify(state, null, 2), "utf8");
  }

  private mutate<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.mutationQueue.then(operation, operation);
    this.mutationQueue = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}

const emptyQueueState = (): QueueState => ({
  tasks: [],
  conversationVersions: {},
});

const buildTaskId = (): string =>
  `q_${Date.now()}_${Math.floor(Math.random() * 100000)}`;

const mergeUserInput = (previous: string, next: string): string =>
  `${previous}\n\nAdditional user message:\n${next}`;

const priorityValue = (type: QueueTask["type"]): number => {
  if (type === "user") {
    return 0;
  }
  if (type === "scheduled_recurring") {
    return 1;
  }
  return 2;
};

const comparePriority = (a: QueueTask, b: QueueTask): number => {
  const pa = priorityValue(a.type);
  const pb = priorityValue(b.type);
  if (pa !== pb) {
    return pa - pb;
  }
  const due = new Date(a.dueAt).getTime() - new Date(b.dueAt).getTime();
  if (due !== 0) {
    return due;
  }
  return new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime();
};

const toPreview = (text: string): string => {
  const oneLine = text.replace(/\s+/g, " ").trim();
  return oneLine.length <= 120 ? oneLine : `${oneLine.slice(0, 117)}...`;
};
