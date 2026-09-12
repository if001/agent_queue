import { mkdir, open, readFile, rename, stat, unlink, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import {
  AgentInputQueueTask,
  MentionQueueTask,
  QueueError,
  QueueErrorRecord,
  QueueStatus,
  QueueTask,
} from "./types";

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

interface QueueErrorState {
  errors: QueueErrorRecord[];
}

export class FileQueueStore {
  private mutationQueue: Promise<void> = Promise.resolve();

  constructor(private readonly filePath: string) {}

  enqueueMentionTask(input: MentionQueueTaskInput): Promise<MentionQueueTask> {
    return this.mutate(() => this.enqueueMentionTaskUnlocked(input));
  }

  private async enqueueMentionTaskUnlocked(input: MentionQueueTaskInput): Promise<MentionQueueTask> {
    const state = await this.readState();
    await this.archiveTerminalTasks(state);
    const conversationVersion =
      (state.conversationVersions[input.targetThreadId] ?? 0) + 1;
    state.conversationVersions[input.targetThreadId] = conversationVersion;
    const pending = state.tasks.find(
      (item): item is MentionQueueTask =>
        item.action === "mention" &&
        item.targetThreadId === input.targetThreadId &&
        !item.locked &&
        !item.failedAt,
    );
    const processing = state.tasks.find(
      (item): item is MentionQueueTask =>
        item.action === "mention" &&
        item.targetThreadId === input.targetThreadId &&
        item.locked &&
        !item.failedAt,
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
    const archived = await this.archiveTerminalTasks(state);
    if (input.sourceInteractionId) {
      const duplicate = state.tasks.find(
        (task) =>
          task.sourceInteractionId === input.sourceInteractionId &&
          task.targetThreadId === input.targetThreadId &&
          !task.failedAt,
      );
      if (duplicate) {
        if (archived) {
          await this.writeState(state);
        }
        return duplicate;
      }
    }
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
    const archived = await this.archiveTerminalTasks(state);
    const items = state.tasks;
    const staleLockBefore = now.getTime() - 5 * 60 * 1000;
    for (const item of items) {
      if (item.locked && (!item.lockedAt || Date.parse(item.lockedAt) <= staleLockBefore)) {
        item.locked = false;
        delete item.lockedAt;
      }
    }
    const candidates = items
      .filter((item) => !item.locked && !item.failedAt && new Date(item.dueAt).getTime() <= now.getTime())
      .sort(comparePriority);
    const next = candidates[0];
    if (!next) {
      if (archived) {
        await this.writeState(state);
      }
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
    items[idx] = { ...current, locked: true, lockedAt: now.toISOString() };
    await this.writeState(state);
    return items[idx] ?? null;
  }

  ack(taskId: string): Promise<void> {
    return this.mutate(() => this.ackUnlocked(taskId));
  }

  private async ackUnlocked(taskId: string): Promise<void> {
    const state = await this.readState();
    const archived = await this.archiveTerminalTasks(state);
    const items = state.tasks;
    const index = items.findIndex((item) => item.id === taskId);
    if (index < 0) {
      if (archived) {
        await this.writeState(state);
      }
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
        lockedAt: undefined,
      };
    } else {
      items.splice(index, 1);
    }
    await this.writeState(state);
  }

  release(taskId: string, nextDueAt?: Date, error?: QueueError): Promise<void> {
    return this.mutate(() => this.releaseUnlocked(taskId, nextDueAt, error));
  }

  private async releaseUnlocked(taskId: string, nextDueAt?: Date, error?: QueueError): Promise<void> {
    const state = await this.readState();
    const archived = await this.archiveTerminalTasks(state);
    const items = state.tasks;
    const index = items.findIndex((item) => item.id === taskId);
    if (index < 0) {
      if (archived) {
        await this.writeState(state);
      }
      return;
    }
    const target = items[index];
    if (!target) {
      return;
    }
    const attempts = (target.attempts ?? 0) + (error ? 1 : 0);
    if (error && attempts >= 3) {
      await this.appendErrors([
        toQueueErrorRecord(target, attempts, new Date().toISOString(), error),
      ]);
      items.splice(index, 1);
      await this.writeState(state);
      return;
    }
    items[index] = {
      ...target,
      attempts,
      ...(error ? { lastError: error } : {}),
      dueAt: (nextDueAt ?? new Date(Date.now() + 30_000)).toISOString(),
      locked: false,
      lockedAt: undefined,
    };
    await this.writeState(state);
  }

  getStatus(now: Date = new Date(), limit: number = 5): Promise<QueueStatus> {
    return this.mutate(async () => {
      const state = await this.readState();
      const archived = await this.archiveTerminalTasks(state);
      if (archived) {
        await this.writeState(state);
      }
      return this.buildStatus(state.tasks, now, limit);
    });
  }

  private buildStatus(items: QueueTask[], now: Date, limit: number): QueueStatus {
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
      if (
        !item.locked &&
        !item.failedAt &&
        new Date(item.dueAt).getTime() <= now.getTime()
      ) {
        readyByType[item.type] += 1;
      }
    }

    const next = items
      .filter((item) => !item.locked && !item.failedAt)
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

  getLatestConversationVersion(threadId: string): Promise<number> {
    return this.mutate(async () => {
      const state = await this.readState();
      const archived = await this.archiveTerminalTasks(state);
      if (archived) {
        await this.writeState(state);
      }
      return state.conversationVersions[threadId] ?? 0;
    });
  }

  private async archiveTerminalTasks(state: QueueState): Promise<boolean> {
    const failedTasks = state.tasks.filter((task) => task.failedAt);
    if (failedTasks.length === 0) {
      return false;
    }
    await this.appendErrors(
      failedTasks.map((task) =>
        toQueueErrorRecord(
          task,
          task.attempts ?? 0,
          task.failedAt as string,
          normalizeStoredError(task.lastError),
        ),
      ),
    );
    const failedIds = new Set(failedTasks.map((task) => task.id));
    state.tasks = state.tasks.filter((task) => !failedIds.has(task.id));
    return true;
  }

  private async appendErrors(records: QueueErrorRecord[]): Promise<void> {
    const errorFilePath = buildErrorFilePath(this.filePath);
    const state = await readErrorState(errorFilePath);
    const existingIds = new Set(state.errors.map((record) => record.taskId));
    for (const record of records) {
      if (!existingIds.has(record.taskId)) {
        state.errors.push(record);
        existingIds.add(record.taskId);
      }
    }
    await writeJsonFile(errorFilePath, state);
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
    await writeJsonFile(this.filePath, state);
  }

  private mutate<T>(operation: () => Promise<T>): Promise<T> {
    const lockedOperation = () => this.withFileLock(operation);
    const result = this.mutationQueue.then(lockedOperation, lockedOperation);
    this.mutationQueue = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private async withFileLock<T>(operation: () => Promise<T>): Promise<T> {
    const lockPath = `${this.filePath}.lock`;
    await mkdir(dirname(this.filePath), { recursive: true });
    for (let attempt = 0; attempt < 100; attempt += 1) {
      try {
        const handle = await open(lockPath, "wx");
        try {
          return await operation();
        } finally {
          await handle.close();
          await unlink(lockPath).catch(() => undefined);
        }
      } catch (error: unknown) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        const lockStat = await stat(lockPath).catch(() => null);
        if (lockStat && Date.now() - lockStat.mtimeMs > 30_000) {
          await unlink(lockPath).catch(() => undefined);
          continue;
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    }
    throw new Error(`queue lock timeout: ${lockPath}`);
  }
}

const emptyQueueState = (): QueueState => ({
  tasks: [],
  conversationVersions: {},
});

const emptyQueueErrorState = (): QueueErrorState => ({ errors: [] });

const buildErrorFilePath = (queueFilePath: string): string =>
  queueFilePath.endsWith(".json")
    ? `${queueFilePath.slice(0, -".json".length)}.errors.json`
    : `${queueFilePath}.errors.json`;

const readErrorState = async (filePath: string): Promise<QueueErrorState> => {
  try {
    const body = await readFile(filePath, "utf8");
    const parsed = JSON.parse(body) as QueueErrorState;
    return parsed && Array.isArray(parsed.errors) ? parsed : emptyQueueErrorState();
  } catch {
    return emptyQueueErrorState();
  }
};

const writeJsonFile = async (filePath: string, value: unknown): Promise<void> => {
  await mkdir(dirname(filePath), { recursive: true });
  const temporaryPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  await writeFile(temporaryPath, JSON.stringify(value, null, 2), "utf8");
  await rename(temporaryPath, filePath);
};

const normalizeStoredError = (error: QueueError | string | undefined): QueueError => {
  if (typeof error === "object" && error !== null) {
    return { name: error.name, message: error.message };
  }
  return { name: "Error", message: error ?? "handler failed" };
};

const toQueueErrorRecord = (
  task: QueueTask,
  attempts: number,
  failedAt: string,
  error: QueueError,
): QueueErrorRecord => ({
  taskId: task.id,
  type: task.type,
  action: task.action,
  source: task.source,
  targetThreadId: task.targetThreadId,
  createdAt: task.createdAt,
  failedAt,
  attempts,
  error,
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
