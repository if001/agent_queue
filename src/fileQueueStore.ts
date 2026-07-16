import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { AgentInputQueueTask, MentionQueueTask, QueueStatus, QueueTask } from "./types";

type PersistedQueueTaskInput =
  | Omit<MentionQueueTask, "id" | "createdAt" | "locked">
  | Omit<AgentInputQueueTask, "id" | "createdAt" | "locked">;

export class FileQueueStore {
  constructor(private readonly filePath: string) {}

  async enqueueTask(input: PersistedQueueTaskInput): Promise<QueueTask> {
    const items = await this.readAll();
    const task: QueueTask = {
      id: `q_${Date.now()}_${Math.floor(Math.random() * 100000)}`,
      ...input,
      createdAt: new Date().toISOString(),
      locked: false,
    };
    items.push(task);
    await this.writeAll(items);
    return task;
  }

  async dequeueReady(now: Date): Promise<QueueTask | null> {
    const items = await this.readAll();
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
    await this.writeAll(items);
    return items[idx] ?? null;
  }

  async ack(taskId: string): Promise<void> {
    const items = await this.readAll();
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
      items[index] = { ...target, dueAt: nextDueAt, locked: false };
    } else {
      items.splice(index, 1);
    }
    await this.writeAll(items);
  }

  async release(taskId: string, nextDueAt?: Date): Promise<void> {
    const items = await this.readAll();
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
    await this.writeAll(items);
  }

  async getStatus(now: Date = new Date(), limit: number = 5): Promise<QueueStatus> {
    const items = await this.readAll();
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

  private async readAll(): Promise<QueueTask[]> {
    try {
      const body = await readFile(this.filePath, "utf8");
      const parsed = JSON.parse(body) as QueueTask[];
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  private async writeAll(items: QueueTask[]): Promise<void> {
    await mkdir(dirname(this.filePath), { recursive: true });
    await writeFile(this.filePath, JSON.stringify(items, null, 2), "utf8");
  }
}

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
