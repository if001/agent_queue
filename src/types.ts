export type QueueTaskType = "user" | "scheduled_recurring" | "scheduled_once";
export type QueueTaskAction = "mention" | "agent_input";
export type QueueTaskSource = "user" | "simple_pomdp" | "scheduled";

interface BaseQueueTask {
  id: string;
  type: QueueTaskType;
  action: QueueTaskAction;
  text: string;
  channelId: string;
  userId: string;
  targetThreadId: string;
  conversationVersion: number;
  source: QueueTaskSource;
  sourceInteractionId?: string;
  dueAt: string;
  intervalMinutes?: number;
  createdAt: string;
  locked: boolean;
}

export interface MentionQueueTask extends BaseQueueTask {
  type: "user";
  action: "mention";
  authorId: string;
  mentionsBot: boolean;
}

export interface AgentInputQueueTask extends BaseQueueTask {
  type: "scheduled_once" | "scheduled_recurring";
  action: "agent_input";
}

export type QueueTask = MentionQueueTask | AgentInputQueueTask;

export interface QueueStatusItem {
  id: string;
  type: QueueTask["type"];
  action: QueueTask["action"];
  dueAt: string;
  locked: boolean;
  targetThreadId: string;
  conversationVersion: number;
  textPreview: string;
}

export interface QueueStatus {
  now: string;
  counts: {
    total: number;
    locked: number;
    byType: Record<QueueTask["type"], number>;
    readyByType: Record<QueueTask["type"], number>;
  };
  next: QueueStatusItem[];
}

export interface QueueStore {
  dequeueReady(now: Date): Promise<QueueTask | null>;
  ack(taskId: string): Promise<void>;
  release(taskId: string, nextDueAt?: Date): Promise<void>;
  getLatestConversationVersion(threadId: string): Promise<number>;
}

export interface QueueStatusProvider {
  getStatus(now?: Date, limit?: number): Promise<QueueStatus>;
}

export interface QueueApi extends QueueStore, QueueStatusProvider {
  enqueueMention(input: {
    botId: string;
    userId: string;
    channelId: string;
    text: string;
    mentionsBot: boolean;
    dueAt?: Date;
  }): Promise<MentionQueueTask>;
  enqueueConversationInput(input: {
    botId: string;
    userId: string;
    channelId: string;
    text: string;
    source?: Exclude<QueueTaskSource, "user">;
    sourceInteractionId?: string;
    dueAt?: Date;
    intervalMinutes?: number;
  }): Promise<AgentInputQueueTask>;
  enqueueScheduledInput(input: {
    botId: string;
    userId: string;
    channelId: string;
    text: string;
    dueAt?: Date;
    intervalMinutes?: number;
  }): Promise<AgentInputQueueTask>;
}
