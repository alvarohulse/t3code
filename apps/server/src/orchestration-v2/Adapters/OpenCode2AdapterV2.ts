/**
 * The OpenCode 2 runtime behind the `opencode` driver. It talks to the
 * instance's `opencode serve` process through the HTTP client and reads that
 * server's `/api/event` stream, routed here by session id.
 *
 * A turn is one `session.prompt` (or `session.command`, or `session.compact`
 * for `/compact`); the session's next `session.execution.*` terminal ends it.
 * Plan mode is OpenCode's `plan` agent, switched before the prompt like the
 * model. Approvals, questions, subagents, steering, fork and rollback arrive
 * in later layers: sessions run in Full access only, with the `subagent` tool
 * denied, the capabilities below say no, and a permission or form that still
 * reaches a session is answered so the turn cannot hang.
 *
 * @module orchestration-v2/Adapters/OpenCode2AdapterV2
 */
import {
  AbsolutePath,
  Agent,
  Location,
  Model,
  Provider,
  Session,
  Skill,
  type OpenCodeEvent,
} from "@opencode/client/effect";
import { Mcp } from "@opencode/schema/mcp";
import type {
  OrchestrationV2ConversationMessage,
  OrchestrationV2ExecutionNode,
  OrchestrationV2ProviderCapabilities,
  OrchestrationV2ProviderSession,
  OrchestrationV2ProviderThread,
  OrchestrationV2ProviderTurn,
  OrchestrationV2TurnItem,
  ProviderInstanceId,
} from "@t3tools/contracts";
import type * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import { paginate, type OpenCode2StreamEvent } from "../../provider/opencode2/OpenCode2Client.ts";
import * as OpenCode2Server from "../../provider/opencode2/OpenCode2Server.ts";
import {
  parseOpenCodeModelSlug,
  type OpenCodeRuntimeError,
} from "../../provider/opencodeRuntime.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import { buildRuntimeInstructions } from "../../provider/RuntimeInstructions.ts";
import { t3OrchestrationSystemPrompt } from "../../provider/T3OrchestrationInstructions.ts";
import { SKILL_MENTION_PATTERN } from "@t3tools/shared/composerInlineTokens";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";

import { providerMessageTextWithAttachmentPaths } from "../AttachmentPrompt.ts";
import * as IdAllocator from "../IdAllocator.ts";
import { makeProviderFailure } from "../ProviderFailure.ts";
import {
  ProviderAdapterEnsureThreadError,
  ProviderAdapterEventStreamError,
  ProviderAdapterForkThreadError,
  ProviderAdapterInterruptError,
  ProviderAdapterOpenSessionError,
  ProviderAdapterProtocolError,
  ProviderAdapterReadThreadSnapshotError,
  ProviderAdapterResumeThreadError,
  ProviderAdapterRollbackThreadError,
  ProviderAdapterRuntimeRequestResponseError,
  ProviderAdapterSteerRunUnsupportedError,
  ProviderAdapterTurnStartError,
  ProviderAdapterV2,
  ProviderAdapterV2Error,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2SessionRuntime,
  type ProviderAdapterV2Shape,
  type ProviderAdapterV2TurnInput,
} from "../ProviderAdapter.ts";
import { turnScopedSelectionTransition } from "../ProviderSelectionTransition.ts";
import { OPENCODE_PROVIDER } from "./OpenCodeAdapterV2.ts";
import { openCodeToolTurnItem } from "./OpenCodeToolItems.ts";

const OpenCode2ProviderCapabilities = {
  sessions: {
    // One server serves every location, so one session runtime owns them all.
    supportsMultipleProviderThreadsPerSession: true,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: false,
    supportsSteeringByInterruptRestart: true,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: false,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: true,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "none",
  },
  // Sessions run with every tool allowed; the snapshot offers only Full access.
  runtimePolicy: { enforcement: "native" },
} satisfies OrchestrationV2ProviderCapabilities;

type EventOf<T extends OpenCodeEvent["type"]> = Extract<OpenCodeEvent, { readonly type: T }>;
type Tokens = EventOf<"session.step.ended">["data"]["tokens"];

interface ActiveTurn {
  readonly input: ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  /** Open text and reasoning blocks, keyed `<assistantMessageID>:<kind>:<ordinal>`. */
  readonly texts: Map<string, OpenBlock>;
  readonly tools: Map<string, { readonly name: string; input: Record<string, unknown> }>;
  readonly startedAt: Map<string, DateTime.Utc>;
  readonly ordinals: Map<string, number>;
  nextOrdinal: number;
  /** Input includes cache reads and writes, as 1.x reports it; the parts are also kept apart. */
  readonly usage: {
    input: number;
    cached: number;
    cacheWrite: number;
    output: number;
    reasoning: number;
  };
  steps: number;
  lastStep: Tokens | undefined;
  /**
   * The user (or compaction) message that started the turn: its prompt's
   * answer, or its inbox item for a command. A reconnect backfills from here.
   */
  promptId: string | undefined;
  /**
   * The session's newest history item before the turn started, or null when
   * the history was empty. A `session.command` answers without the id of the
   * item it queues, so a stream lost before its inbox event leaves only this
   * to backfill from: everything after it is the turn's.
   */
  before: string | null | undefined;
  /** The compaction running in this turn, `/compact` or OpenCode's own when the context fills. */
  compaction: { readonly nativeId: string; readonly startedAt: DateTime.Utc } | undefined;
  compactions: number;
  interrupted: boolean;
  /**
   * Set on a turn started after a timed-out Stop's run left the server: that
   * run's tail can still be on the stream, and everything before this turn's
   * own `session.execution.started` belongs to it.
   */
  awaitingStart: boolean;
}

interface OpenBlock {
  readonly block: { readonly assistantMessageID: string; readonly ordinal: number };
  readonly kind: "text" | "reasoning";
  readonly startedAt: DateTime.Utc;
  text: string;
}

interface ThreadState {
  providerThread: OrchestrationV2ProviderThread;
  readonly providerTurns: Map<string, OrchestrationV2ProviderTurn>;
  active: ActiveTurn | undefined;
  /** What the native session runs now, so a changed selection is switched before prompting. */
  model: ModelRef | undefined;
  /** The session's agent (`build`, `plan`, ...), undefined when OpenCode's default is in use. */
  agent: string | undefined;
  /** T3's MCP server as registered for this thread, and the instructions entry sent with it. */
  mcp:
    | { readonly name: string; readonly directory: string; readonly credential: string }
    | undefined;
  instructions: string | undefined;
  /**
   * Set when a turn ended here while OpenCode may still be running it: a Stop
   * that timed out, or a prompt whose request failed without a clear answer.
   * Execution events carry only the session id, so the next execution end
   * belongs to that run; it clears this and ends no turn.
   */
  unsettled: boolean;
}

/** One wording for every capability later layers add. */
const notYet = (feature: string) =>
  new ProviderAdapterProtocolError({
    driver: OPENCODE_PROVIDER,
    detail: `OpenCode 2 ${feature} is not supported yet`,
  });

const ref = (nativeId: string, strength: "strong" | "weak" = "strong") => ({
  driver: OPENCODE_PROVIDER,
  nativeId,
  strength,
});

const sessionIdOf = (providerThread: OrchestrationV2ProviderThread) => {
  const nativeId = providerThread.nativeThreadRef?.nativeId;
  return nativeId === undefined || nativeId === null
    ? Effect.fail(
        new ProviderAdapterProtocolError({
          driver: OPENCODE_PROVIDER,
          detail: `Provider thread ${providerThread.id} has no OpenCode session`,
        }),
      )
    : Effect.succeed(nativeId);
};

const textOf = (content: ReadonlyArray<{ readonly type: string; readonly text?: string }>) =>
  content.flatMap((part) => (part.type === "text" && part.text ? [part.text] : [])).join("\n");

/**
 * Every tool runs without asking, except `subagent`: a background child wakes
 * its parent in a turn T3 would not see. Both go when approvals and subagents land.
 */
const SESSION_PERMISSIONS = [
  { action: "*", resource: "*", effect: "allow" },
  { action: "subagent", resource: "*", effect: "deny" },
] as const;

type PermissionRule = {
  readonly action: string;
  readonly resource: string;
  readonly effect: "allow" | "deny" | "ask";
};

/**
 * T3's MCP server is registered per directory, not per session, so each thread
 * gets its own `t3-code-<thread>` entry with its own credential. OpenCode names
 * an MCP tool's permission `<server>_<tool>` (non-alphanumerics become `_`), and
 * the last matching rule wins: every thread's T3 server is denied, then this
 * thread's own is allowed again.
 */
const t3McpServerName = (threadId: string) =>
  `t3-code-${threadId.replaceAll(/[^a-zA-Z0-9_-]/g, "_")}`;
const sessionRules = (threadId: string): ReadonlyArray<PermissionRule> => [
  ...SESSION_PERMISSIONS,
  { action: "t3-code-*", resource: "*", effect: "deny" },
  { action: `${t3McpServerName(threadId)}_*`, resource: "*", effect: "allow" },
];

const sameRules = (
  left: ReadonlyArray<PermissionRule> | undefined,
  right: ReadonlyArray<PermissionRule>,
) =>
  left?.length === right.length &&
  left.every(
    (rule, index) =>
      rule.action === right[index]?.action &&
      rule.resource === right[index]?.resource &&
      rule.effect === right[index]?.effect,
  );

export const OPENCODE_2_FULL_ACCESS_ONLY =
  "OpenCode 2 needs Full access for now; approvals come in a later update. Switch this thread's mode to continue.";

// Questions are answered, not cancelled: a cancelled form ends the execution as a user stop.
const QUESTION_REPLY = "Questions aren't supported by this OpenCode integration yet.";

const INTERRUPT_TIMEOUT = "10 seconds";
/** The session instructions entry T3 writes its per-turn system prompt to. */
const INSTRUCTIONS_KEY = "t3-code";
/** How long a turn waits on the directory's commands or skills before sending the text as is. */
const INVENTORY_TIMEOUT = "5 seconds";
const ACTIVE_CHECK_TIMEOUT = "5 seconds";
/** A lost event stream is resubscribed this many times, this far apart, before the session breaks. */
const RECONNECT_ATTEMPTS = 5;
const RECONNECT_DELAY = "2 seconds";
const RECONCILE_TIMEOUT = "15 seconds";
/** How long a new turn waits for a reconnect in progress. */
const RECONNECT_WAIT = "30 seconds";
/** Answers that mean the server refused a prompt; any other failure may have been accepted. */
const CLEAR_PROMPT_REJECTIONS: ReadonlySet<string> = new Set([
  "InvalidRequestError",
  "ConflictError",
  "UnauthorizedError",
  "CommandNotFoundError",
  "SkillNotFoundError",
]);

export const OPENCODE_2_STILL_STOPPING =
  "OpenCode is still stopping the previous turn. Send the message again in a moment.";
const REQUEST_REPLY_TIMEOUT = "10 seconds";

/** Whether an answer to a paused request reached the server, trying twice. */
const deliver = <E>(answer: Effect.Effect<void, E>) =>
  answer.pipe(
    Effect.retry({ times: 1 }),
    Effect.timeout(REQUEST_REPLY_TIMEOUT),
    Effect.exit,
    Effect.map(Exit.isSuccess),
  );

type ModelRef = ReturnType<typeof Model.Ref.make>;

// Errors already in the adapter channel keep their tag; only lower-level ones are wrapped.
const isProviderAdapterError = Schema.is(ProviderAdapterV2Error);

/**
 * The model OpenCode should run for a `provider/model` slug and its reasoning
 * variant, or undefined for any other slug: sending none would run OpenCode's
 * default while T3 records the requested model.
 */
const modelRef = (selection: ProviderAdapterV2TurnInput["modelSelection"]) => {
  const parsed = parseOpenCodeModelSlug(selection.model);
  if (parsed === null) return undefined;
  const variant = getModelSelectionStringOptionValue(selection, "variant");
  return Model.Ref.make({
    providerID: Provider.ID.make(parsed.providerID),
    id: Model.ID.make(parsed.modelID),
    ...(variant === undefined ? {} : { variant: Model.VariantID.make(variant) }),
  });
};
const malformedModel = (model: string) =>
  `OpenCode model '${model}' must use provider/model format`;
const sameModel = (left: ModelRef, right: ModelRef | undefined) =>
  left.providerID === right?.providerID &&
  left.id === right?.id &&
  (left.variant ?? "default") === (right?.variant ?? "default");

/** OpenCode's own agents for T3's interaction modes; plan mode is its read-only `plan` agent. */
const agentFor = (input: ProviderAdapterV2TurnInput) =>
  input.runtimePolicy.interactionMode === "plan" ? "plan" : "build";

/** `/name args` naming one of the workspace's commands, which OpenCode expands itself. */
const commandOf = (text: string) => {
  const match = /^\/([^\s/]+)(?:\s+([\s\S]*))?$/.exec(text.trim());
  return match === null ? undefined : { name: match[1]!, text: match[2] ?? "" };
};

/** Whether a prompt names any skill at all, before the directory's skills are read. */
const SKILL_MENTION = new RegExp(SKILL_MENTION_PATTERN.source, "u");

/** The workspace skills a prompt names with the composer's `$skill` tokens. */
const skillsNamed = (text: string, known: ReadonlySet<string>) => [
  ...new Set(
    [...text.matchAll(SKILL_MENTION_PATTERN)].flatMap((match) =>
      known.has(match[2] ?? "") ? [match[2]!] : [],
    ),
  ),
];

/** The turn's own tokens: steps add up, and the last step's input is the live context size. */
const turnTokenUsage = (turn: ActiveTurn, status: OrchestrationV2ProviderTurn["status"]) =>
  turn.steps === 0
    ? {
        usageScope: "main_agent" as const,
        usageStatus: "unavailable" as const,
        hasSubagents: false,
      }
    : {
        usageScope: "main_agent" as const,
        usageStatus: status === "completed" ? ("complete" as const) : ("partial" as const),
        inputTokens: turn.usage.input,
        cachedInputTokens: turn.usage.cached,
        cacheCreationTokens: turn.usage.cacheWrite,
        outputTokens: turn.usage.output,
        reasoningTokens: turn.usage.reasoning,
        hasSubagents: false,
      };

/**
 * The adapter for one provider instance. It talks to the instance's
 * {@link OpenCode2Server.OpenCode2Server}, which the driver builds from the instance's settings.
 */
export const make = Effect.fn("OpenCode2Adapter.make")(function* (instanceId: ProviderInstanceId) {
  const server = yield* OpenCode2Server.OpenCode2Server;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const serverConfig = yield* ServerConfig;
  const driver = OPENCODE_PROVIDER;
  // Context windows by `provider/model`, from the latest `/api/model` read.
  const contextWindows = new Map<string, number>();

  /**
   * Lends the instance's server to a session until its scope closes. A spawned
   * server that died is started again on the next borrow.
   */
  const borrow = Effect.gen(function* () {
    const lent = yield* Deferred.make<OpenCode2Server.OpenCode2Connection, OpenCodeRuntimeError>();
    yield* server
      .withConnection((connection) =>
        Deferred.succeed(lent, connection).pipe(Effect.andThen(Effect.never)),
      )
      .pipe(
        Effect.catch((error) => Deferred.fail(lent, error)),
        Effect.forkScoped,
      );
    return yield* Deferred.await(lent);
  });

  const openSession = Effect.fn("OpenCode2Adapter.openSession")(function* (
    input: Parameters<ProviderAdapterV2Shape["openSession"]>[0],
    initial: {
      readonly connection: OpenCode2Server.OpenCode2Connection;
      readonly scope: Scope.Closeable;
    },
  ) {
    let connection = initial.connection;
    // Replaced when the session reconnects to a restarted server.
    let client = connection.client;
    const sessionScope = yield* Effect.scope;
    const now = yield* DateTime.now;
    let session: OrchestrationV2ProviderSession = {
      id: input.providerSessionId,
      driver,
      providerInstanceId: instanceId,
      status: "ready",
      cwd: input.runtimePolicy.cwd ?? serverConfig.cwd,
      model: input.modelSelection.model,
      capabilities: OpenCode2ProviderCapabilities,
      createdAt: now,
      updatedAt: now,
      lastError: null,
    };
    const events = yield* Queue.unbounded<ProviderAdapterV2Event, Cause.Done>();
    const threads = new Map<string, ThreadState>();
    const emit = (event: ProviderAdapterV2Event) => Queue.offer(events, event).pipe(Effect.asVoid);

    const setSessionStatus = (
      status: OrchestrationV2ProviderSession["status"],
      lastError: string | null,
    ) =>
      Effect.gen(function* () {
        session = { ...session, status, lastError, updatedAt: yield* DateTime.now };
        yield* emit({ type: "provider_session.updated", driver, providerSession: session });
      });

    const ordinalOf = (turn: ActiveTurn, nativeId: string) => {
      const known = turn.ordinals.get(nativeId);
      if (known !== undefined) return known;
      const next = turn.nextOrdinal++;
      turn.ordinals.set(nativeId, next);
      return next;
    };

    const itemBase = (
      state: ThreadState,
      turn: ActiveTurn,
      nativeId: string,
      status: OrchestrationV2TurnItem["status"],
      startedAt: DateTime.Utc,
      completedAt: DateTime.Utc | null,
      updatedAt: DateTime.Utc,
    ) => ({
      id: idAllocator.derive.turnItemFromProviderItem({ driver, nativeItemId: nativeId }),
      threadId: turn.input.threadId,
      runId: turn.input.runId,
      nodeId: idAllocator.derive.nodeFromProviderItem({ driver, nativeItemId: nativeId }),
      providerThreadId: state.providerThread.id,
      providerTurnId: turn.providerTurn.id,
      nativeItemRef: ref(nativeId),
      parentItemId: null,
      ordinal: ordinalOf(turn, nativeId),
      status,
      title: null,
      startedAt,
      completedAt,
      updatedAt,
    });

    const emitNode = (
      state: ThreadState,
      turn: ActiveTurn,
      nativeId: string,
      kind: OrchestrationV2ExecutionNode["kind"],
      status: OrchestrationV2ExecutionNode["status"],
      startedAt: DateTime.Utc,
      completedAt: DateTime.Utc | null,
    ) =>
      emit({
        type: "node.updated",
        driver,
        node: {
          id: idAllocator.derive.nodeFromProviderItem({ driver, nativeItemId: nativeId }),
          threadId: turn.input.threadId,
          runId: turn.input.runId,
          parentNodeId: turn.input.rootNodeId,
          rootNodeId: turn.input.rootNodeId,
          kind,
          status,
          countsForRun: false,
          providerThreadId: state.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          nativeItemRef: ref(nativeId),
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt,
          completedAt,
        },
      });

    /** One text or reasoning block, re-emitted with its accumulated text on every change. */
    const emitText = Effect.fnUntraced(function* (
      state: ThreadState,
      turn: ActiveTurn,
      data: {
        readonly assistantMessageID: string;
        readonly ordinal: number;
        readonly text?: string;
      },
      kind: "text" | "reasoning",
      update: (current: string) => string,
      completed = "text" in data,
    ) {
      const nativeId = `${data.assistantMessageID}:${kind}:${data.ordinal}`;
      const updatedAt = yield* DateTime.now;
      const entry = turn.texts.get(nativeId) ?? {
        block: data,
        kind,
        startedAt: updatedAt,
        text: "",
      };
      entry.text = update(entry.text);
      if (completed) turn.texts.delete(nativeId);
      else turn.texts.set(nativeId, entry);
      if (entry.text.length === 0) return;
      const status = completed ? "completed" : "running";
      const completedAt = completed ? updatedAt : null;
      const nodeKind = kind === "text" ? "assistant_message" : "reasoning";
      yield* emitNode(state, turn, nativeId, nodeKind, status, entry.startedAt, completedAt);
      const base = itemBase(state, turn, nativeId, status, entry.startedAt, completedAt, updatedAt);
      if (kind === "reasoning") {
        yield* emit({
          type: "turn_item.updated",
          driver,
          turnItem: { ...base, type: "reasoning", text: entry.text, streaming: !completed },
        });
        return;
      }
      const messageId = idAllocator.derive.messageFromProviderItem({
        driver,
        nativeItemId: nativeId,
      });
      const message: OrchestrationV2ConversationMessage = {
        createdBy: "agent",
        creationSource: "provider",
        id: messageId,
        threadId: turn.input.threadId,
        runId: turn.input.runId,
        nodeId: base.nodeId,
        role: "assistant",
        text: entry.text,
        attachments: [],
        streaming: !completed,
        createdAt: entry.startedAt,
        updatedAt,
      };
      yield* emit({ type: "message.updated", driver, message });
      yield* emit({
        type: "turn_item.updated",
        driver,
        turnItem: {
          ...base,
          type: "assistant_message",
          messageId,
          text: entry.text,
          streaming: !completed,
        },
      });
    });

    const emitTool = Effect.fnUntraced(function* (
      state: ThreadState,
      turn: ActiveTurn,
      id: string,
      status: "running" | "completed" | "failed" | "interrupted",
      result?: { readonly output: string | undefined; readonly metadata: unknown },
    ) {
      const tool = turn.tools.get(id);
      if (tool === undefined) return;
      const updatedAt = yield* DateTime.now;
      const startedAt = turn.startedAt.get(id) ?? updatedAt;
      const completedAt = status === "running" ? null : updatedAt;
      yield* emitNode(state, turn, id, "tool_call", status, startedAt, completedAt);
      yield* emit({
        type: "turn_item.updated",
        driver,
        turnItem: openCodeToolTurnItem(
          itemBase(state, turn, id, status, startedAt, completedAt, updatedAt),
          {
            name: tool.name,
            input: tool.input,
            output: result?.output,
            completedMetadata: status === "completed" ? result?.metadata : undefined,
          },
        ),
      });
    });

    /** The turn's compaction item; its summary is the text OpenCode carries forward. */
    const emitCompaction = Effect.fnUntraced(function* (
      state: ThreadState,
      turn: ActiveTurn,
      status: "running" | "completed" | "failed" | "interrupted",
      summary?: string,
    ) {
      const compaction = turn.compaction;
      if (compaction === undefined) return;
      const updatedAt = yield* DateTime.now;
      const completedAt = status === "running" ? null : updatedAt;
      yield* emit({
        type: "turn_item.updated",
        driver,
        turnItem: {
          ...itemBase(
            state,
            turn,
            compaction.nativeId,
            status,
            compaction.startedAt,
            completedAt,
            updatedAt,
          ),
          nodeId: turn.input.rootNodeId,
          type: "compaction",
          driver,
          title:
            status === "running"
              ? "Compacting context"
              : status === "completed"
                ? "Context compacted"
                : status === "failed"
                  ? "Compaction failed"
                  : "Compaction interrupted",
          ...(summary === undefined || summary.length === 0 ? {} : { summary }),
        },
      });
      if (status !== "running") turn.compaction = undefined;
    });

    const emitProviderTurn = (
      state: ThreadState,
      turn: ActiveTurn,
      providerTurn: OrchestrationV2ProviderTurn,
    ) => {
      state.providerTurns.set(String(providerTurn.id), providerTurn);
      return emit({
        type: "provider_turn.updated",
        driver,
        threadId: turn.input.threadId,
        providerTurn,
      });
    };

    const finishTurn = Effect.fnUntraced(function* (
      state: ThreadState,
      terminal:
        | { readonly status: "completed" | "interrupted" }
        | { readonly status: "failed"; readonly failure: ReturnType<typeof makeProviderFailure> },
      threadDisposition: "reusable" | "broken" = "reusable",
    ) {
      const turn = state.active;
      if (turn === undefined) return;
      state.active = undefined;
      const completedAt = yield* DateTime.now;
      // Blocks still open when the execution ends are final as they stand.
      for (const open of turn.texts.values()) {
        yield* emitText(state, turn, open.block, open.kind, (text) => text, true);
      }
      for (const id of turn.tools.keys()) {
        yield* emitTool(
          state,
          turn,
          id,
          terminal.status === "completed" ? "completed" : "interrupted",
        );
      }
      yield* emitCompaction(state, turn, terminal.status === "failed" ? "failed" : "interrupted");
      const window = contextWindows.get(turn.input.modelSelection.model);
      const lastStep = turn.lastStep;
      yield* emitProviderTurn(state, turn, {
        ...turn.providerTurn,
        status: terminal.status,
        completedAt,
        turnTokenUsage: turnTokenUsage(turn, terminal.status),
        ...(lastStep === undefined
          ? {}
          : {
              tokenUsage: {
                usedTokens:
                  lastStep.input + lastStep.cache.read + lastStep.cache.write + lastStep.output,
                maxTokens: window ?? null,
                inputTokens: lastStep.input + lastStep.cache.read + lastStep.cache.write,
                cachedInputTokens: lastStep.cache.read,
                outputTokens: lastStep.output,
                reasoningOutputTokens: lastStep.reasoning,
                updatedAt: DateTime.formatIso(completedAt),
              },
            }),
      });
      state.providerThread = {
        ...state.providerThread,
        status: threadDisposition === "broken" ? "error" : "idle",
        updatedAt: completedAt,
      };
      yield* emit({
        type: "provider_thread.updated",
        driver,
        providerThread: state.providerThread,
      });
      const anyActive = [...threads.values()].some((candidate) => candidate.active !== undefined);
      yield* setSessionStatus(anyActive ? "running" : "ready", null);
      const base = {
        type: "turn.terminal" as const,
        driver,
        providerThreadId: state.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        runOrdinal: turn.input.runOrdinal,
        threadDisposition,
      };
      yield* emit(
        terminal.status === "failed"
          ? {
              ...base,
              status: "failed",
              failure: terminal.failure,
              failureItemOrdinal: ordinalOf(turn, `terminal-failure:${turn.providerTurn.id}`),
            }
          : { ...base, status: terminal.status, failure: null },
      );
      // A spawned server lists its models lazily, so a window still unknown is
      // read again for the next turn, off this stream so it never delays one.
      if (window === undefined) yield* Effect.forkIn(readModels, sessionScope);
    });

    /**
     * Answers a permission or form T3 cannot show yet, so a session never waits
     * on it. An answer that cannot be delivered leaves OpenCode paused, so the
     * turn it would block ends as failed and the session is stopped instead.
     */
    const refuseRequest = Effect.fnUntraced(function* (
      event: EventOf<"permission.asked"> | EventOf<"form.created">,
    ) {
      const sessionId =
        event.type === "permission.asked" ? event.data.sessionID : event.data.form.sessionID;
      yield* Effect.logWarning("Answered an OpenCode request this runtime cannot show yet.", {
        type: event.type,
      });
      const delivered = yield* event.type === "permission.asked"
        ? deliver(
            client.permission.reply({
              sessionID: event.data.sessionID,
              requestID: event.data.id,
              decision: "reject",
              message: "T3 Code cannot answer this request for OpenCode 2 yet.",
            }),
          )
        : deliver(
            client.session.form.reply({
              sessionID: event.data.form.sessionID,
              formID: event.data.form.id,
              answer: Object.fromEntries(
                event.data.form.fields.map((field) => [field.key, QUESTION_REPLY]),
              ),
            }),
          );
      if (delivered) return;
      yield* Effect.logWarning("Could not answer an OpenCode request; ending its turn.", {
        type: event.type,
      });
      const state = threads.get(sessionId);
      if (state !== undefined) {
        yield* finishTurn(state, {
          status: "failed",
          failure: makeProviderFailure({
            message: "OpenCode is waiting on a request T3 Code couldn't answer.",
            class: "provider_error",
          }),
        });
      }
      yield* client.session
        .interrupt({ sessionID: Session.ID.make(sessionId) })
        .pipe(Effect.timeout("2 seconds"), Effect.ignore({ log: true }));
    });

    const handleEvent = Effect.fnUntraced(function* (event: OpenCode2StreamEvent) {
      // The end of the run a timed-out Stop left behind; no turn is its own.
      const endedSession =
        event.type === "unreadable.execution.ended"
          ? event.sessionID
          : event.type === "session.execution.succeeded" ||
              event.type === "session.execution.failed" ||
              event.type === "session.execution.interrupted"
            ? event.data.sessionID
            : undefined;
      const ended = endedSession === undefined ? undefined : threads.get(endedSession);
      if (ended?.unsettled === true) {
        ended.unsettled = false;
        return;
      }
      // Only marks where a turn's own execution begins; it never ends one.
      if (event.type === "unreadable.execution.started") {
        const turn = threads.get(event.sessionID)?.active;
        if (turn !== undefined) turn.awaitingStart = false;
        return;
      }
      if (event.type === "unreadable.execution.ended") {
        const state = threads.get(event.sessionID);
        if (state === undefined || state.active?.awaitingStart === true) return;
        return yield* finishTurn(
          state,
          event.executionType === "session.execution.succeeded"
            ? { status: state.active?.interrupted === true ? "interrupted" : "completed" }
            : event.executionType === "session.execution.interrupted"
              ? { status: "interrupted" }
              : {
                  status: "failed",
                  failure: makeProviderFailure({
                    message: "OpenCode ended the turn with an error this version cannot read.",
                    class: "provider_error",
                  }),
                },
        );
      }
      if (event.type === "permission.asked" && threads.has(event.data.sessionID)) {
        return yield* refuseRequest(event);
      }
      if (event.type === "form.created" && threads.has(event.data.form.sessionID)) {
        return yield* refuseRequest(event);
      }
      if (!("sessionID" in event.data) || typeof event.data.sessionID !== "string") return;
      const state = threads.get(event.data.sessionID);
      const turn = state?.active;
      if (state === undefined || turn === undefined) return;
      // A session runs one execution at a time, and each opens with `started`
      // on this ordered stream, so what comes before it is the stopped run's.
      if (turn.awaitingStart) {
        if (event.type === "session.execution.started") turn.awaitingStart = false;
        return;
      }
      switch (event.type) {
        case "session.inbox.enqueued":
          if (turn.promptId === undefined && event.data.item.type !== "synthetic") {
            turn.promptId = event.data.inboxID;
          }
          return;
        case "session.text.started":
        case "session.reasoning.started":
        case "session.text.delta":
        case "session.reasoning.delta":
        case "session.text.ended":
        case "session.reasoning.ended": {
          const data = event.data;
          const kind = event.type.startsWith("session.text.") ? "text" : "reasoning";
          return yield* emitText(state, turn, data, kind, (text) =>
            "delta" in data ? text + data.delta : "text" in data ? data.text : text,
          );
        }
        case "session.tool.input.started":
          turn.tools.set(event.data.id, { name: event.data.name, input: {} });
          turn.startedAt.set(event.data.id, yield* DateTime.now);
          return yield* emitTool(state, turn, event.data.id, "running");
        case "session.tool.called": {
          const tool = turn.tools.get(event.data.id);
          if (tool !== undefined) tool.input = event.data.input;
          return yield* emitTool(state, turn, event.data.id, "running");
        }
        case "session.tool.success": {
          const output = textOf(event.data.content);
          yield* emitTool(state, turn, event.data.id, "completed", {
            output,
            metadata: event.data.metadata,
          });
          turn.tools.delete(event.data.id);
          return;
        }
        case "session.tool.failed": {
          const aborted = event.data.error.type === "aborted";
          yield* emitTool(state, turn, event.data.id, aborted ? "interrupted" : "failed", {
            output: event.data.error.message,
            metadata: event.data.metadata,
          });
          turn.tools.delete(event.data.id);
          return;
        }
        // `/compact` and OpenCode's own compaction when the context fills
        // (`reason: "auto"`) both run inside a turn's execution.
        case "session.compaction.started":
          turn.compaction = {
            nativeId: `${turn.providerTurn.id}:compaction:${turn.compactions++}`,
            startedAt: yield* DateTime.now,
          };
          return yield* emitCompaction(state, turn, "running");
        case "session.compaction.ended":
          return yield* emitCompaction(state, turn, "completed", event.data.text);
        case "session.compaction.failed":
          return yield* emitCompaction(state, turn, "failed");
        case "session.step.ended":
        case "session.step.failed": {
          const tokens = event.data.tokens;
          if (tokens === undefined) return;
          turn.steps += 1;
          turn.lastStep = tokens;
          turn.usage.input += tokens.input + tokens.cache.read + tokens.cache.write;
          turn.usage.cached += tokens.cache.read;
          turn.usage.cacheWrite += tokens.cache.write;
          turn.usage.output += tokens.output + tokens.reasoning;
          turn.usage.reasoning += tokens.reasoning;
          return;
        }
        case "session.execution.succeeded":
          return yield* finishTurn(state, {
            status: turn.interrupted ? "interrupted" : "completed",
          });
        case "session.execution.interrupted":
          return yield* finishTurn(state, { status: "interrupted" });
        case "session.execution.failed":
          return yield* finishTurn(state, {
            status: "failed",
            failure: makeProviderFailure({
              message: event.data.error.message,
              code: event.data.error.type,
              class: "provider_error",
            }),
          });
        default:
          return;
      }
    });

    // The stream is the only terminal signal, and it is volatile: events sent
    // while it is down are gone, and a restarted server never ends the
    // execution it lost. So a lost stream reconnects, then reconciles each
    // running turn from the server's own state (see `reconcile`). Only when
    // reconnecting keeps failing are the turns failed and the session broken,
    // so T3 reopens it. Set first, so a turn starting meanwhile waits or refuses.
    let streamFailure: string | undefined;
    let reconnected = yield* Deferred.make<void>();
    const failAll = Effect.fnUntraced(function* (message: string) {
      streamFailure = message;
      yield* Deferred.succeed(reconnected, undefined);
      for (const state of threads.values()) {
        const failure = makeProviderFailure({ message, class: "transport_error" });
        yield* finishTurn(state, { status: "failed", failure }, "broken");
      }
      yield* setSessionStatus("error", message);
      yield* Queue.end(events);
    });

    /**
     * Emits what a running turn missed while the stream was down, from the
     * session's history since the turn's prompt: text, reasoning and tools,
     * each under the same native id its live events would have used, so
     * nothing already shown is duplicated. Returns how that history says the
     * turn's execution ended: the `idle` item OpenCode appends after each one,
     * or undefined when there is none after the prompt.
     */
    const backfill = Effect.fnUntraced(function* (sessionId: string, state: ThreadState) {
      const turn = state.active;
      if (turn === undefined) return undefined;
      const { promptId, before } = turn;
      if (promptId === undefined && before === undefined) return undefined;
      const recent = yield* paginate(
        { sessionID: Session.ID.make(sessionId), order: "desc" as const, limit: 50 },
        client.message.list,
      ).pipe(
        promptId === undefined
          ? Stream.takeWhile((message) => message.id !== before)
          : Stream.takeUntil((message) => message.id === promptId),
        Stream.runCollect,
      );
      const idle = recent.find((message) => message.type === "idle");
      for (const message of recent.toReversed()) {
        if (message.type !== "assistant" || state.active !== turn) continue;
        const ordinals = { text: 0, reasoning: 0 };
        for (const part of message.content) {
          if (part.type === "text" || part.type === "reasoning") {
            const block = { assistantMessageID: message.id, ordinal: ordinals[part.type]++ };
            yield* emitText(state, turn, block, part.type, () => part.text, true);
            continue;
          }
          if (part.type !== "tool") continue;
          if (!turn.tools.has(part.id)) {
            turn.tools.set(part.id, { name: part.name, input: {} });
            turn.startedAt.set(part.id, yield* DateTime.now);
          }
          const tool = turn.tools.get(part.id)!;
          if (typeof part.state.input === "object" && part.state.input !== null) {
            tool.input = part.state.input as Record<string, unknown>;
          }
          if (part.state.status === "completed") {
            yield* emitTool(state, turn, part.id, "completed", {
              output: textOf(part.state.content),
              metadata: part.state.metadata,
            });
            turn.tools.delete(part.id);
          } else if (part.state.status === "error") {
            yield* emitTool(state, turn, part.id, "failed", {
              output: part.state.error.message,
              metadata: part.state.metadata,
            });
            turn.tools.delete(part.id);
          } else {
            yield* emitTool(state, turn, part.id, "running");
          }
        }
      }
      return idle === undefined ? undefined : { outcome: idle.outcome };
    });

    /**
     * Settles each turn that was running when the stream dropped, from the
     * server: a session still running keeps its turn (its next events arrive on
     * the new stream); one that stopped ends the turn with the outcome of the
     * `idle` item after its prompt. A restarted server writes no such item for
     * the execution it lost, and `session.outcome` is still the previous
     * execution's, so no `idle` means the turn was interrupted.
     */
    const reconcile = Effect.gen(function* () {
      const running = [...threads].filter(([, state]) => state.active !== undefined);
      if (running.length === 0) return;
      const active = yield* client.session.active();
      for (const [sessionId, state] of running) {
        const turn = state.active;
        if (turn === undefined) continue;
        const ended = yield* backfill(sessionId, state);
        if (sessionId in active || state.active !== turn) continue;
        yield* finishTurn(
          state,
          ended?.outcome === "succeeded"
            ? { status: turn.interrupted ? "interrupted" : "completed" }
            : ended?.outcome === "failed"
              ? {
                  status: "failed",
                  failure: makeProviderFailure({
                    message:
                      "OpenCode ended the turn with an error while T3 Code was reconnecting.",
                    class: "provider_error",
                  }),
                }
              : { status: "interrupted" },
        );
      }
    });

    /**
     * Subscribes again, on a restarted server if the old one is gone, and
     * reconciles. Retried a few times; the caller fails everything after that.
     */
    const reconnect = Effect.gen(function* () {
      const scope = yield* Scope.make();
      const next = yield* borrow.pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.tapError(() => Scope.close(scope, Exit.void)),
      );
      const stream = yield* next.events.pipe(Effect.tapError(() => Scope.close(scope, Exit.void)));
      const previous = currentScope;
      connection = next;
      client = next.client;
      currentScope = scope;
      yield* Scope.close(previous, Exit.void);
      // A restarted server forgot T3's MCP servers; the next turn adds them again.
      for (const state of threads.values()) state.mcp = undefined;
      yield* reconcile.pipe(Effect.timeout(RECONCILE_TIMEOUT));
      return stream;
    }).pipe(
      Effect.retry({ times: RECONNECT_ATTEMPTS - 1, schedule: Schedule.spaced(RECONNECT_DELAY) }),
    );

    let currentScope = initial.scope;
    // The borrow in use when the session closes is returned with it, so a
    // spawned server can still reach its idle shutdown.
    yield* Effect.addFinalizer(() => Scope.close(currentScope, Exit.void));
    const follow = (stream: Stream.Stream<OpenCode2StreamEvent, unknown>): Effect.Effect<void> =>
      stream.pipe(
        Stream.runForEach(handleEvent),
        Effect.exit,
        Effect.flatMap(() =>
          Effect.gen(function* () {
            streamFailure = "The OpenCode event stream was lost. Reconnecting.";
            yield* Effect.logWarning("Lost the OpenCode event stream; reconnecting.");
            const next = yield* reconnect.pipe(Effect.option);
            if (next._tag === "None") {
              return yield* failAll("The OpenCode event stream was lost and could not reconnect.");
            }
            streamFailure = undefined;
            const done = reconnected;
            reconnected = yield* Deferred.make<void>();
            yield* Deferred.succeed(done, undefined);
            return yield* follow(next.value);
          }),
        ),
      );
    // Subscribed before any session or prompt call, so no event of theirs is missed.
    yield* follow(yield* connection.events).pipe(Effect.forkScoped);

    // A server T3 did not start keeps running after T3 stops, so stop the turns
    // it would otherwise finish unseen. A spawned server stops with its owner.
    if (connection.external) {
      yield* Effect.addFinalizer(() =>
        Effect.forEach(
          [...threads].filter(([, state]) => state.active !== undefined),
          ([sessionId]) =>
            client.session
              .interrupt({ sessionID: Session.ID.make(sessionId) })
              .pipe(Effect.timeout("1 second"), Effect.ignore({ log: true })),
          { concurrency: 8, discard: true },
        ),
      );
    }

    // Context windows come from the server's model list, read when the session
    // opens and again after a turn whose model had none yet.
    const readModels = Effect.suspend(() =>
      client.model.list({ location: { directory: session.cwd ?? serverConfig.cwd } }),
    ).pipe(
      Effect.timeout("5 seconds"),
      Effect.tap((models) =>
        Effect.sync(() => {
          for (const model of models.data) {
            // A model's input limit, when it has one, is its real headroom.
            contextWindows.set(
              `${model.providerID}/${model.id}`,
              model.limit.input ?? model.limit.context,
            );
          }
        }),
      ),
      Effect.ignore({ log: true }),
    );
    yield* readModels;

    const register = (
      providerThread: OrchestrationV2ProviderThread,
      sessionId: string,
      native: { readonly model?: ModelRef | undefined; readonly agent?: string | undefined },
    ) => {
      const existing = threads.get(sessionId);
      if (existing !== undefined) {
        existing.providerThread = providerThread;
        existing.model = native.model;
        existing.agent = native.agent;
        return providerThread;
      }
      threads.set(sessionId, {
        providerThread,
        providerTurns: new Map(),
        active: undefined,
        model: native.model,
        agent: native.agent,
        mcp: undefined,
        instructions: undefined,
        unsettled: false,
      });
      return providerThread;
    };

    const promptText = (turnInput: ProviderAdapterV2TurnInput) =>
      providerMessageTextWithAttachmentPaths({
        text: turnInput.message.text,
        attachments: turnInput.message.attachments,
        attachmentsDir: serverConfig.attachmentsDir,
      }).trim();

    /**
     * T3's MCP server for this thread, and the per-turn instructions. The MCP
     * server is registered for the session's directory under the thread's own
     * name and credential (the session rules allow only it), and removed when
     * the thread unloads or the session closes. OpenCode 2 has no per-prompt
     * system field, so the instructions are a session instructions entry,
     * which applies from the next step; it is only rewritten when it changes.
     */
    const prepareTurn = Effect.fnUntraced(function* (
      sessionId: string,
      state: ThreadState,
      turnInput: ProviderAdapterV2TurnInput,
    ) {
      const mcpSession = McpProviderSession.readMcpProviderSession(turnInput.threadId);
      const directory = turnInput.runtimePolicy.cwd ?? serverConfig.cwd;
      const name = t3McpServerName(turnInput.threadId);
      // An external server may not reach T3's MCP endpoint, as with 1.x.
      const wanted =
        mcpSession === undefined || connection.external
          ? undefined
          : { name, directory, credential: mcpSession.authorizationHeader };
      if (
        state.mcp !== undefined &&
        (wanted === undefined ||
          state.mcp.directory !== wanted.directory ||
          state.mcp.credential !== wanted.credential)
      ) {
        yield* removeMcp(state.mcp);
        state.mcp = undefined;
      }
      // T3's tools are an addition: a server that cannot add them still runs the turn.
      if (wanted !== undefined && state.mcp === undefined) {
        const added = yield* client.mcp
          .add({
            server: name,
            location: { directory },
            config: new Mcp.RemoteConfig({
              type: "remote",
              url: mcpSession!.endpoint,
              headers: { Authorization: wanted.credential },
              oauth: false,
            }),
          })
          .pipe(
            Effect.timeout(INVENTORY_TIMEOUT),
            Effect.as(true),
            Effect.catchCause((cause) =>
              Effect.logWarning("Could not add T3 Code's MCP server to OpenCode.", cause).pipe(
                Effect.as(false),
              ),
            ),
          );
        if (added) state.mcp = wanted;
      }
      const instructions = [
        buildRuntimeInstructions({ harness: "OpenCode", model: turnInput.modelSelection.model }),
        t3OrchestrationSystemPrompt(state.mcp !== undefined),
      ]
        .filter((part) => part !== undefined && part.length > 0)
        .join("\n\n");
      if (instructions !== state.instructions) {
        yield* client.session.instructions.entry.put({
          sessionID: Session.ID.make(sessionId),
          key: INSTRUCTIONS_KEY,
          value: instructions,
        });
        state.instructions = instructions;
      }
    });

    const removeMcp = (mcp: { readonly name: string; readonly directory: string }) =>
      client.mcp
        .remove({ server: mcp.name, location: { directory: mcp.directory } })
        .pipe(Effect.timeout("5 seconds"), Effect.ignore({ log: true }));

    // T3's MCP registrations outlive a session only on an external server; a
    // spawned one forgets them when it stops.
    yield* Effect.addFinalizer(() =>
      Effect.forEach(
        [...threads.values()].flatMap((state) => (state.mcp === undefined ? [] : [state.mcp])),
        removeMcp,
        { concurrency: 8, discard: true },
      ),
    );

    const markBefore = (sessionId: string, before: string | null) =>
      Effect.sync(() => {
        const turn = threads.get(sessionId)?.active;
        if (turn !== undefined) turn.before = before;
      });

    /**
     * What a turn sends: `/compact` compacts, `/name args` naming a workspace
     * command runs it, and anything else is a prompt with the `$skill`s it
     * names attached. Commands and skills are read from the session's
     * directory only when the text could use them.
     */
    const submit = Effect.fnUntraced(function* (
      sessionId: string,
      turnInput: ProviderAdapterV2TurnInput,
    ) {
      const sessionID = Session.ID.make(sessionId);
      const text = turnInput.message.text.trim();
      const bare = turnInput.message.attachments.length === 0;
      if (bare && text === "/compact") {
        return (yield* client.session.compact({ sessionID })).id;
      }
      const location = { directory: turnInput.runtimePolicy.cwd ?? serverConfig.cwd };
      const command = bare ? commandOf(text) : undefined;
      if (command !== undefined) {
        const commands = yield* client.command.list({ location }).pipe(
          Effect.timeout(INVENTORY_TIMEOUT),
          Effect.map((list) => list.data),
          Effect.orElseSucceed(() => []),
        );
        if (commands.some((entry) => entry.name === command.name)) {
          // The command's own inbox item has no id in the answer, so the turn
          // remembers where the history stood before it.
          const newest = yield* client.message.list({ sessionID, order: "desc", limit: 1 });
          yield* markBefore(sessionId, newest.data[0]?.id ?? null);
          yield* client.session.command({ sessionID, ...command });
          return undefined;
        }
      }
      const skills = SKILL_MENTION.test(text)
        ? skillsNamed(
            text,
            yield* client.skill.list({ location }).pipe(
              Effect.timeout(INVENTORY_TIMEOUT),
              Effect.map((list) => new Set(list.data.map((skill) => skill.id))),
              Effect.orElseSucceed(() => new Set<string>()),
            ),
          )
        : [];
      const accepted = yield* client.session.prompt({
        sessionID,
        text: promptText(turnInput),
        ...(skills.length === 0 ? {} : { skills: skills.map((id) => ({ id: Skill.ID.make(id) })) }),
      });
      return accepted.id;
    });

    const runtime: ProviderAdapterV2SessionRuntime = {
      instanceId,
      driver,
      providerSessionId: input.providerSessionId,
      get providerSession() {
        return session;
      },
      events: Stream.fromQueue(events),
      getModelContextWindow: (selection) =>
        selection.instanceId === instanceId ? contextWindows.get(selection.model) : undefined,
      ensureThread: (threadInput) =>
        Effect.gen(function* () {
          if (threadInput.existingProviderThread?.nativeThreadRef != null) {
            return yield* runtime.resumeThread({
              providerThread: threadInput.existingProviderThread,
              threadId: threadInput.threadId,
              modelSelection: threadInput.modelSelection,
              runtimePolicy: threadInput.runtimePolicy,
            });
          }
          const model = modelRef(threadInput.modelSelection);
          if (model === undefined) {
            return yield* new ProviderAdapterProtocolError({
              driver: OPENCODE_PROVIDER,
              detail: malformedModel(threadInput.modelSelection.model),
            });
          }
          const created = yield* client.session.create({
            location: Location.PublicRef.make({
              directory: AbsolutePath.make(threadInput.runtimePolicy.cwd ?? serverConfig.cwd),
            }),
            model,
            permissions: sessionRules(threadInput.threadId),
          });
          const createdAt = yield* DateTime.now;
          const providerThread: OrchestrationV2ProviderThread = {
            ...(threadInput.existingProviderThread ?? {
              id: idAllocator.derive.providerThread({ driver, nativeThreadId: created.id }),
              driver,
              providerInstanceId: instanceId,
              appThreadId: threadInput.threadId,
              ownerNodeId: null,
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt,
            }),
            providerSessionId: input.providerSessionId,
            nativeThreadRef: ref(created.id),
            nativeConversationHeadRef: null,
            status: "idle",
            updatedAt: createdAt,
          };
          return register(providerThread, created.id, created);
        }).pipe(
          Effect.mapError((cause) =>
            isProviderAdapterError(cause)
              ? cause
              : new ProviderAdapterEnsureThreadError({
                  driver,
                  threadId: threadInput.threadId,
                  cause,
                }),
          ),
        ),
      resumeThread: (threadInput) =>
        Effect.gen(function* () {
          const sessionId = yield* sessionIdOf(threadInput.providerThread);
          // 1.x session ids survive the upgrade; a server without this session
          // fails the resume, so T3 recreates the thread with a handoff.
          const native = yield* client.session.get({ sessionID: Session.ID.make(sessionId) });
          // A session made by 1.x or an earlier build may still allow what
          // T3 now denies, such as subagents.
          const rules = sessionRules(
            threadInput.threadId ?? threadInput.providerThread.appThreadId ?? "",
          );
          if (!sameRules(native.permissions, rules)) {
            yield* client.session.update({
              sessionID: Session.ID.make(sessionId),
              permissions: rules,
            });
          }
          // A thread moved to another worktree takes its session with it.
          const cwd = threadInput.runtimePolicy?.cwd;
          if (cwd != null && native.location.directory !== cwd) {
            yield* client.session.move({
              sessionID: Session.ID.make(sessionId),
              directory: AbsolutePath.make(cwd),
            });
          }
          return register(
            {
              ...threadInput.providerThread,
              providerSessionId: input.providerSessionId,
              status: "idle",
              updatedAt: yield* DateTime.now,
            },
            sessionId,
            native,
          );
        }).pipe(
          Effect.mapError((cause) =>
            isProviderAdapterError(cause)
              ? cause
              : new ProviderAdapterResumeThreadError({
                  driver,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
          ),
        ),
      // `/compact` is its own turn, which `startTurn` sends as `session.compact`.
      compactThread: (turnInput) =>
        runtime.startTurn({ ...turnInput, message: { ...turnInput.message, text: "/compact" } }),
      startTurn: (turnInput) =>
        Effect.gen(function* () {
          const sessionId = yield* sessionIdOf(turnInput.providerThread);
          const state = threads.get(sessionId);
          if (state === undefined) {
            return yield* new ProviderAdapterProtocolError({
              driver: OPENCODE_PROVIDER,
              detail: `OpenCode session ${sessionId} is not registered`,
            });
          }
          if (state.active !== undefined) {
            return yield* new ProviderAdapterProtocolError({
              driver: OPENCODE_PROVIDER,
              detail: `OpenCode session ${sessionId} already has an active turn`,
            });
          }
          // A lost stream is reconnecting: wait for it rather than prompt into
          // a dead stream, and refuse the turn before any request if it gave up.
          if (streamFailure !== undefined) {
            yield* Deferred.await(reconnected).pipe(Effect.timeout(RECONNECT_WAIT), Effect.ignore);
          }
          if (streamFailure !== undefined) {
            return yield* new ProviderAdapterEventStreamError({
              driver,
              providerSessionId: input.providerSessionId,
              cause: streamFailure,
            });
          }
          // After a timed-out Stop the server says whether that run is gone. A
          // run still going is stopped again and this turn fails so it can be
          // sent again; a run that is gone may still have its end on the
          // stream, which the turn skips.
          const afterUnsettled = state.unsettled;
          let stillStopping = false;
          if (state.unsettled) {
            const active = yield* client.session
              .active()
              .pipe(Effect.timeout(ACTIVE_CHECK_TIMEOUT));
            stillStopping = sessionId in active;
            if (stillStopping) {
              yield* client.session
                .interrupt({ sessionID: Session.ID.make(sessionId) })
                .pipe(Effect.timeout(INTERRUPT_TIMEOUT), Effect.ignore({ log: true }));
            } else {
              state.unsettled = false;
            }
          }
          // Installs the turn; every path after it ends the turn with a terminal.
          const begin = Effect.gen(function* () {
            const startedAt = yield* DateTime.now;
            const nativeTurnId = `${sessionId}:attempt:${turnInput.attemptId}`;
            const providerTurn: OrchestrationV2ProviderTurn = {
              id: idAllocator.derive.providerTurn({ driver, nativeTurnId }),
              providerThreadId: turnInput.providerThread.id,
              nodeId: turnInput.rootNodeId,
              runAttemptId: turnInput.attemptId,
              nativeTurnRef: ref(nativeTurnId, "weak"),
              ordinal: turnInput.providerTurnOrdinal,
              status: "running",
              startedAt,
              completedAt: null,
            };
            const turn: ActiveTurn = {
              input: turnInput,
              providerTurn,
              texts: new Map(),
              tools: new Map(),
              startedAt: new Map(),
              ordinals: new Map(),
              nextOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
              usage: { input: 0, cached: 0, cacheWrite: 0, output: 0, reasoning: 0 },
              steps: 0,
              lastStep: undefined,
              promptId: undefined,
              before: undefined,
              compaction: undefined,
              compactions: 0,
              interrupted: false,
              awaitingStart: afterUnsettled,
            };
            // No stream is left to end this turn, so it must not start.
            if (streamFailure !== undefined) {
              return yield* new ProviderAdapterEventStreamError({
                driver,
                providerSessionId: input.providerSessionId,
                cause: streamFailure,
              });
            }
            state.active = turn;
            yield* emitProviderTurn(state, turn, providerTurn);
            state.providerThread = {
              ...state.providerThread,
              status: "active",
              firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
              lastRunOrdinal: turnInput.runOrdinal,
              updatedAt: startedAt,
            };
            yield* emit({
              type: "provider_thread.updated",
              driver,
              providerThread: state.providerThread,
            });
            yield* setSessionStatus("running", null);
            return turn;
          });
          if (stillStopping) {
            yield* begin;
            return yield* finishTurn(state, {
              status: "failed",
              failure: makeProviderFailure({
                message: OPENCODE_2_STILL_STOPPING,
                class: "provider_error",
              }),
            });
          }
          // A turn T3 will not run still starts and fails, so the refusal is what
          // the user reads. Sessions allow every tool, so any other mode would
          // silently run as Full access.
          const model = modelRef(turnInput.modelSelection);
          if (turnInput.runtimePolicy.runtimeMode !== "full-access" || model === undefined) {
            yield* begin;
            return yield* finishTurn(state, {
              status: "failed",
              failure: makeProviderFailure({
                message:
                  turnInput.runtimePolicy.runtimeMode !== "full-access"
                    ? OPENCODE_2_FULL_ACCESS_ONLY
                    : malformedModel(turnInput.modelSelection.model),
                class: "validation_error",
              }),
            });
          }
          // A selection or mode changed since the last turn applies now; OpenCode
          // keeps the session's model and agent otherwise. Switching to or from
          // `plan` queues OpenCode's own "Plan mode" reminder for the prompt.
          const agent = agentFor(turnInput);
          if (agent !== (state.agent ?? "build")) {
            yield* client.session.switchAgent({
              sessionID: Session.ID.make(sessionId),
              agent: Agent.ID.make(agent),
            });
            state.agent = agent;
          }
          if (!sameModel(model, state.model)) {
            yield* client.session.switchModel({ sessionID: Session.ID.make(sessionId), model });
            state.model = model;
          }
          yield* prepareTurn(sessionId, state, turnInput);
          const turn = yield* begin;
          yield* submit(sessionId, turnInput).pipe(
            Effect.tap((promptId) =>
              Effect.sync(() => {
                if (promptId !== undefined) turn.promptId ??= promptId;
              }),
            ),
            // Deleted outside T3: the thread is broken, and forgetting it makes
            // the next turn resume, fail, and recreate it with a handoff.
            Effect.catchTags({
              SessionNotFoundError: () =>
                finishTurn(
                  state,
                  {
                    status: "failed",
                    failure: makeProviderFailure({
                      message:
                        "The OpenCode session no longer exists. Send the message again to continue in a new session.",
                      class: "provider_error",
                    }),
                  },
                  "broken",
                ).pipe(Effect.andThen(Effect.sync(() => threads.delete(sessionId)))),
            }),
            Effect.tapError((cause) =>
              state.active === turn
                ? Effect.gen(function* () {
                    // Without a clear rejection the server may have taken the
                    // prompt, so the next turn checks before it prompts again.
                    if (!CLEAR_PROMPT_REJECTIONS.has(cause._tag)) state.unsettled = true;
                    yield* finishTurn(state, {
                      status: "failed",
                      failure: makeProviderFailure({ cause, class: "provider_error" }),
                    });
                  })
                : Effect.void,
            ),
          );
        }).pipe(
          Effect.mapError((cause) =>
            isProviderAdapterError(cause)
              ? cause
              : new ProviderAdapterTurnStartError({
                  driver,
                  threadId: turnInput.threadId,
                  providerThreadId: turnInput.providerThread.id,
                  runId: turnInput.runId,
                  cause,
                }),
          ),
        ),
      steerTurn: (steerInput) =>
        Effect.fail(
          new ProviderAdapterSteerRunUnsupportedError({
            driver,
            providerThreadId: steerInput.providerThread.id,
          }),
        ),
      interruptTurn: (interruptInput) =>
        Effect.gen(function* () {
          const sessionId = yield* sessionIdOf(interruptInput.providerThread);
          const state = threads.get(sessionId);
          const turn = state?.active;
          if (
            state === undefined ||
            turn === undefined ||
            turn.providerTurn.id !== interruptInput.providerTurnId
          ) {
            return;
          }
          // The session answers with `session.execution.interrupted`, which ends
          // the turn. A server that does not answer in time is stuck, so the turn
          // ends here instead of waiting on it.
          turn.interrupted = true;
          const reply = yield* client.session
            .interrupt({ sessionID: Session.ID.make(sessionId) })
            .pipe(
              Effect.timeoutOption(INTERRUPT_TIMEOUT),
              // A Stop that never reached the server stopped nothing.
              Effect.tapError(() =>
                Effect.sync(() => {
                  turn.interrupted = false;
                }),
              ),
            );
          if (reply._tag === "None") {
            state.unsettled = true;
            return yield* finishTurn(state, { status: "interrupted" });
          }
          // Nothing was running. Unless the execution already ended (its event
          // is on the way), the turn is still open and nothing stopped.
          if (!reply.value.interrupted && state.active === turn) {
            turn.interrupted = false;
            return yield* new ProviderAdapterProtocolError({
              driver: OPENCODE_PROVIDER,
              detail: `OpenCode session ${sessionId} had nothing running to stop`,
            });
          }
        }).pipe(
          Effect.mapError((cause) =>
            isProviderAdapterError(cause)
              ? cause
              : new ProviderAdapterInterruptError({
                  driver,
                  providerThreadId: interruptInput.providerThread.id,
                  providerTurnId: interruptInput.providerTurnId,
                  cause,
                }),
          ),
        ),
      unloadThread: ({ providerThread }) =>
        Effect.gen(function* () {
          const nativeId = providerThread.nativeThreadRef?.nativeId;
          if (nativeId === undefined || nativeId === null) return;
          const state = threads.get(nativeId);
          if (state === undefined || state.active !== undefined) return;
          threads.delete(nativeId);
          if (state.mcp !== undefined) yield* removeMcp(state.mcp);
        }),
      respondToRuntimeRequest: (requestInput) =>
        Effect.fail(
          new ProviderAdapterRuntimeRequestResponseError({
            driver,
            requestId: requestInput.requestId,
            cause: notYet("answering runtime requests"),
          }),
        ),
      readThreadSnapshot: ({ providerThread }) =>
        Effect.gen(function* () {
          const sessionId = yield* sessionIdOf(providerThread);
          const history = yield* paginate(
            { sessionID: Session.ID.make(sessionId), order: "asc" as const, limit: 100 },
            client.message.list,
          ).pipe(Stream.runCollect);
          const snapshotAt = yield* DateTime.now;
          const messages = history.flatMap((message): Array<OrchestrationV2ConversationMessage> => {
            const text =
              message.type === "user"
                ? message.text
                : message.type === "assistant"
                  ? textOf(message.content)
                  : "";
            if (text.length === 0 || (message.type !== "user" && message.type !== "assistant")) {
              return [];
            }
            const createdAt = message.time.created;
            return [
              {
                createdBy: message.type === "user" ? "user" : "agent",
                creationSource: "provider",
                id: idAllocator.derive.messageFromProviderItem({
                  driver,
                  nativeItemId: message.id,
                }),
                threadId: providerThread.appThreadId ?? input.threadId,
                runId: null,
                nodeId: null,
                role: message.type,
                text,
                attachments: [],
                streaming: false,
                createdAt,
                updatedAt: createdAt,
              },
            ];
          });
          const lastUser = history.findLast((message) => message.type === "user")?.id;
          const state = threads.get(sessionId);
          return {
            providerThread: {
              ...providerThread,
              providerSessionId: input.providerSessionId,
              nativeConversationHeadRef: lastUser === undefined ? null : ref(lastUser, "weak"),
              status: "idle" as const,
              updatedAt: snapshotAt,
            },
            providerTurns: state === undefined ? [] : [...state.providerTurns.values()],
            messages,
            runtimeRequests: [],
          };
        }).pipe(
          Effect.mapError((cause) =>
            isProviderAdapterError(cause)
              ? cause
              : new ProviderAdapterReadThreadSnapshotError({
                  driver,
                  providerThreadId: providerThread.id,
                  cause,
                }),
          ),
        ),
      rollbackThread: (rollbackInput) =>
        Effect.fail(
          new ProviderAdapterRollbackThreadError({
            driver,
            providerThreadId: rollbackInput.providerThread.id,
            checkpointId: rollbackInput.target.checkpointId,
            cause: notYet("rollback"),
          }),
        ),
      forkThread: (forkInput) =>
        Effect.fail(
          new ProviderAdapterForkThreadError({
            driver,
            providerThreadId: forkInput.sourceProviderThread.id,
            cause: notYet("fork"),
          }),
        ),
    };
    return runtime;
  });

  return ProviderAdapterV2.of({
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(OpenCode2ProviderCapabilities),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    // The session borrows the instance's server for as long as it is open, so a
    // spawned server is not idle-stopped under a long tool call.
    openSession: (input) =>
      Effect.gen(function* () {
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer((exit) => Scope.close(scope, exit));
        const connection = yield* borrow.pipe(Effect.provideService(Scope.Scope, scope));
        return yield* openSession(input, { connection, scope });
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterOpenSessionError({
              driver,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      ),
  });
});
