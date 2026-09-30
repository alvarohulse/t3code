/**
 * Failures and settings a live OpenCode 2 server cannot produce on demand,
 * driven through the real adapter and `@opencode/client` against a replayed
 * HTTP server. Frames reuse the shapes recorded against 2.0.18.
 */
import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type ProviderReplayEntry,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Exit from "effect/Exit";
import { TestClock } from "effect/testing";
import { describe } from "vite-plus/test";

import type {
  ProviderAdapterV2Event,
  ProviderAdapterV2SessionRuntime,
} from "../ProviderAdapter.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import { IdAllocatorV2, layer as idAllocatorLayer } from "../IdAllocator.ts";
import { OPENCODE_PROVIDER } from "./OpenCodeAdapterV2.ts";
import { OPENCODE_2_FULL_ACCESS_ONLY, OPENCODE_2_STILL_STOPPING } from "./OpenCode2AdapterV2.ts";
import { openCode2ReplayRuntime } from "./OpenCode2AdapterV2.testkit.ts";

const SESSION = "ses_f148ca2deffeJcwCnRQtb0YFNX";
const WORK = "/work/opencode2";
const instanceId = ProviderInstanceId.make("opencode");
const threadId = ThreadId.make("thread:opencode2-adapter");

const out = (type: string, input?: unknown): ProviderReplayEntry => ({
  type: "expect_outbound",
  frame: input === undefined ? { type } : { type, input },
});
/** A recorded response body; `{ data }` is the server's envelope, `null` an empty 204. */
const reply = (operation: string, data: unknown): ProviderReplayEntry => ({
  type: "emit_inbound",
  frame: { type: "sdk.response", operation, data },
});
const replyData = (operation: string, data: unknown) => reply(operation, { data });
const event = (type: string, data: Record<string, unknown>): ProviderReplayEntry => ({
  type: "emit_inbound",
  frame: {
    type: "sdk.event",
    event: { id: `evt_${type.replaceAll(".", "")}0000`, created: 1, type, data, ...durable },
  },
});
const durable = { durable: { aggregateID: SESSION, seq: 1, version: 1 } };

/** The rules T3 gives every session it runs, with only this thread's own T3 MCP server allowed. */
const t3Rules = [
  { action: "*", resource: "*", effect: "allow" },
  { action: "subagent", resource: "*", effect: "deny" },
  { action: "t3-code-*", resource: "*", effect: "deny" },
  { action: "t3-code-thread_opencode2-adapter_*", resource: "*", effect: "allow" },
];
const sessionInfo = (overrides: Record<string, unknown> = {}) => ({
  id: SESSION,
  permissions: t3Rules,
  projectID: "global",
  model: { id: "big-pickle", providerID: "opencode", variant: "default" },
  cost: 0,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  time: { created: 1790656601394, updated: 1790656601394 },
  location: { directory: WORK },
  ...overrides,
});
// `/api/model` as 2.0.18 lists big-pickle: its 160k input limit is the usable window.
const modelCatalog = {
  location: { directory: WORK },
  data: [
    {
      id: "big-pickle",
      modelID: "big-pickle",
      providerID: "opencode",
      family: "big-pickle",
      name: "Big Pickle",
      compatibility: { reasoningField: "reasoning_content" },
      package: "@opencode/ai/providers/openai-compatible",
      settings: { apiKey: "public", baseURL: "https://opencode.ai/zen/v1", provider: "opencode" },
      capabilities: { tools: true, input: ["text"], output: ["text"] },
      variants: [],
      time: { released: 1760659200000 },
      cost: [{ input: 0, output: 0, cache: { read: 0, write: 0 } }],
      status: "active",
      enabled: true,
      limit: { context: 200000, input: 160000, output: 32000 },
    },
  ],
};

const promptAccepted = replyData("session.prompt", {
  id: "msg_0eb735d41001NJee1EvVePJAK5",
  sessionID: SESSION,
  time: { created: 1790656601410 },
  type: "user",
  payload: { text: "hi" },
  delivery: "steer",
});

/**
 * A thread's first turn writes T3's instructions entry before it starts; the
 * adapter only rewrites it when it changes, so later turns do not.
 */
const withInstructions = (
  entries: ReadonlyArray<ProviderReplayEntry>,
): ReadonlyArray<ProviderReplayEntry> => {
  const first = entries.findIndex(
    (entry) =>
      entry.type === "expect_outbound" &&
      typeof entry.frame === "object" &&
      entry.frame !== null &&
      "type" in entry.frame &&
      ["session.prompt", "session.command", "session.compact"].includes(String(entry.frame.type)),
  );
  // T3's MCP server is added before the entry that describes it.
  const after = entries.findIndex(
    (entry, index) =>
      index < first &&
      entry.type === "emit_inbound" &&
      typeof entry.frame === "object" &&
      entry.frame !== null &&
      "operation" in entry.frame &&
      entry.frame.operation === "mcp.add",
  );
  const at = after < 0 ? first : after + 1;
  return first < 0
    ? entries
    : [
        ...entries.slice(0, at),
        out("session.instructions.entry.put", {
          sessionID: SESSION,
          key: "t3-code",
          value: "<any>",
        }),
        reply("session.instructions.entry.put", null),
        ...entries.slice(at),
      ];
};

const openCode2ReplayRuntimeWithInstructions = (
  entries: ReadonlyArray<ProviderReplayEntry>,
  options?: { readonly external?: boolean },
) => openCode2ReplayRuntime(withInstructions(entries), options);

/** What every session sends when it opens: the event stream, then the model list. */
const opening: ReadonlyArray<ProviderReplayEntry> = [
  out("event.subscribe"),
  out("model.list", "<any>"),
  reply("model.list", modelCatalog),
];

const bigPickle: ModelSelection = { instanceId, model: "opencode/big-pickle" };
const policy = (runtimeMode: "full-access" | "approval-required" = "full-access") => ({
  runtimeMode,
  interactionMode: "default" as const,
  cwd: WORK,
});

const providerThread = (now: DateTime.Utc): OrchestrationV2ProviderThread => ({
  id: ProviderThreadId.make("provider-thread:opencode2-adapter"),
  driver: OPENCODE_PROVIDER,
  providerInstanceId: instanceId,
  providerSessionId: ProviderSessionId.make("provider-session:opencode2-adapter"),
  appThreadId: threadId,
  ownerNodeId: null,
  nativeThreadRef: { driver: OPENCODE_PROVIDER, nativeId: SESSION, strength: "strong" },
  nativeConversationHeadRef: null,
  status: "idle",
  firstRunOrdinal: null,
  lastRunOrdinal: null,
  handoffIds: [],
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
});

const turnInput = (
  thread: OrchestrationV2ProviderThread,
  modelSelection: ModelSelection = bigPickle,
  runtimeMode: "full-access" | "approval-required" = "full-access",
) => ({
  appThread: {} as OrchestrationV2AppThread,
  threadId,
  runId: RunId.make("run:opencode2-adapter"),
  runOrdinal: 1,
  providerTurnOrdinal: 1,
  attemptId: RunAttemptId.make("attempt:opencode2-adapter"),
  rootNodeId: NodeId.make("node:opencode2-adapter"),
  providerThread: thread,
  message: {
    messageId: MessageId.make("message:opencode2-adapter"),
    text: "hi",
    attachments: [],
    createdBy: "user" as const,
    creationSource: "web" as const,
    scheduledTaskId: undefined,
    senderThreadId: undefined,
  },
  modelSelection,
  runtimePolicy: policy(runtimeMode),
});

/** Resumes the recorded session and returns the runtime, the thread, and its event stream. */
const resumed = (entries: ReadonlyArray<ProviderReplayEntry>, options?: { external?: boolean }) =>
  Effect.gen(function* () {
    const runtime = yield* openCode2ReplayRuntime(
      withInstructions([
        ...opening,
        out("session.get", { sessionID: SESSION }),
        replyData("session.get", sessionInfo()),
        ...entries,
      ]),
      options,
    );
    const thread = yield* runtime.resumeThread({
      providerThread: providerThread(yield* DateTime.now),
      threadId,
      modelSelection: bigPickle,
      runtimePolicy: policy(),
    });
    return { runtime, thread };
  });

const terminalOf = (runtime: ProviderAdapterV2SessionRuntime) =>
  runtime.events.pipe(
    Stream.filter(
      (event): event is Extract<ProviderAdapterV2Event, { type: "turn.terminal" }> =>
        event.type === "turn.terminal",
    ),
    Stream.runHead,
    Effect.map(Option.getOrUndefined),
  );

// The history the spike read back after its `simple` turn (recordings/simple.ndjson).
const history = {
  data: [
    {
      id: "msg_0eb732081001RntUJfRtTXOAjd",
      time: { created: 1790656585885 },
      text: "Think carefully step by step about whether 391 is prime, showing your reasoning, then answer in one short sentence.",
      type: "user",
    },
    {
      id: "msg_0eb7320a9001vve3OV5uNi2HRT",
      time: { created: 1790656585925, streamed: 1790656590719, completed: 1790656590736 },
      type: "assistant",
      agent: "build",
      model: { id: "space-bunny-free", providerID: "opencode", variant: "high" },
      content: [
        { type: "reasoning", text: "Check divisibility up to sqrt(391)." },
        { type: "text", text: "391 is not prime: it's the product 17 × 23." },
      ],
      finish: "stop",
      cost: 0,
      tokens: { input: 8701, output: 113, reasoning: 147, cache: { read: 489, write: 0 } },
    },
    {
      id: "msg_0eb733399001NhwTrB32UU6d6H",
      time: { created: 1790656590745 },
      type: "idle",
      outcome: "succeeded",
    },
  ],
  cursor: {},
};

describe("OpenCode2 adapter", () => {
  it.effect("switches the session's model and variant before a turn that changed them", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.switchModel", {
          sessionID: SESSION,
          model: { providerID: "openrouter", id: "deepseek/deepseek-v4-flash", variant: "high" },
        }),
        reply("session.switchModel", null),
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(
        turnInput(thread, {
          instanceId,
          model: "openrouter/deepseek/deepseek-v4-flash",
          options: [{ id: "variant", value: "high" }],
        }),
      );
      assert.equal((yield* Fiber.join(terminal))?.status, "completed");
    }).pipe(Effect.scoped),
  );

  it.effect(
    "refuses a turn outside Full access instead of running it with every tool allowed",
    () =>
      Effect.gen(function* () {
        // No prompt is expected: the turn fails without reaching the server.
        const { runtime, thread } = yield* resumed([]);
        const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
        yield* runtime.startTurn(turnInput(thread, bigPickle, "approval-required"));
        const refused = yield* Fiber.join(terminal);
        assert.deepInclude(refused?.failure, {
          class: "validation_error",
          message: OPENCODE_2_FULL_ACCESS_ONLY,
        });
      }).pipe(Effect.scoped),
  );

  it.effect("ends the turn when its terminal event is one this build cannot decode", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        // A reason added after 2.0.18: the full schema rejects the frame.
        event("session.execution.interrupted", { sessionID: SESSION, reason: "budget" }),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      assert.equal((yield* Fiber.join(terminal))?.status, "interrupted");
    }).pipe(Effect.scoped),
  );

  it.effect("keeps a turn running through a start event this build cannot decode", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        {
          type: "emit_inbound",
          frame: {
            type: "sdk.event",
            event: {
              id: "evt_executionstartedx",
              created: 1,
              type: "session.execution.started",
              data: { sessionID: SESSION },
              durable: "not-an-envelope",
            },
          },
        },
        event("session.text.started", {
          sessionID: SESSION,
          assistantMessageID: "msg_0eb735d5b001oAFVeY5jz3WD4Z",
          ordinal: 0,
        }),
        event("session.text.ended", {
          sessionID: SESSION,
          assistantMessageID: "msg_0eb735d5b001oAFVeY5jz3WD4Z",
          ordinal: 0,
          text: "DONE",
        }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const collected = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(turnInput(thread));
      const seen = yield* Fiber.join(collected);
      const terminals = seen.filter((event) => event.type === "turn.terminal");
      assert.deepEqual(
        terminals.map((event) => event.type === "turn.terminal" && event.status),
        ["completed"],
      );
      // The reply after the malformed start still reached the turn.
      assert.isTrue(
        seen.some(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "assistant_message" &&
            event.turnItem.text === "DONE",
        ),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("denies the subagent tool on the sessions it creates", () =>
    Effect.gen(function* () {
      const runtime = yield* openCode2ReplayRuntimeWithInstructions([
        ...opening,
        out("session.create", {
          location: { directory: WORK },
          model: { providerID: "opencode", id: "big-pickle" },
          permissions: t3Rules,
        }),
        replyData("session.create", sessionInfo()),
      ]);
      const thread = yield* runtime.ensureThread({
        threadId,
        modelSelection: bigPickle,
        runtimePolicy: policy(),
      });
      assert.equal(thread.nativeThreadRef?.nativeId, SESSION);
    }).pipe(Effect.scoped),
  );

  it.effect("stops running turns on an external server when the session closes", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const { runtime, thread } = yield* resumed(
        [
          out("session.prompt", { sessionID: SESSION, text: "<any>" }),
          promptAccepted,
          out("session.interrupt", { sessionID: SESSION }),
          reply("session.interrupt", { interrupted: true }),
        ],
        { external: true },
      ).pipe(Scope.provide(scope));
      yield* runtime.startTurn(turnInput(thread));
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("ends a turn locally when a stuck server never answers Stop", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", "<hang>"),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      const interrupt = yield* runtime
        .interruptTurn({
          providerThread: thread,
          providerTurnId: yield* providerTurnId,
        })
        .pipe(Effect.forkScoped);
      yield* TestClock.adjust("11 seconds");
      yield* Fiber.join(interrupt);
      assert.equal((yield* Fiber.join(terminal))?.status, "interrupted");
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  /** A prompt accepted, then a Stop the server never answers, advanced past its timeout. */
  const stopTimedOut: ReadonlyArray<ProviderReplayEntry> = [
    out("session.prompt", { sessionID: SESSION, text: "<any>" }),
    promptAccepted,
    out("session.interrupt", { sessionID: SESSION }),
    reply("session.interrupt", "<hang>"),
  ];
  const secondTurn = (thread: OrchestrationV2ProviderThread) => ({
    ...turnInput(thread),
    runId: RunId.make("run:opencode2-adapter:2"),
    runOrdinal: 2,
    providerTurnOrdinal: 2,
    attemptId: RunAttemptId.make("attempt:opencode2-adapter:2"),
  });
  const stopFirstTurn = (
    runtime: ProviderAdapterV2SessionRuntime,
    thread: OrchestrationV2ProviderThread,
  ) =>
    Effect.gen(function* () {
      yield* runtime.startTurn(turnInput(thread));
      const interrupt = yield* runtime
        .interruptTurn({ providerThread: thread, providerTurnId: yield* providerTurnId })
        .pipe(Effect.forkScoped);
      yield* TestClock.adjust("11 seconds");
      yield* Fiber.join(interrupt);
    });
  const terminals = (runtime: ProviderAdapterV2SessionRuntime, count: number) =>
    runtime.events.pipe(
      Stream.filter(
        (event): event is Extract<ProviderAdapterV2Event, { type: "turn.terminal" }> =>
          event.type === "turn.terminal",
      ),
      Stream.take(count),
      Stream.runCollect,
      Effect.forkScoped,
    );

  it.effect("never lets a timed-out Stop's late end finish the next turn", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        ...stopTimedOut,
        // The server no longer runs the stopped execution, so the next turn goes ahead.
        out("session.active"),
        reply("session.active", { data: {} }),
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        // The stopped execution's end arrives late, then the new turn's own.
        event("session.execution.succeeded", { sessionID: SESSION }),
        event("session.execution.started", { sessionID: SESSION }),
        event("session.execution.failed", {
          sessionID: SESSION,
          error: { type: "provider", message: "second turn failed" },
        }),
      ]);
      const ended = yield* terminals(runtime, 2);
      yield* stopFirstTurn(runtime, thread);
      yield* runtime.startTurn(secondTurn(thread));
      const [first, second] = yield* Fiber.join(ended);
      assert.equal(first?.status, "interrupted");
      // Only the second turn's own end finishes it.
      assert.equal(second?.status, "failed");
      assert.equal(second?.failure?.message, "second turn failed");
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect(
    "starts the next turn once the server no longer runs a timed-out Stop's execution",
    () =>
      Effect.gen(function* () {
        const { runtime, thread } = yield* resumed([
          ...stopTimedOut,
          out("session.active"),
          reply("session.active", { data: {} }),
          out("session.prompt", { sessionID: SESSION, text: "<any>" }),
          promptAccepted,
          event("session.execution.started", { sessionID: SESSION }),
          event("session.execution.succeeded", { sessionID: SESSION }),
        ]);
        const ended = yield* terminals(runtime, 2);
        yield* stopFirstTurn(runtime, thread);
        yield* runtime.startTurn(secondTurn(thread));
        const [, second] = yield* Fiber.join(ended);
        assert.equal(second?.status, "completed");
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("reads a timed-out Stop's next turn from an execution start it cannot decode", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        ...stopTimedOut,
        out("session.active"),
        reply("session.active", { data: {} }),
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        // A newer server's start: the full schema rejects it, but it still opens the turn.
        {
          type: "emit_inbound",
          frame: {
            type: "sdk.event",
            event: {
              id: "evt_executionstartednewer",
              created: 1,
              type: "session.execution.started",
              data: { sessionID: SESSION },
              durable: "not-an-envelope",
            },
          },
        },
        event("session.text.ended", {
          sessionID: SESSION,
          assistantMessageID: "msg_0eb735d5b001oAFVeY5jz3WD4Z",
          ordinal: 0,
          text: "DONE",
        }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const ended = yield* terminals(runtime, 2);
      yield* stopFirstTurn(runtime, thread);
      yield* runtime.startTurn(secondTurn(thread));
      const [, second] = yield* Fiber.join(ended);
      assert.equal(second?.status, "completed");
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("checks the server before prompting again after a prompt request failed", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        // The request failed, but the server may have taken the prompt.
        reply("session.prompt", {
          status: 502,
          body: { _tag: "UnknownError", message: "bad gateway" },
        }),
        out("session.active"),
        reply("session.active", { data: { [SESSION]: { type: "running" } } }),
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", { interrupted: true }),
      ]);
      const ended = yield* terminals(runtime, 2);
      yield* runtime.startTurn(turnInput(thread)).pipe(Effect.ignore);
      yield* runtime.startTurn(secondTurn(thread));
      const [first, second] = yield* Fiber.join(ended);
      assert.equal(first?.status, "failed");
      assert.equal(second?.failure?.message, OPENCODE_2_STILL_STOPPING);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("prompts again without a check after the server refused a prompt", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        reply("session.prompt", {
          status: 400,
          body: { _tag: "InvalidRequestError", message: "bad prompt" },
        }),
        // A clear refusal: nothing runs, so the next turn prompts directly.
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const ended = yield* terminals(runtime, 2);
      yield* runtime.startTurn(turnInput(thread)).pipe(Effect.ignore);
      yield* runtime.startTurn(secondTurn(thread));
      const [first, second] = yield* Fiber.join(ended);
      assert.deepEqual([first?.status, second?.status], ["failed", "completed"]);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("stops a timed-out Stop's execution again and fails the turn while it runs", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        ...stopTimedOut,
        out("session.active"),
        reply("session.active", { data: { [SESSION]: { type: "running" } } }),
        // Stopped again, and the turn fails without a prompt.
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", { interrupted: true }),
      ]);
      const ended = yield* terminals(runtime, 2);
      yield* stopFirstTurn(runtime, thread);
      yield* runtime.startTurn(secondTurn(thread));
      const [, second] = yield* Fiber.join(ended);
      assert.equal(second?.status, "failed");
      assert.equal(second?.failure?.message, OPENCODE_2_STILL_STOPPING);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("does not report a Stop the server says did nothing", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", { interrupted: false }),
      ]);
      yield* runtime.startTurn(turnInput(thread));
      const failed = yield* runtime
        .interruptTurn({
          providerThread: thread,
          providerTurnId: yield* providerTurnId,
        })
        .pipe(Effect.flip);
      assert.equal(failed._tag, "ProviderAdapterProtocolError");
    }).pipe(Effect.scoped),
  );

  it.effect("reports a turn as completed when its Stop request failed", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", {
          status: 500,
          body: { _tag: "UnknownError", message: "interrupt failed" },
        }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      const failed = yield* runtime
        .interruptTurn({ providerThread: thread, providerTurnId: yield* providerTurnId })
        .pipe(Effect.flip);
      assert.equal(failed._tag, "ProviderAdapterInterruptError");
      assert.equal((yield* Fiber.join(terminal))?.status, "completed");
    }).pipe(Effect.scoped),
  );

  it.effect("refuses to resume a thread without an OpenCode session as a protocol error", () =>
    Effect.gen(function* () {
      const runtime = yield* openCode2ReplayRuntime(opening);
      const failed = yield* runtime
        .resumeThread({
          providerThread: { ...providerThread(yield* DateTime.now), nativeThreadRef: null },
        })
        .pipe(Effect.flip);
      assert.equal(failed._tag, "ProviderAdapterProtocolError");
    }).pipe(Effect.scoped),
  );

  it.effect("gives a resumed session T3's rules when it was made with others", () =>
    Effect.gen(function* () {
      const runtime = yield* openCode2ReplayRuntimeWithInstructions([
        ...opening,
        out("session.get", { sessionID: SESSION }),
        // Made before the subagent rule: it still allows everything.
        replyData(
          "session.get",
          sessionInfo({ permissions: [{ action: "*", resource: "*", effect: "allow" }] }),
        ),
        out("session.update", { sessionID: SESSION, permissions: t3Rules }),
        reply("session.update", null),
      ]);
      yield* runtime.resumeThread({
        providerThread: providerThread(yield* DateTime.now),
        threadId,
        modelSelection: bigPickle,
        runtimePolicy: policy(),
      });
    }).pipe(Effect.scoped),
  );

  it.effect("moves the session when the thread's worktree changed", () =>
    Effect.gen(function* () {
      const runtime = yield* openCode2ReplayRuntimeWithInstructions([
        ...opening,
        out("session.get", { sessionID: SESSION }),
        replyData("session.get", sessionInfo()),
        out("session.move", { sessionID: SESSION, directory: "/work/opencode2-feature" }),
        reply("session.move", null),
      ]);
      yield* runtime.resumeThread({
        providerThread: providerThread(yield* DateTime.now),
        threadId,
        modelSelection: bigPickle,
        runtimePolicy: { ...policy(), cwd: "/work/opencode2-feature" },
      });
    }).pipe(Effect.scoped),
  );

  it.effect(
    "moves the session when a thread it resumes through ensureThread changed worktree",
    () =>
      Effect.gen(function* () {
        const runtime = yield* openCode2ReplayRuntimeWithInstructions([
          ...opening,
          out("session.get", { sessionID: SESSION }),
          replyData("session.get", sessionInfo()),
          out("session.move", { sessionID: SESSION, directory: "/work/opencode2-feature" }),
          reply("session.move", null),
        ]);
        yield* runtime.ensureThread({
          threadId,
          modelSelection: bigPickle,
          runtimePolicy: { ...policy(), cwd: "/work/opencode2-feature" },
          existingProviderThread: providerThread(yield* DateTime.now),
        });
      }).pipe(Effect.scoped),
  );

  it.effect("breaks the thread and forgets it when the session was deleted outside T3", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        reply("session.prompt", {
          status: 404,
          body: {
            _tag: "SessionNotFoundError",
            sessionID: SESSION,
            message: `Session not found: ${SESSION}`,
          },
        }),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread)).pipe(Effect.ignore);
      const ended = yield* Fiber.join(terminal);
      assert.equal(ended?.status, "failed");
      assert.equal(ended?.threadDisposition, "broken");
      // The next turn must resume (and fail into a handoff), not reuse the dead session.
      const again = yield* runtime.startTurn(turnInput(thread)).pipe(Effect.flip);
      assert.equal(again._tag, "ProviderAdapterProtocolError");
      assert.include(again.message, "not registered");
    }).pipe(Effect.scoped),
  );

  it.effect("refuses a model slug that is not provider/model before creating a session", () =>
    Effect.gen(function* () {
      // Nothing but the session's opening is expected: no create, no prompt.
      const runtime = yield* openCode2ReplayRuntime([...opening]);
      const created = yield* runtime
        .ensureThread({
          threadId,
          modelSelection: { instanceId, model: "big-pickle" },
          runtimePolicy: policy(),
        })
        .pipe(Effect.flip);
      assert.equal(created._tag, "ProviderAdapterProtocolError");
      assert.include(created.message, "OpenCode model 'big-pickle' must use provider/model format");
    }).pipe(Effect.scoped),
  );

  it.effect("refuses a turn whose model slug is not provider/model before prompting", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread, { instanceId, model: "big-pickle" }));
      const refused = yield* Fiber.join(terminal);
      assert.equal(refused?.failure?.class, "validation_error");
      assert.include(refused?.failure?.message, "must use provider/model format");
    }).pipe(Effect.scoped),
  );

  it.effect("ends the turn when a permission it refuses cannot be answered", () =>
    Effect.gen(function* () {
      const failedReply = reply("permission.reply", {
        status: 500,
        body: { _tag: "UnknownError", message: "reply failed" },
      });
      const replyOut = out("permission.reply", {
        sessionID: SESSION,
        requestID: "per_0eb7c4d7e001Pyt8o50Vi4KrOO",
        decision: "reject",
        message: "<any>",
      });
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        {
          type: "emit_inbound",
          frame: {
            type: "sdk.event",
            event: {
              id: "evt_permissionasked0",
              created: 1,
              type: "permission.asked",
              data: {
                id: "per_0eb7c4d7e001Pyt8o50Vi4KrOO",
                sessionID: SESSION,
                action: "shell",
                resources: ["echo FIRST"],
              },
            },
          },
        },
        // One try and one retry, then the turn ends and the session is stopped.
        replyOut,
        failedReply,
        replyOut,
        failedReply,
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", { interrupted: true }),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      const ended = yield* Fiber.join(terminal);
      assert.equal(ended?.status, "failed");
      assert.equal(
        ended?.status === "failed" ? ended.failure.message : undefined,
        "OpenCode is waiting on a request T3 Code couldn't answer.",
      );
      // Let the best-effort interrupt reach the server before the scope closes.
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
    }).pipe(Effect.scoped),
  );

  it.effect("answers a question form instead of cancelling it", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        {
          type: "emit_inbound",
          frame: {
            type: "sdk.event",
            event: {
              id: "evt_formcreated0000",
              created: 1,
              type: "form.created",
              data: {
                form: {
                  id: "frm_0eb79ab35001fkvFECSh3wYNVD",
                  sessionID: SESSION,
                  title: "Questions",
                  metadata: { kind: "question" },
                  fields: [
                    {
                      key: "q0",
                      title: "Color preference",
                      type: "string",
                      options: [{ value: "Red", label: "Red" }],
                      custom: true,
                    },
                  ],
                },
              },
            },
          },
        },
        out("session.form.reply", {
          sessionID: SESSION,
          formID: "frm_0eb79ab35001fkvFECSh3wYNVD",
          answer: { q0: "Questions aren't supported by this OpenCode integration yet." },
        }),
        reply("session.form.reply", null),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      assert.equal((yield* Fiber.join(terminal))?.status, "completed");
    }).pipe(Effect.scoped),
  );

  it.effect("refuses a turn once reconnecting to a lost event stream has given up", () =>
    Effect.gen(function* () {
      // The server goes away with no turn running, and never comes back.
      const { runtime, thread } = yield* resumed([{ type: "runtime_exit", status: "success" }]);
      // Reconnecting retries a few times, seconds apart, then ends the runtime's events.
      const drained = yield* runtime.events.pipe(Stream.runDrain, Effect.forkScoped);
      yield* TestClock.adjust("1 minute");
      yield* Fiber.join(drained);
      const refused = yield* runtime.startTurn(turnInput(thread)).pipe(Effect.flip);
      assert.equal(refused._tag, "ProviderAdapterEventStreamError");
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("fails the session once reconnecting to a lost event stream has given up", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        { type: "runtime_exit", status: "success" },
      ]);
      // The runtime's event stream has one consumer.
      const events = yield* runtime.events.pipe(Stream.runCollect, Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      yield* TestClock.adjust("1 minute");
      const collected = yield* Fiber.join(events);
      const terminal = collected.find((event) => event.type === "turn.terminal");
      assert.deepInclude(terminal, { status: "failed", threadDisposition: "broken" });
      const last = collected.findLast((event) => event.type === "provider_session.updated");
      assert.equal(
        last?.type === "provider_session.updated" ? last.providerSession.status : undefined,
        "error",
      );
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );
  // A stream that drops and comes back: what the gap lost is read from the
  // session's history, and the turn ends from the server's own state. Real
  // servers only do this on a network blip, so these are driven here.
  const reconnected = (active: Record<string, unknown>, ended?: "succeeded") => [
    { type: "runtime_exit", status: "success" } as const,
    out("event.subscribe"),
    out("session.active"),
    replyData("session.active", active),
    out("message.list", { sessionID: SESSION, order: "desc", limit: "50" }),
    reply("message.list", {
      data: [
        // OpenCode appends an `idle` item after each execution it ends.
        ...(ended === undefined
          ? []
          : [{ id: "msg_idle_gap", time: { created: 3 }, type: "idle", outcome: ended }]),
        {
          id: "msg_assistant_gap",
          time: { created: 2 },
          type: "assistant",
          agent: "build",
          model: { id: "big-pickle", providerID: "opencode", variant: "default" },
          content: [
            { type: "text", text: "Sent while the stream was down." },
            {
              type: "tool",
              id: "call_gap",
              name: "shell",
              executed: true,
              state: {
                status: "completed",
                input: { command: "echo GAP" },
                content: [{ type: "text", text: "GAP" }],
                metadata: {},
              },
              time: { created: 2, ran: 2, completed: 2 },
            },
          ],
          finish: "stop",
        },
        {
          id: "msg_0eb735d41001NJee1EvVePJAK5",
          time: { created: 1 },
          text: "hi",
          type: "user",
        },
      ],
      cursor: {},
    }),
  ];
  const turnItems = (collected: ReadonlyArray<ProviderAdapterV2Event>) =>
    collected.flatMap((event) =>
      event.type === "turn_item.updated" ? [`${event.turnItem.type}:${event.turnItem.status}`] : [],
    );

  it.effect("ends a turn that finished while the stream was down with the server's outcome", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        ...reconnected({}, "succeeded"),
      ]);
      const events = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(turnInput(thread));
      const collected = yield* Fiber.join(events);
      // The reply and the tool call it ran are shown before the turn ends.
      assert.includeMembers(turnItems(collected), [
        "assistant_message:completed",
        "command_execution:completed",
      ]);
      assert.deepInclude(collected.at(-1), { type: "turn.terminal", status: "completed" });
    }).pipe(Effect.scoped),
  );

  it.effect("keeps a turn still running after a reconnect open for its next events", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        ...reconnected({ [SESSION]: { type: "running" } }),
        event("session.text.ended", {
          sessionID: SESSION,
          assistantMessageID: "msg_assistant_after",
          ordinal: 0,
          text: "Arrived on the new stream.",
        }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const events = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(turnInput(thread));
      const collected = yield* Fiber.join(events);
      const texts = collected.flatMap((event) =>
        event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
          ? [event.turnItem.text]
          : [],
      );
      assert.includeMembers(texts, [
        "Sent while the stream was down.",
        "Arrived on the new stream.",
      ]);
      assert.deepInclude(collected.at(-1), { type: "turn.terminal", status: "completed" });
    }).pipe(Effect.scoped),
  );

  it.effect(
    "registers T3's MCP server for the thread alone and removes it when the thread unloads",
    () =>
      Effect.gen(function* () {
        McpProviderSession.setMcpProviderSession({
          environmentId: EnvironmentId.make("environment:opencode2-adapter"),
          threadId,
          providerSessionId: "mcp:opencode2-adapter",
          providerInstanceId: instanceId,
          endpoint: "http://127.0.0.1:3773/mcp",
          authorizationHeader: "Bearer thread-credential",
          browserToolsAvailable: false,
        });
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => McpProviderSession.clearMcpProviderSession(threadId)),
        );
        const server = "t3-code-thread_opencode2-adapter";
        const { runtime, thread } = yield* resumed([
          // Registered for the session's directory under the thread's own name;
          // the session's rules allow only this name's tools (see `t3Rules`).
          out("mcp.add", {
            server,
            "location[directory]": WORK,
            config: {
              type: "remote",
              url: "http://127.0.0.1:3773/mcp",
              headers: { Authorization: "Bearer thread-credential" },
              oauth: false,
            },
          }),
          reply("mcp.add", null),
          out("session.prompt", { sessionID: SESSION, text: "<any>" }),
          promptAccepted,
          event("session.execution.succeeded", { sessionID: SESSION }),
          out("mcp.remove", { server, "location[directory]": WORK }),
          reply("mcp.remove", null),
        ]);
        const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
        yield* runtime.startTurn(turnInput(thread));
        assert.equal((yield* Fiber.join(terminal))?.status, "completed");
        yield* runtime.unloadThread!({ providerThread: thread });
      }).pipe(Effect.scoped),
  );

  it.effect("reads user and assistant text from the session's message list", () =>
    Effect.gen(function* () {
      const runtime = yield* openCode2ReplayRuntimeWithInstructions([
        ...opening,
        out("message.list", { sessionID: SESSION, order: "asc", limit: "100" }),
        reply("message.list", history),
      ]);
      const snapshot = yield* runtime.readThreadSnapshot({
        providerThread: providerThread(yield* DateTime.now),
      });
      assert.deepEqual(
        snapshot.messages.map((message) => [message.role, message.text]),
        [
          ["user", history.data[0]!.text],
          ["assistant", "391 is not prime: it's the product 17 × 23."],
        ],
      );
      assert.equal(
        snapshot.providerThread.nativeConversationHeadRef?.nativeId,
        history.data[0]!.id,
      );
    }).pipe(Effect.scoped),
  );

  it.effect("ends a turn before re-reading a model list that never answers", () =>
    Effect.gen(function* () {
      // The session opened before the catalog loaded, so the turn's model has no window.
      const runtime = yield* openCode2ReplayRuntimeWithInstructions([
        out("event.subscribe"),
        out("model.list", "<any>"),
        reply("model.list", { location: { directory: WORK }, data: [] }),
        out("session.get", { sessionID: SESSION }),
        replyData("session.get", sessionInfo()),
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        event("session.execution.succeeded", { sessionID: SESSION }),
        out("model.list", "<any>"),
        reply("model.list", "<hang>"),
      ]);
      const thread = yield* runtime.resumeThread({
        providerThread: providerThread(yield* DateTime.now),
        threadId,
        modelSelection: bigPickle,
        runtimePolicy: policy(),
      });
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      assert.equal((yield* Fiber.join(terminal))?.status, "completed");
      // The terminal did not wait on the re-read, which is still in flight.
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
    }).pipe(Effect.scoped),
  );

  it.effect("reports cache writes as cache creation, not only as input", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        // A step from a provider that reports prompt-cache writes.
        event("session.step.ended", {
          sessionID: SESSION,
          assistantMessageID: "msg_0eb735d5b001oAFVeY5jz3WD4Z",
          finish: "stop",
          cost: 0,
          tokens: { input: 1200, output: 40, reasoning: 0, cache: { read: 300, write: 2500 } },
        }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const turn = yield* runtime.events.pipe(
        Stream.filter(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.completedAt !== null,
        ),
        Stream.runHead,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(turnInput(thread));
      const settled = Option.getOrUndefined(yield* Fiber.join(turn));
      assert.deepInclude(
        settled?.type === "provider_turn.updated" ? settled.providerTurn.turnTokenUsage : undefined,
        {
          inputTokens: 1200 + 300 + 2500,
          cachedInputTokens: 300,
          cacheCreationTokens: 2500,
          outputTokens: 40,
        },
      );
    }).pipe(Effect.scoped),
  );

  it.effect("reports a model's input limit as its context window", () =>
    Effect.gen(function* () {
      const runtime = yield* openCode2ReplayRuntime([...opening]);
      assert.equal(runtime.getModelContextWindow?.(bigPickle), 160000);
    }).pipe(Effect.scoped),
  );
});

/** The provider turn the adapter derives for `turnInput`'s attempt. */
const providerTurnId = Effect.gen(function* () {
  const ids = yield* IdAllocatorV2;
  return ids.derive.providerTurn({
    driver: OPENCODE_PROVIDER,
    nativeTurnId: `${SESSION}:attempt:attempt:opencode2-adapter`,
  });
}).pipe(Effect.provide(idAllocatorLayer));
