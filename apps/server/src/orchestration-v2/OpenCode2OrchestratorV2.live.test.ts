/**
 * Runs OpenCode 2 through the whole orchestrator with the real driver: the
 * driver probes the binary, spawns `opencode serve`, and routes to the 2.x
 * adapter. One turn reads a file and runs a shell command; a second is
 * stopped while its shell command runs.
 *
 *   OPENCODE2_BIN=/path/to/opencode vp test run src/orchestration-v2/OpenCode2OrchestratorV2.live.test.ts
 *
 * The server runs with isolated HOME and XDG directories on the free
 * `opencode/big-pickle` model; `OPENCODE2_MODEL` picks another. A second run
 * covers plan mode, a workspace command and skill, and `/compact`.
 */
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { SourceControlProviderRegistry } from "../sourceControl/SourceControlProviderRegistry.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlite from "node:sqlite";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  type ModelSelection,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { describe } from "vite-plus/test";

import * as ResetCreditCoordinator from "../provider/Layers/resetCreditCoordinator.ts";
import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as HostPowerMonitor from "../background/HostPowerMonitor.ts";
import { ServerConfig } from "../config.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { AntigravityInstallation } from "../provider/AntigravityInstallation.ts";
import { CodexInstallation } from "../provider/CodexInstallation.ts";
import { ServerEnvironmentIdentity } from "../environment/ServerEnvironment.ts";
import * as ModelManifest from "../provider/ModelManifest.ts";
import { ProviderInstanceRegistryHydrationLive } from "../provider/Layers/ProviderInstanceRegistryHydration.ts";
import {
  NoOpProviderEventLoggers,
  ProviderEventLoggers,
} from "../provider/Layers/ProviderEventLoggers.ts";
import * as OpenCode2Client from "../provider/opencode2/OpenCode2Client.ts";
import { OpenCodeRuntimeLive } from "../provider/opencodeRuntime.ts";
import * as OpenCodeServerLedger from "../provider/OpenCodeServerLedger.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import { runDaemonWithOptions as runEffectWorkerDaemonWithOptions } from "./EffectWorker.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import { ProviderInstanceRegistry } from "../provider/Services/ProviderInstanceRegistry.ts";
import { worktreeRepairDependenciesTestLayer } from "./ProviderTurnStartService.testkit.ts";
import { OrchestrationV2LayerLive } from "./runtimeLayer.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";

const binaryPath = process.env.OPENCODE2_BIN;
const ROOT = process.env.OPENCODE2_LIVE_ROOT ?? "";
const INSTANCE = ProviderInstanceId.make("opencode");
const MODEL: ModelSelection = {
  instanceId: INSTANCE,
  model: process.env.OPENCODE2_MODEL ?? "opencode/big-pickle",
};
// The free model the thread switches to mid-conversation.
const SWITCHED_MODEL = "opencode/mimo-v2.6-flash-free";

/** The OpenCode servers the driver spawned, newest last, so a test can kill one by its own PID. */
const spawnedPids: Array<number> = [];
const spawnedServers = Layer.succeed(
  OpenCodeServerLedger.OpenCodeServerLedger,
  OpenCodeServerLedger.OpenCodeServerLedger.of({
    track: ({ pid }) =>
      Effect.sync(() => {
        spawnedPids.push(pid);
        return Effect.void;
      }),
  }),
);

/**
 * Credentials for T3's MCP server. `OPENCODE2_MCP_URL` points them at a stand-in
 * MCP server the run can see called; without it they point nowhere, as in replay.
 */
const MCP_URL = process.env.OPENCODE2_MCP_URL ?? "http://127.0.0.1/mcp";
const mcpRegistryLayer = Layer.succeed(
  McpSessionRegistry.McpSessionRegistry,
  McpSessionRegistry.McpSessionRegistry.of({
    issue: ({ threadId, providerInstanceId }) =>
      Effect.succeed({
        config: {
          environmentId: EnvironmentId.make("environment:opencode2-live"),
          threadId,
          providerSessionId: `mcp-live:${threadId}`,
          providerInstanceId,
          endpoint: MCP_URL,
          authorizationHeader: `Bearer mcp-live:${threadId}`,
          browserToolsAvailable: false,
        },
      }),
    resolve: () => Effect.succeed(undefined),
    touch: () => Effect.void,
    revokeProviderSession: () => Effect.void,
    revokeThread: () => Effect.void,
    revokeAll: Effect.void,
  }),
);

const PlatformTestLayer = Layer.merge(
  NodeServices.layer,
  Layer.mock(SourceControlProviderRegistry)({ resolveLink: () => Effect.die("unused title link") }),
);
const serverConfigLayer = ServerConfig.layerTest(`${ROOT}/work`, { prefix: "t3-opencode2-live-" });
const vcsDriverRegistryLayer = VcsDriverRegistry.layer.pipe(
  Layer.provide(VcsProcess.layer),
  Layer.provide(serverConfigLayer),
  Layer.provide(PlatformTestLayer),
);
// Isolated OpenCode state: the server never touches the developer's own data.
const serverSettingsLayer = ServerSettingsService.layerTest({
  providerInstances: {
    [INSTANCE]: {
      driver: ProviderDriverKind.make("opencode"),
      enabled: true,
      environment: [
        { name: "HOME", value: ROOT },
        { name: "XDG_CONFIG_HOME", value: `${ROOT}/config` },
        { name: "XDG_DATA_HOME", value: `${ROOT}/data` },
        { name: "XDG_STATE_HOME", value: `${ROOT}/state` },
        { name: "XDG_CACHE_HOME", value: `${ROOT}/cache` },
      ],
      // `OPENCODE2_SERVER_URL` connects to an external server instead of spawning one.
      config: {
        enabled: true,
        binaryPath,
        ...(process.env.OPENCODE2_SERVER_URL === undefined
          ? {}
          : {
              serverUrl: process.env.OPENCODE2_SERVER_URL,
              serverPassword: process.env.OPENCODE2_SERVER_PASSWORD ?? "",
            }),
      },
    },
  },
});
const backgroundPolicyLayer = BackgroundPolicy.layer.pipe(
  Layer.provide(Layer.effect(HostPowerMonitor.HostPowerMonitor, HostPowerMonitor.make())),
  Layer.provide(serverSettingsLayer),
);
const providerInstanceRegistryLayer = ProviderInstanceRegistryHydrationLive.pipe(
  Layer.provide(
    Layer.mergeAll(
      serverConfigLayer.pipe(Layer.provide(PlatformTestLayer)),
      serverSettingsLayer,
      ServerSecretStore.layer.pipe(
        Layer.provide(serverConfigLayer),
        Layer.provide(PlatformTestLayer),
      ),
      NodeServices.layer,
      FetchHttpClient.layer,
      OpenCodeRuntimeLive.pipe(Layer.provide(spawnedServers), Layer.provide(PlatformTestLayer)),
      Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers),
      ModelManifest.layerTest,
      AntigravityInstallation.layer.pipe(
        Layer.provide(serverConfigLayer.pipe(Layer.provide(PlatformTestLayer))),
        Layer.provide(FetchHttpClient.layer),
        Layer.provide(PlatformTestLayer),
      ),
      // The Codex driver now resolves managed ChatGPT installs; these runs never launch Codex.
      Layer.mock(CodexInstallation)({ managedDirectory: "unused-managed-installation" }),
      Layer.succeed(ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(
          EnvironmentId.make("00000000-0000-4000-8000-000000000001"),
        ),
      }),
    ),
  ),
);
const liveLayer = OrchestrationV2LayerLive.pipe(
  Layer.provide(worktreeRepairDependenciesTestLayer),
  Layer.provide(mcpRegistryLayer),
  Layer.provide(SqlitePersistenceMemory),
  Layer.provide(CheckpointStore.layer.pipe(Layer.provide(vcsDriverRegistryLayer))),
  Layer.provide(serverConfigLayer),
  Layer.provide(serverSettingsLayer),
  // Merged, not only provided: the test reads the same instance the orchestrator uses.
  Layer.provideMerge(providerInstanceRegistryLayer),
  Layer.provide(ResetCreditCoordinator.layer),
  Layer.provide(backgroundPolicyLayer),
  Layer.provide(PlatformTestLayer),
);

const settled = (projection: OrchestrationV2ThreadProjection) =>
  projection.runs.length > 0 &&
  projection.runs.every(
    (run) => !["queued", "starting", "running", "waiting"].includes(run.status),
  );

const waitFor = Effect.fn("OpenCode2Live.waitFor")(function* (
  threadId: ThreadId,
  done: (projection: OrchestrationV2ThreadProjection) => boolean,
) {
  const orchestrator = yield* OrchestratorV2;
  for (let attempt = 0; attempt < 240; attempt += 1) {
    const projection = yield* orchestrator.getThreadProjection(threadId);
    if (done(projection)) return projection;
    yield* Effect.sleep("500 millis");
  }
  const last = yield* orchestrator.getThreadProjection(threadId);
  const items = last.turnItems.map((item) =>
    item.type === "error" ? `error:${item.failure.message}` : `${item.type}:${item.status}`,
  );
  return yield* Effect.die(
    new Error(
      `Timed out waiting on OpenCode 2 thread ${threadId}: runs ${last.runs.map((run) => run.status).join(",")}; items ${items.join(",")}`,
    ),
  );
});

const send = Effect.fn("OpenCode2Live.send")(function* (
  threadId: ThreadId,
  key: string,
  text: string,
  modelSelection: ModelSelection = MODEL,
) {
  const orchestrator = yield* OrchestratorV2;
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    createdBy: "user",
    creationSource: "web",
    commandId: CommandId.make(`command:opencode2-live:${key}`),
    threadId,
    messageId: MessageId.make(`message:opencode2-live:${key}`),
    text,
    attachments: [],
    modelSelection,
    dispatchMode: { type: "start_immediately" },
  });
});

const AssistantModel = Schema.fromJsonString(
  Schema.Struct({
    model: Schema.Struct({ providerID: Schema.String, id: Schema.String }),
    agent: Schema.optional(Schema.String),
  }),
);
const decodeAssistantModel = Schema.decodeUnknownSync(AssistantModel);

/**
 * The `provider/model` of each assistant message in a native session, oldest
 * first, from the spawned server's own database under the isolated XDG root.
 */
const assistantModels = (nativeSessionId: string) =>
  Effect.sync(() => {
    const db = new NodeSqlite.DatabaseSync(`${ROOT}/data/opencode/opencode.db`, { readOnly: true });
    try {
      return db
        .prepare(
          "SELECT data FROM session_message WHERE session_id = ? AND type = 'assistant' ORDER BY seq",
        )
        .all(nativeSessionId)
        .map((row) => decodeAssistantModel(row.data).model)
        .map((model) => `${model.providerID}/${model.id}`);
    } finally {
      db.close();
    }
  });

/** The agent that wrote each assistant message in a native session, oldest first. */
const assistantAgents = (nativeSessionId: string) =>
  Effect.sync(() => {
    const db = new NodeSqlite.DatabaseSync(`${ROOT}/data/opencode/opencode.db`, { readOnly: true });
    try {
      return db
        .prepare(
          "SELECT data FROM session_message WHERE session_id = ? AND type = 'assistant' ORDER BY seq",
        )
        .all(nativeSessionId)
        .map((row) => decodeAssistantModel(row.data).agent);
    } finally {
      db.close();
    }
  });

describe.runIf(binaryPath !== undefined && ROOT !== "")("OpenCode 2 live orchestrator", () => {
  it.live(
    "runs a tool turn and stops a running shell command through the real driver",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        yield* fs.writeFileString(path.join(ROOT, "work", "hello.txt"), "hello from t3 live\n");
        yield* runEffectWorkerDaemonWithOptions({ concurrency: 2 }).pipe(Effect.forkScoped);

        // A status check starts a fresh server, which lists no models for its
        // first few hundred milliseconds; the picker must still get them.
        const instance = yield* (yield* ProviderInstanceRegistry).getInstance(INSTANCE);
        assert.isDefined(instance);
        const status = yield* instance!.snapshot.refresh;
        assert.equal(status.status, "ready");
        assert.include(
          status.models.map((model) => model.slug),
          "opencode/big-pickle",
        );
        const orchestrator = yield* OrchestratorV2;
        const threadId = ThreadId.make("thread:opencode2-live");
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:opencode2-live:create"),
          threadId,
          projectId: ProjectId.make("project:opencode2-live"),
          title: "OpenCode 2 live",
          modelSelection: MODEL,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: `${ROOT}/work`,
        });

        yield* send(
          threadId,
          "tools",
          // The shell call outlasts the spawned server's 30 second idle timeout.
          "Use the read tool to read hello.txt, then run the shell command `sleep 35 && echo TOOL_OK` with the shell tool in the foreground (not in the background) and wait for it to finish, then reply DONE.",
        );
        const first = yield* waitFor(threadId, settled);
        assert.deepEqual(
          first.runs.map((run) => run.status),
          ["completed"],
        );
        const shell = first.turnItems.find((item) => item.type === "command_execution");
        assert.deepInclude(shell, { status: "completed", exitCode: 0 });
        assert.include(shell?.type === "command_execution" ? shell.output : "", "TOOL_OK");
        assert.isDefined(
          first.turnItems.find((item) => item.type === "dynamic_tool" && item.toolName === "read"),
        );
        assert.isAbove(first.providerTurns[0]?.tokenUsage?.maxTokens ?? 0, 0);

        yield* send(
          threadId,
          "stop",
          "Run the shell command `sleep 60 && echo LATE` with the shell tool in the foreground (not in the background) and wait for it to finish, then reply DONE.",
        );
        const running = yield* waitFor(threadId, (projection) =>
          projection.turnItems.some(
            (item) =>
              item.type === "command_execution" &&
              item.status === "running" &&
              item.input.includes("sleep 60"),
          ),
        );
        const secondRun = running.runs.at(-1)!;
        yield* orchestrator.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("command:opencode2-live:interrupt"),
          threadId,
          runId: secondRun.id,
        });
        const stopped = yield* waitFor(threadId, settled);
        assert.deepEqual(
          stopped.runs.map((run) => run.status),
          ["completed", "interrupted"],
        );
        const sleep = stopped.turnItems.find(
          (item) => item.type === "command_execution" && item.input.includes("sleep 60"),
        );
        assert.equal(sleep?.status, "interrupted");

        // A model change applies to the same native session on the next turn.
        const switched: ModelSelection = { instanceId: INSTANCE, model: SWITCHED_MODEL };
        yield* send(threadId, "switch", "Reply with exactly: SWITCHED", switched);
        const third = yield* waitFor(
          threadId,
          (projection) => projection.runs.length === 3 && settled(projection),
        );
        assert.equal(third.runs.at(-1)?.status, "completed");
        const sessionId = third.providerThreads[0]?.nativeThreadRef?.nativeId;
        assert.isDefined(sessionId);
        const models = yield* assistantModels(sessionId!);
        assert.equal(models.at(-1), SWITCHED_MODEL);
        assert.notEqual(models[0], SWITCHED_MODEL);

        // Supervised threads are refused, not run with every tool allowed.
        yield* orchestrator.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("command:opencode2-live:runtime-mode"),
          threadId,
          runtimeMode: "approval-required",
        });
        yield* send(threadId, "supervised", "Create a file named supervised.txt containing NO.");
        const refused = yield* waitFor(
          threadId,
          (projection) => projection.runs.length === 4 && settled(projection),
        );
        assert.equal(refused.runs.at(-1)?.status, "failed");
        assert.isFalse(yield* fs.exists(path.join(ROOT, "work", "supervised.txt")));
      }).pipe(Effect.provide(Layer.merge(liveLayer, NodeServices.layer)), Effect.scoped),
    360_000,
  );

  it.live(
    "runs plan mode, a workspace command and skill, and /compact through the real driver",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const work = path.join(ROOT, "work");
        yield* fs.makeDirectory(path.join(work, ".opencode", "command"), { recursive: true });
        yield* fs.makeDirectory(path.join(work, ".opencode", "skills", "greet"), {
          recursive: true,
        });
        yield* fs.writeFileString(
          path.join(work, ".opencode", "command", "hello.md"),
          "---\ndescription: Say hello to the workspace\n---\nReply with exactly: HELLO $ARGUMENTS\n",
        );
        yield* fs.writeFileString(
          path.join(work, ".opencode", "skills", "greet", "SKILL.md"),
          "---\nname: greet\ndescription: Greets the user with the secret word MANGO.\n---\nWhen this skill is active, begin your reply with the exact word MANGO.\n",
        );
        yield* runEffectWorkerDaemonWithOptions({ concurrency: 2 }).pipe(Effect.forkScoped);

        // The workspace's own command and skill reach the composer's pickers.
        const instance = yield* (yield* ProviderInstanceRegistry).getInstance(INSTANCE);
        assert.isDefined(instance);
        const workspace = yield* instance!.snapshotForCwd!(work);
        assert.include(
          workspace.skills.map((skill) => skill.name),
          "greet",
        );
        assert.includeMembers(
          workspace.slashCommands.map((command) => command.name),
          ["compact", "hello"],
        );

        const orchestrator = yield* OrchestratorV2;
        const threadId = ThreadId.make("thread:opencode2-live-modes");
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:opencode2-live-modes:create"),
          threadId,
          projectId: ProjectId.make("project:opencode2-live-modes"),
          title: "OpenCode 2 live modes",
          modelSelection: MODEL,
          runtimeMode: "full-access",
          interactionMode: "plan",
          branch: null,
          worktreePath: work,
        });
        const runs = (count: number) => (projection: OrchestrationV2ThreadProjection) =>
          projection.runs.length === count && settled(projection);
        const lastReply = (projection: OrchestrationV2ThreadProjection) =>
          projection.turnItems.findLast((item) => item.type === "assistant_message");

        // Plan mode is OpenCode's plan agent: it plans instead of editing.
        yield* send(
          threadId,
          "modes-plan",
          "Plan how to add a --verbose flag to a script named cli.js. Present a short plan; do not implement it.",
        );
        const planned = yield* waitFor(threadId, runs(1));
        assert.equal(planned.runs[0]?.status, "completed");
        const sessionId = planned.providerThreads[0]?.nativeThreadRef?.nativeId;
        assert.isDefined(sessionId);
        const planSteps = yield* assistantAgents(sessionId!);
        assert.isAbove(planSteps.length, 0);
        assert.isTrue(planSteps.every((agent) => agent === "plan"));

        yield* orchestrator.dispatch({
          type: "thread.interaction-mode.set",
          commandId: CommandId.make("command:opencode2-live-modes:default"),
          threadId,
          interactionMode: "default",
        });
        yield* send(threadId, "modes-command", "/hello WORLD");
        const commanded = yield* waitFor(threadId, runs(2));
        assert.equal(commanded.runs[1]?.status, "completed");
        assert.include(lastReply(commanded)?.text ?? "", "HELLO WORLD");
        assert.equal((yield* assistantAgents(sessionId!)).at(-1), "build");

        yield* send(threadId, "modes-skill", "Use $greet to say hi in three words.");
        const skilled = yield* waitFor(threadId, runs(3));
        assert.equal(skilled.runs[2]?.status, "completed");
        assert.include(lastReply(skilled)?.text ?? "", "MANGO");

        yield* send(threadId, "modes-compact", "/compact");
        const compacted = yield* waitFor(threadId, runs(4));
        assert.equal(compacted.runs[3]?.status, "completed");
        const compaction = compacted.turnItems.find((item) => item.type === "compaction");
        assert.equal(compaction?.status, "completed");
        assert.equal(compaction?.runId, compacted.runs[3]?.id);
        assert.isAbove(
          compaction?.type === "compaction" ? (compaction.summary ?? "").length : 0,
          0,
        );
      }).pipe(Effect.provide(Layer.merge(liveLayer, NodeServices.layer)), Effect.scoped),
    360_000,
  );

  it.live(
    "generates a title, calls T3's MCP server, and reconciles a turn cut off by a killed server",
    () =>
      Effect.gen(function* () {
        const work = `${ROOT}/work`;
        yield* runEffectWorkerDaemonWithOptions({ concurrency: 2 }).pipe(Effect.forkScoped);
        const orchestrator = yield* OrchestratorV2;
        const instance = yield* (yield* ProviderInstanceRegistry).getInstance(INSTANCE);
        assert.isDefined(instance);
        yield* instance!.snapshot.refresh;

        // A thread title, generated in a temporary session on the 2.x server.
        const title = yield* instance!.textGeneration.generateThreadTitle({
          cwd: work,
          message: "fix the login redirect loop after oauth",
          modelSelection: MODEL,
        });
        assert.isAbove(title.title.length, 0);

        const threadId = ThreadId.make("thread:opencode2-live-restart");
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:opencode2-live-restart:create"),
          threadId,
          projectId: ProjectId.make("project:opencode2-live-restart"),
          title: "OpenCode 2 live restart",
          modelSelection: MODEL,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: work,
        });
        const runs = (count: number) => (projection: OrchestrationV2ThreadProjection) =>
          projection.runs.length === count && settled(projection);

        // T3's MCP server for this thread: the stand-in's one tool answers a marker.
        if (process.env.OPENCODE2_MCP_URL !== undefined) {
          yield* send(
            threadId,
            "restart-mcp",
            "Call the echo_marker tool from the T3 Code MCP server with word 'kiwi', then reply with its exact output and nothing else.",
          );
          const called = yield* waitFor(threadId, runs(1));
          assert.equal(called.runs[0]?.status, "completed");
          const reply = called.turnItems.findLast((item) => item.type === "assistant_message");
          assert.include(reply?.type === "assistant_message" ? reply.text : "", "MARKER-KIWI-7Q9");
        }
        const before = (yield* orchestrator.getThreadProjection(threadId)).runs.length;

        // The spawned server dies mid-command; T3 restarts it and settles the turn.
        yield* send(
          threadId,
          "restart-killed",
          "Run the shell command `sleep 60 && echo LATE` with the shell tool in the foreground and wait for it, then reply DONE.",
        );
        yield* waitFor(threadId, (projection) =>
          projection.turnItems.some(
            (item) =>
              item.type === "command_execution" &&
              item.status === "running" &&
              item.input.includes("sleep 60"),
          ),
        );
        const pid = spawnedPids.at(-1);
        assert.isDefined(pid);
        process.kill(pid!, "SIGKILL");
        const reconciled = yield* waitFor(threadId, runs(before + 1));
        assert.equal(reconciled.runs.at(-1)?.status, "interrupted");

        yield* send(
          threadId,
          "restart-next",
          "What did I last ask you to run? Answer in one short sentence.",
        );
        const next = yield* waitFor(threadId, runs(before + 2));
        assert.equal(next.runs.at(-1)?.status, "completed");
        const answer = next.turnItems.findLast((item) => item.type === "assistant_message");
        assert.include(answer?.type === "assistant_message" ? answer.text : "", "sleep 60");
        assert.lengthOf(next.providerThreads, 1);
      }).pipe(Effect.provide(Layer.merge(liveLayer, NodeServices.layer)), Effect.scoped),
    360_000,
  );

  // Needs an external server behind a proxy that cuts its event streams on
  // `GET <proxy>/__drop`, so only the stream drops while the server stays up.
  it.live.runIf(process.env.OPENCODE2_DROP_URL !== undefined)(
    "picks a turn back up after an external server's event stream drops mid-turn",
    () =>
      Effect.gen(function* () {
        const work = `${ROOT}/work`;
        yield* runEffectWorkerDaemonWithOptions({ concurrency: 2 }).pipe(Effect.forkScoped);
        const orchestrator = yield* OrchestratorV2;
        const threadId = ThreadId.make("thread:opencode2-live-drop");
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:opencode2-live-drop:create"),
          threadId,
          projectId: ProjectId.make("project:opencode2-live-drop"),
          title: "OpenCode 2 live drop",
          modelSelection: MODEL,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: work,
        });
        yield* send(
          threadId,
          "drop-running",
          "Run the shell command `sleep 8 && echo AFTER_DROP` with the shell tool in the foreground and wait for it, then reply with its exact output.",
        );
        yield* waitFor(threadId, (projection) =>
          projection.turnItems.some(
            (item) => item.type === "command_execution" && item.status === "running",
          ),
        );
        yield* HttpClient.get(process.env.OPENCODE2_DROP_URL!).pipe(
          Effect.provide(FetchHttpClient.layer),
          Effect.orDie,
        );
        // The turn keeps running on the new stream and ends with its reply.
        const done = yield* waitFor(threadId, settled);
        assert.equal(done.runs[0]?.status, "completed");
        const shell = done.turnItems.find((item) => item.type === "command_execution");
        assert.equal(shell?.status, "completed");
        const reply = done.turnItems.findLast((item) => item.type === "assistant_message");
        assert.include(reply?.type === "assistant_message" ? reply.text : "", "AFTER_DROP");
        // The thread's T3 MCP server is registered on the external server for now.
        const opencode = yield* OpenCode2Client.make.pipe(Effect.provide(FetchHttpClient.layer));
        const api = yield* opencode.connect({
          baseUrl: process.env.OPENCODE2_SERVER_URL!,
          password: process.env.OPENCODE2_SERVER_PASSWORD ?? "",
        });
        const servers = yield* api.client.mcp.list({ location: { directory: work } });
        assert.deepEqual(
          servers.data.map((server) => server.name),
          [],
          "an external server gets no T3 MCP server, as with 1.x",
        );
      }).pipe(Effect.provide(Layer.merge(liveLayer, NodeServices.layer)), Effect.scoped),
    360_000,
  );
});
