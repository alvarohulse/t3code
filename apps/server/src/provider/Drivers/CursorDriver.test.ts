// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "node:crypto";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { CursorDriver } from "./CursorDriver.ts";

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-cursor-driver-copy-command-",
}).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
    }),
  ),
  Layer.provideMerge(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
  Layer.provideMerge(
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die("Disabled Cursor must not make an HTTP request")),
    ),
  ),
);

// The `#!/bin/sh` stub below cannot be resolved as an executable on Windows.
const windowsHost = HostProcessPlatform.defaultValue() === "win32";

it.layer(testLayer)("CursorDriver", (it) => {
  it.effect.skipIf(windowsHost)(
    "quotes a configured executable path in the copyable update command",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-cursor-driver-" });
        const binaryPath = NodePath.join(tempDir, "Cursor Tools", "bin", "cursor-agent");
        yield* fs.makeDirectory(NodePath.dirname(binaryPath), { recursive: true });
        yield* fs.writeFileString(binaryPath, "#!/bin/sh\n");
        yield* fs.chmod(binaryPath, 0o755);

        const instance = yield* CursorDriver.create({
          instanceId: ProviderInstanceId.make("cursor-copy-command"),
          displayName: "Cursor test",
          enabled: false,
          environment: [],
          config: { ...CursorDriver.defaultConfig(), binaryPath },
        });

        const capabilities = yield* instance.snapshot.resolveMaintenance();
        expect(capabilities.update).toMatchObject({
          command: `'${binaryPath}' update`,
          executable: binaryPath,
          args: ["update"],
        });
        expect((yield* instance.snapshot.refresh).status).toBe("disabled");
      }).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("Disabled Cursor must not spawn a process")),
        ),
        Effect.scoped,
      ),
  );

  it.effect("stays manual-only when the configured executable does not exist", () =>
    Effect.gen(function* () {
      const instance = yield* CursorDriver.create({
        instanceId: ProviderInstanceId.make("cursor-missing"),
        displayName: "Cursor test",
        enabled: false,
        environment: [],
        config: {
          ...CursorDriver.defaultConfig(),
          binaryPath: NodePath.join(NodeOS.tmpdir(), "t3-cursor-missing", "cursor-agent"),
        },
      });
      expect((yield* instance.snapshot.resolveMaintenance()).update).toBeNull();
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Disabled Cursor must not spawn a process")),
      ),
      Effect.scoped,
    ),
  );

  it.effect.skipIf(windowsHost)(
    "keeps reading limits when the CLI reports a working login as logged out",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-cursor-driver-" });
        // `agent about` intermittently prints this for a login that works.
        const binaryPath = writeFakeCli({
          directory: tempDir,
          name: "cursor-agent",
          source: [
            'if (process.argv[2] === "about") {',
            '  process.stdout.write("CLI Version         2026.09.28-64d2043\\n");',
            '  process.stdout.write("User Email          Not logged in\\n");',
            "  process.exit(0);",
            "}",
            "process.exit(1);",
          ].join("\n"),
        });
        const httpClient = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              request.url.endsWith("/GetCurrentPeriodUsage")
                ? Response.json({ planUsage: { totalPercentUsed: 42 } })
                : new Response(null, { status: 404 }),
            ),
          ),
        );

        // @effect-diagnostics-next-line preferSchemaOverJson:off
        const payload = Buffer.from(JSON.stringify({ sub: "user_123" })).toString("base64url");
        const accessToken = `header.${payload}.signature`;

        const instance = yield* CursorDriver.create({
          instanceId: ProviderInstanceId.make("cursor-flaky-login"),
          displayName: "Cursor test",
          enabled: true,
          environment: [
            { name: "CURSOR_AUTH_TOKEN", value: accessToken, sensitive: true },
            { name: "HOME", value: tempDir, sensitive: false },
          ],
          config: { ...CursorDriver.defaultConfig(), binaryPath },
        }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient));

        const snapshot = yield* instance.snapshot.refresh;
        expect(snapshot.auth.status).toBe("unauthenticated");
        expect(snapshot.usageLimits?.windows[0]?.usedPercent).toBe(42);
        // Without an email, the account id still matches other environments.
        expect(snapshot.usageLimits?.credentialFingerprint).toBe(
          NodeCrypto.createHash("sha256").update("user_123").digest("hex"),
        );
      }).pipe(Effect.scoped),
  );
});
