import * as NodeCrypto from "node:crypto";
import * as NodeOS from "node:os";
import type { CursorSettings, ServerProviderUsageWindow } from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { CURSOR_USAGE_WINDOWS } from "@t3tools/shared/usageLimits";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";
import {
  clampPercent,
  makeUnavailableUsageLimits,
  makeUsageLimits,
} from "../providerUsageLimits.ts";
import { readMacCursorAccessToken } from "../cursorCredentialStore.ts";

const CursorCredentials = Schema.Struct({ accessToken: Schema.optional(Schema.String) });
const DEFAULT_CURSOR_API_ENDPOINT = "https://api2.cursor.sh";
const decodeCredentials = Schema.decodeEffect(Schema.fromJsonString(CursorCredentials));
const CursorUsageResponse = Schema.Struct({
  billingCycleEnd: Schema.optional(Schema.Union([Schema.String, Schema.Number])),
  planUsage: Schema.optional(
    Schema.Struct({
      totalPercentUsed: Schema.optional(Schema.Number),
      autoPercentUsed: Schema.optional(Schema.Number),
      apiPercentUsed: Schema.optional(Schema.Number),
    }),
  ),
});

const CursorTeams = Schema.Struct({
  teams: Schema.optional(
    Schema.Array(
      Schema.Struct({
        id: Schema.Number,
        hasBilling: Schema.optional(Schema.Boolean),
        billingCycleStart: Schema.optional(Schema.String),
        billingCycleEnd: Schema.optional(Schema.String),
      }),
    ),
  ),
});
const CursorAggregatedUsage = Schema.Struct({ totalCostCents: Schema.optional(Schema.Number) });
const CursorHardLimit = Schema.Struct({
  perUserMonthlyLimitDollars: Schema.optional(Schema.Number),
});

type CursorBillingCycle = { readonly start: number; readonly end: number };

/**
 * The per-user spend limit on a team plan billed per use. Such accounts get
 * no plan percentages, so this is the only bar they have. `totalCostCents`
 * is the caller's own spend including Cursor's token fee, which is what the
 * limit is enforced against.
 */
export function cursorSpendLimitWindow(input: {
  readonly cycle: CursorBillingCycle;
  readonly spentCents: number;
  readonly limitDollars: number;
}): ServerProviderUsageWindow | undefined {
  const { cycle, spentCents, limitDollars } = input;
  if (!(limitDollars > 0) || !Number.isFinite(spentCents) || !(cycle.end > cycle.start)) {
    return undefined;
  }
  const usedUsd = Math.max(0, spentCents) / 100;
  const reset = DateTime.make(cycle.end);
  return {
    id: "spendLimit",
    kind: "monthly",
    label: "Spend limit",
    usedPercent: clampPercent((usedUsd / limitDollars) * 100),
    windowDurationMins: Math.round((cycle.end - cycle.start) / 60_000),
    ...(Option.isSome(reset) ? { resetsAt: DateTime.formatIso(reset.value) } : {}),
    spend: { usedUsd, limitUsd: limitDollars },
  };
}

/** Cursor's dashboard percentages include bonus usage; spend / limit does not. */
export function cursorUsageResponseToLimits(
  response: typeof CursorUsageResponse.Type,
  checkedAt: string,
  spendLimit?: ServerProviderUsageWindow,
) {
  const reset = DateTime.make(Number(response.billingCycleEnd));
  const resetsAt =
    Number(response.billingCycleEnd) > 0 && Option.isSome(reset)
      ? DateTime.formatIso(reset.value)
      : undefined;
  const windows: ServerProviderUsageWindow[] = [];
  if (response.planUsage) {
    for (const { id, label } of CURSOR_USAGE_WINDOWS) {
      const usedPercent = response.planUsage[id];
      if (usedPercent === undefined || !Number.isFinite(usedPercent)) continue;
      windows.push({
        id,
        kind: "monthly",
        label,
        usedPercent: clampPercent(usedPercent),
        ...(resetsAt ? { resetsAt } : {}),
      });
    }
  }
  if (spendLimit) windows.push(spendLimit);
  return windows.length > 0
    ? makeUsageLimits({ checkedAt, windows })
    : makeUnavailableUsageLimits({ checkedAt, reason: "unsupported" });
}

/** The account id inside a Cursor access token, which is the same for every login of one account. */
function cursorTokenSubject(token: string): string | null {
  try {
    const payload: unknown = JSON.parse(
      Buffer.from(token.split(".")[1] ?? "", "base64url").toString("utf8"),
    );
    const subject =
      typeof payload === "object" && payload !== null && "sub" in payload ? payload.sub : null;
    return typeof subject === "string" && subject ? subject : null;
  } catch {
    return null;
  }
}

export const readCursorUsageLimits = Effect.fn("readCursorUsageLimits")(function* (
  settings: Pick<CursorSettings, "apiEndpoint">,
  environment: NodeJS.ProcessEnv = process.env,
  allowKeychain = false,
  keychainToken: () => Promise<string | null> = readMacCursorAccessToken,
) {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  return yield* Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const platform = yield* HostProcessPlatform;
    const endpoint = (
      settings.apiEndpoint.trim() ||
      environment.CURSOR_API_ENDPOINT?.trim() ||
      DEFAULT_CURSOR_API_ENDPOINT
    ).replace(/\/$/, "");
    const client = yield* HttpClient.HttpClient;
    let token = environment.CURSOR_AUTH_TOKEN?.trim();
    const apiKey = environment.CURSOR_API_KEY?.trim();
    // An explicit API key can name a different account from the stored login,
    // so it is exchanged for its own account's token and the login is ignored.
    if (!token && apiKey) {
      const exchanged = yield* client
        .execute(
          HttpClientRequest.post(`${endpoint}/auth/exchange_user_api_key`).pipe(
            HttpClientRequest.bearerToken(apiKey),
            HttpClientRequest.bodyJsonUnsafe({}),
          ),
        )
        .pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.flatMap(HttpClientResponse.schemaBodyJson(CursorCredentials)),
        );
      token = exchanged.accessToken?.trim();
      if (!token) return makeUnavailableUsageLimits({ checkedAt, reason: "unsupported" });
    }
    const credentialStore = environment.AGENT_CLI_CREDENTIAL_STORE;
    if (!token && credentialStore === "memory") {
      return makeUnavailableUsageLimits({
        checkedAt,
        reason: "unsupported",
        message: "Cursor usage requires a CLI login or CURSOR_AUTH_TOKEN.",
      });
    }
    if (!token && platform === "darwin" && credentialStore !== "file") {
      if (!allowKeychain) {
        return makeUnavailableUsageLimits({
          checkedAt,
          reason: "unsupported",
          message: "Enable Cursor account usage in T3 Code to read its Keychain login.",
        });
      }
      if (endpoint !== DEFAULT_CURSOR_API_ENDPOINT) {
        return makeUnavailableUsageLimits({
          checkedAt,
          reason: "unsupported",
          message: "Cursor account usage requires the default Cursor endpoint when using Keychain.",
        });
      }
      token = (yield* Effect.tryPromise(keychainToken))?.trim();
    } else if (!token) {
      const home =
        (platform === "win32" ? environment.USERPROFILE : environment.HOME) || NodeOS.homedir();
      const directory =
        platform === "win32"
          ? path.join(environment.APPDATA || path.join(home, "AppData", "Roaming"), "Cursor")
          : platform === "darwin"
            ? path.join(home, ".cursor")
            : path.join(environment.XDG_CONFIG_HOME || path.join(home, ".config"), "cursor");
      const credentials = yield* fs.readFileString(path.join(directory, "auth.json")).pipe(
        Effect.catchTags({
          PlatformError: (error) =>
            error.reason._tag === "NotFound" ? Effect.succeed("{}") : Effect.fail(error),
        }),
        Effect.flatMap(decodeCredentials),
      );
      token = credentials.accessToken?.trim();
    }
    if (!token) return makeUnavailableUsageLimits({ checkedAt, reason: "unsupported" });
    const response = yield* client.execute(
      HttpClientRequest.post(`${endpoint}/aiserver.v1.DashboardService/GetCurrentPeriodUsage`).pipe(
        HttpClientRequest.bearerToken(token),
        HttpClientRequest.setHeaders({
          "connect-protocol-version": "1",
          "x-cursor-client-type": "cli",
        }),
        HttpClientRequest.bodyJsonUnsafe({}),
      ),
    );
    const body = yield* HttpClientResponse.schemaBodyJson(CursorUsageResponse)(
      yield* HttpClientResponse.filterStatusOk(response),
    );
    const dashboard = <S extends Schema.Top>(method: string, payload: object, schema: S) =>
      client
        .execute(
          HttpClientRequest.post(`${endpoint}/aiserver.v1.DashboardService/${method}`).pipe(
            HttpClientRequest.bearerToken(token),
            HttpClientRequest.setHeaders({ "connect-protocol-version": "1" }),
            HttpClientRequest.bodyJsonUnsafe(payload),
          ),
        )
        .pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.flatMap(HttpClientResponse.schemaBodyJson(schema)),
        );
    // Only the caller's own spend and limit are read; the team-wide spend
    // listing names every member and is never requested.
    const spendLimit = yield* Effect.gen(function* () {
      const { teams = [] } = yield* dashboard("GetTeams", {}, CursorTeams);
      const team = teams.find((candidate) => candidate.hasBilling);
      if (!team?.billingCycleStart || !team.billingCycleEnd) return undefined;
      const cycle = { start: Number(team.billingCycleStart), end: Number(team.billingCycleEnd) };
      const [usage, limit] = yield* Effect.all(
        [
          dashboard(
            "GetAggregatedUsageEvents",
            {
              teamId: team.id,
              startDate: team.billingCycleStart,
              endDate: team.billingCycleEnd,
            },
            CursorAggregatedUsage,
          ),
          dashboard("GetHardLimit", { teamId: team.id }, CursorHardLimit),
        ],
        { concurrency: 2 },
      );
      return cursorSpendLimitWindow({
        cycle,
        spentCents: usage.totalCostCents ?? 0,
        limitDollars: limit.perUserMonthlyLimitDollars ?? 0,
      });
    }).pipe(Effect.orElseSucceed(() => undefined));
    const limits = cursorUsageResponseToLimits(body, checkedAt, spendLimit);
    const accountId = cursorTokenSubject(token);
    // Matches the account across environments even when one CLI reports no email.
    return accountId && limits.unavailable === undefined
      ? {
          ...limits,
          credentialFingerprint: NodeCrypto.createHash("sha256").update(accountId).digest("hex"),
        }
      : limits;
  }).pipe(
    Effect.timeout("10 seconds"),
    Effect.orElseSucceed(() =>
      makeUnavailableUsageLimits({
        checkedAt,
        reason: "probeFailed",
        message: "Cursor could not read usage limits.",
      }),
    ),
  );
});
