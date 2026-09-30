// @effect-diagnostics globalDate:off
/**
 * Folds parsed transcript records into `(day, hourStart?, provider, model)`
 * buckets.
 *
 * `Intl.DateTimeFormat` is the only reliable way to resolve a wall-clock day in
 * an arbitrary IANA zone, and it takes a `Date`. That is why the raw `Date`
 * construction is allowed here; nothing in this module reads the clock.
 *
 * Pure, so the bucketing and de-duplication rules are testable without touching
 * the filesystem or the network.
 *
 * @module usageAggregation
 */
import type { UsageBucket, UsageDay, UsageResolution, UsageTokenTotals } from "@t3tools/contracts";

import {
  addTotals,
  EMPTY_TOTALS,
  isLargerUsageSnapshot,
  type UsageRecord,
} from "./usageTranscripts.ts";
import { cacheSavingsUsd, priceUsage, type RateTable } from "./usagePricing.ts";

function scaleTotals(totals: UsageTokenTotals, sign: 1 | -1): UsageTokenTotals {
  return {
    uncachedInputTokens: sign * totals.uncachedInputTokens,
    cachedInputTokens: sign * totals.cachedInputTokens,
    cacheCreationTokens: sign * totals.cacheCreationTokens,
    outputTokens: sign * totals.outputTokens,
    reasoningTokens: sign * totals.reasoningTokens,
  };
}

/**
 * Formats an instant as a `YYYY-MM-DD` day in `timeZone`.
 *
 * `en-CA` yields ISO-ordered parts, which is why it is used here rather than
 * assembling the day from `Date` getters (those are host-local only).
 */
function makeDayFormatter(timeZone: string): (timestampMs: number) => string {
  let format: Intl.DateTimeFormat;
  try {
    format = new Intl.DateTimeFormat("en-CA", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
  } catch {
    // An unknown zone should degrade to UTC rather than fail the whole scan.
    format = new Intl.DateTimeFormat("en-CA", {
      timeZone: "UTC",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
  }
  return (timestampMs) => format.format(new Date(timestampMs));
}

const HOUR_MS = 60 * 60 * 1000;

interface MutableBucket {
  totals: UsageTokenTotals;
  costUsd: number;
  cacheSavingsUsd: number;
  records: number;
  unpricedRecords: number;
  providerReportedRecords: number;
  sessions: Set<string>;
}

export interface AggregateOptions {
  readonly timeZone: string;
  readonly sinceDay: string;
  readonly untilDay: string;
  readonly rates: RateTable;
  readonly priceOverrides?: RateTable;
  readonly resolution?: UsageResolution;
  readonly sinceTimeMs?: number;
  readonly untilTimeMs?: number;
}

export interface AggregateResult {
  readonly buckets: readonly UsageBucket[];
  /** Records dropped because an earlier record carried the same dedupe key. */
  readonly duplicatesDropped: number;
  /** Records whose day fell outside the requested window. */
  readonly outOfWindow: number;
}

/**
 * Accumulates records across many files.
 *
 * De-duplication is global across the whole scan, not per file: Claude Code
 * copies a message's records forward when a session is resumed or forked, so
 * the same `dedupeKey` legitimately appears in several transcripts. Each key
 * counts once, in the bucket of its first record, at its largest snapshot.
 */
export class UsageAggregator {
  readonly #buckets = new Map<string, MutableBucket>();
  /** The counted record per key, and its bucket (`null` when out of window). */
  readonly #seen = new Map<
    string,
    { record: UsageRecord; readonly bucket: MutableBucket | null }
  >();
  readonly #toDay: (timestampMs: number) => string;
  readonly #hourlyWindow: { readonly sinceTimeMs: number; readonly untilTimeMs: number } | null;
  readonly #options: AggregateOptions;
  #duplicatesDropped = 0;
  #outOfWindow = 0;

  constructor(options: AggregateOptions) {
    this.#options = options;
    this.#toDay = makeDayFormatter(options.timeZone);
    if (options.resolution === "hour") {
      if (options.sinceTimeMs === undefined || options.untilTimeMs === undefined) {
        throw new Error("Hourly usage aggregation requires exact time bounds");
      }
      this.#hourlyWindow = {
        sinceTimeMs: options.sinceTimeMs,
        untilTimeMs: options.untilTimeMs,
      };
    } else {
      this.#hourlyWindow = null;
    }
  }

  /**
   * Folds one record in. Returns whether it actually contributed, so callers
   * can derive per-window facts (distinct sessions, for one) from the records
   * that landed rather than everything the mtime prefilter happened to admit.
   */
  add(record: UsageRecord, sourcePath?: string): boolean {
    const counted = record.dedupeKey === null ? undefined : this.#seen.get(record.dedupeKey);
    if (counted !== undefined) {
      this.#duplicatesDropped += 1;
      if (isLargerUsageSnapshot(record, counted.record)) {
        if (counted.bucket !== null) {
          this.#apply(counted.bucket, counted.record, -1);
          this.#apply(counted.bucket, record, 1);
        }
        counted.record = record;
      }
      return false;
    }

    const bucket = this.#bucketFor(record, sourcePath);
    if (record.dedupeKey !== null) this.#seen.set(record.dedupeKey, { record, bucket });
    if (bucket === null) {
      this.#outOfWindow += 1;
      return false;
    }

    this.#apply(bucket, record, 1);
    bucket.records += 1;
    if (record.sessionId.length > 0) bucket.sessions.add(record.sessionId);
    return true;
  }

  /** Adds (`sign = 1`) or removes (`sign = -1`) a record's tokens and cost. */
  #apply(bucket: MutableBucket, record: UsageRecord, sign: 1 | -1): void {
    const priced = priceUsage(this.#options.rates, record, this.#options.priceOverrides);
    bucket.totals = addTotals(bucket.totals, scaleTotals(record.totals, sign));
    bucket.costUsd += sign * priced.costUsd;
    bucket.cacheSavingsUsd +=
      sign * cacheSavingsUsd(this.#options.rates, record, this.#options.priceOverrides);
    if (priced.costSource === "unpriced") bucket.unpricedRecords += sign;
    if (priced.costSource === "providerReported") bucket.providerReportedRecords += sign;
  }

  #bucketFor(record: UsageRecord, sourcePath: string | undefined): MutableBucket | null {
    if (
      this.#hourlyWindow !== null &&
      (record.timestampMs < this.#hourlyWindow.sinceTimeMs ||
        record.timestampMs >= this.#hourlyWindow.untilTimeMs)
    ) {
      return null;
    }

    const day = this.#toDay(record.timestampMs);
    if (
      this.#hourlyWindow === null &&
      (day < this.#options.sinceDay || day > this.#options.untilDay)
    ) {
      return null;
    }

    const hourStart =
      this.#hourlyWindow === null
        ? ""
        : new Date(
            this.#hourlyWindow.sinceTimeMs +
              Math.floor((record.timestampMs - this.#hourlyWindow.sinceTimeMs) / HOUR_MS) * HOUR_MS,
          ).toISOString();
    const key = `${day}\u0000${hourStart}\u0000${record.provider}\u0000${record.model}\u0000${sourcePath ?? ""}`;
    let bucket = this.#buckets.get(key);
    if (bucket === undefined) {
      bucket = {
        totals: EMPTY_TOTALS,
        costUsd: 0,
        cacheSavingsUsd: 0,
        records: 0,
        unpricedRecords: 0,
        providerReportedRecords: 0,
        sessions: new Set<string>(),
      };
      this.#buckets.set(key, bucket);
    }
    return bucket;
  }

  finish(): AggregateResult {
    const buckets: UsageBucket[] = [];
    for (const [key, bucket] of this.#buckets) {
      const [day = "", hourStart = "", provider = "", model = "", sourcePath = ""] =
        key.split("\u0000");
      buckets.push({
        day: day as UsageDay,
        ...(hourStart === "" ? {} : { hourStart }),
        provider: provider as UsageBucket["provider"],
        model,
        ...(sourcePath === "" ? {} : { sourcePath }),
        totals: bucket.totals,
        costUsd: bucket.costUsd,
        cacheSavingsUsd: bucket.cacheSavingsUsd,
        costSource: resolveCostSource(bucket),
        records: bucket.records,
        unpricedRecords: bucket.unpricedRecords,
        sessions: bucket.sessions.size,
      });
    }
    // Stable ordering keeps payloads diffable and snapshot tests meaningful.
    buckets.sort(
      (a, b) =>
        a.day.localeCompare(b.day) ||
        (a.hourStart ?? "").localeCompare(b.hourStart ?? "") ||
        a.provider.localeCompare(b.provider) ||
        a.model.localeCompare(b.model),
    );

    return {
      buckets,
      duplicatesDropped: this.#duplicatesDropped,
      outOfWindow: this.#outOfWindow,
    };
  }
}

/**
 * A bucket mixes records from one model, but their cost provenance can differ
 * when only some records carried a reported cost. The weakest provenance in the
 * bucket wins so the UI never overstates confidence.
 */
function resolveCostSource(bucket: MutableBucket): UsageBucket["costSource"] {
  if (bucket.unpricedRecords === bucket.records) return "unpriced";
  if (bucket.providerReportedRecords === bucket.records) return "providerReported";
  return "modelPriced";
}
