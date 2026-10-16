/**
 * Rate Limiter
 *
 * Redis-based rate limiter for Instagram private replies.
 *
 * The cap matches Meta's documented limit for this exact call: 750 private
 * replies per hour per Instagram professional account, for comments on posts
 * and reels. Exceeding it risks 429s and app-level restrictions, so the worker
 * requeues rather than pushing through.
 * https://developers.facebook.com/docs/graph-api/overview/rate-limiting/
 *
 * That documented cap is far above what Instagram tolerates before it restricts
 * an account for automated messaging: an account sending ~170 DMs an hour was
 * restricted. The default is therefore 60 an hour, overridable per deployment
 * with DM_RATE_LIMIT_PER_HOUR (never above Meta's 750).
 */

import Redis from "ioredis";

const RATE_LIMIT_MAX = Math.min(
  Number(process.env.DM_RATE_LIMIT_PER_HOUR) || 60,
  750
); // private replies per hour, per account
const RATE_LIMIT_WINDOW = 3600; // 1 hour in seconds
const REQUEUE_DELAY_MS = 30 * 60 * 1000; // 30 minutes
const MAX_REQUEUE_ATTEMPTS = 3;

let redis: Redis | null = null;

function getRedis(): Redis {
  if (!redis) {
    redis = new Redis(process.env.REDIS_URL!, {
      maxRetriesPerRequest: null, // required by BullMQ
    });
  }
  return redis;
}

export interface RateLimitResult {
  allowed: boolean;
  currentCount: number;
  remainingDMs: number;
  shouldRequeue: boolean;
  requeueDelayMs: number;
  shouldSkip: boolean;
  reserved: boolean;
}

const RESERVE_DM_SLOT_SCRIPT = `
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
local max = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

if current >= max then
  return {0, current, 0}
end

local next_count = redis.call("INCR", KEYS[1])
if next_count == 1 then
  redis.call("EXPIRE", KEYS[1], ttl)
end

return {1, next_count, max - next_count}
`;

function toScriptNumber(value: unknown): number {
  if (typeof value === "number") return value;
  if (typeof value === "string") return Number.parseInt(value, 10);
  return 0;
}

function blockedResult(
  count: number,
  requeueAttempt: number
): RateLimitResult {
  if (requeueAttempt >= MAX_REQUEUE_ATTEMPTS) {
    return {
      allowed: false,
      currentCount: count,
      remainingDMs: 0,
      shouldRequeue: false,
      requeueDelayMs: 0,
      shouldSkip: true,
      reserved: false,
    };
  }

  return {
    allowed: false,
    currentCount: count,
    remainingDMs: 0,
    shouldRequeue: true,
    requeueDelayMs: REQUEUE_DELAY_MS,
    shouldSkip: false,
    reserved: false,
  };
}

/**
 * Check if an Instagram account is within its DM rate limit.
 *
 * Uses a Redis counter with a 1-hour TTL per account.
 * Key pattern: `rate:dm:{instagramAccountId}`
 *
 * @param instagramAccountId - The Instagram account ID to check
 * @param requeueAttempt - How many times this job has been requeued (0 = first attempt)
 * @returns Rate limit result with action recommendations
 */
export async function checkRateLimit(
  instagramAccountId: string,
  requeueAttempt: number = 0
): Promise<RateLimitResult> {
  const client = getRedis();
  const key = `rate:dm:${instagramAccountId}`;

  const currentCount = await client.get(key);
  const count = currentCount ? parseInt(currentCount, 10) : 0;

  if (count >= RATE_LIMIT_MAX) {
    // Over the limit
    if (requeueAttempt >= MAX_REQUEUE_ATTEMPTS) {
      // Exceeded max requeue attempts — skip this DM
      return {
        allowed: false,
        currentCount: count,
        remainingDMs: 0,
        shouldRequeue: false,
        requeueDelayMs: 0,
        shouldSkip: true,
        reserved: false,
      };
    }

    return {
      allowed: false,
      currentCount: count,
      remainingDMs: 0,
      shouldRequeue: true,
      requeueDelayMs: REQUEUE_DELAY_MS,
      shouldSkip: false,
      reserved: false,
    };
  }

  return {
    allowed: true,
    currentCount: count,
    remainingDMs: RATE_LIMIT_MAX - count,
    shouldRequeue: false,
    requeueDelayMs: 0,
    shouldSkip: false,
    reserved: false,
  };
}

/**
 * Atomically reserve a DM send slot for an Instagram account.
 * This is the worker-safe path; it prevents concurrent jobs from all passing
 * the rate-limit check before any of them increments the Redis counter.
 */
export async function reserveDMSlot(
  instagramAccountId: string,
  requeueAttempt: number = 0
): Promise<RateLimitResult> {
  const client = getRedis();
  const key = `rate:dm:${instagramAccountId}`;

  const result = await client.eval(
    RESERVE_DM_SLOT_SCRIPT,
    1,
    key,
    RATE_LIMIT_MAX,
    RATE_LIMIT_WINDOW
  );
  const values = Array.isArray(result) ? result : [];
  const allowedFlag = toScriptNumber(values[0]);
  const count = toScriptNumber(values[1]);
  const remaining = toScriptNumber(values[2]);

  if (allowedFlag !== 1) {
    return blockedResult(count, requeueAttempt);
  }

  return {
    allowed: true,
    currentCount: count,
    remainingDMs: remaining,
    shouldRequeue: false,
    requeueDelayMs: 0,
    shouldSkip: false,
    reserved: true,
  };
}

/**
 * Release a DM slot previously taken by reserveDMSlot.
 *
 * reserveDMSlot increments the hourly counter before the send, so concurrent
 * jobs can't all pass the check at once. When that send then fails (closed
 * messaging window, expired token, rejected reply) the reserved slot is never
 * used and must be handed back. Otherwise a comment that never delivers a DM
 * still burns one slot per attempt, and BullMQ's retries burn several. On a
 * post with many failing sends the counter inflates past the real number of
 * DMs and legitimate replies get skipped as rate-limited until the TTL expires.
 *
 * DECR is atomic and preserves the key's TTL, so the hourly window still resets
 * when it originally would. A missing key (the window already rolled over)
 * would decrement to -1 with no expiry, so that case is clamped back to zero.
 */
export async function releaseDMSlot(
  instagramAccountId: string
): Promise<number> {
  const client = getRedis();
  const key = `rate:dm:${instagramAccountId}`;
  const next = await client.decr(key);
  if (next < 0) {
    await client.del(key);
    return 0;
  }
  return next;
}

// Per-person ceiling on messages from one account. A normal flow sends a person
// three or four messages (opening DM, follow prompt, link, follow-up); anything
// past this is a loop, and repeated DMs to one person are what Instagram flags
// as spam. Slots are never handed back: a failed send may still have arrived.
const RECIPIENT_MAX_PER_HOUR = 6;
const RECIPIENT_MAX_PER_DAY = 12;

const RESERVE_RECIPIENT_SCRIPT = `
local hour = tonumber(redis.call("GET", KEYS[1]) or "0")
local day = tonumber(redis.call("GET", KEYS[2]) or "0")
if hour >= tonumber(ARGV[1]) or day >= tonumber(ARGV[2]) then
  return 0
end
if redis.call("INCR", KEYS[1]) == 1 then
  redis.call("EXPIRE", KEYS[1], 3600)
end
if redis.call("INCR", KEYS[2]) == 1 then
  redis.call("EXPIRE", KEYS[2], 86400)
end
return 1
`;

/**
 * Reserve one message to a person, or refuse it when the account already sent
 * them RECIPIENT_MAX_PER_HOUR in the last hour or RECIPIENT_MAX_PER_DAY in the
 * last day. A circuit breaker for retry loops, independent of why they happen.
 */
export async function reserveRecipientSend(
  instagramAccountId: string,
  recipientId: string
): Promise<boolean> {
  const client = getRedis();
  const base = `rate:recipient:${instagramAccountId}:${recipientId}`;
  const result = await client.eval(
    RESERVE_RECIPIENT_SCRIPT,
    2,
    `${base}:hour`,
    `${base}:day`,
    RECIPIENT_MAX_PER_HOUR,
    RECIPIENT_MAX_PER_DAY
  );
  return toScriptNumber(result) === 1;
}

/**
 * Backwards-compatible helper for tests and admin scripts.
 * Prefer reserveDMSlot in workers.
 */
export async function incrementDMCounter(
  instagramAccountId: string
): Promise<number> {
  const result = await reserveDMSlot(instagramAccountId, MAX_REQUEUE_ATTEMPTS);
  return result.currentCount;
}

/**
 * Get the current DM count for an Instagram account.
 */
export async function getCurrentDMCount(
  instagramAccountId: string
): Promise<number> {
  const client = getRedis();
  const key = `rate:dm:${instagramAccountId}`;
  const count = await client.get(key);
  return count ? parseInt(count, 10) : 0;
}

/**
 * Reset the rate limiter for an account (useful for testing).
 */
export async function resetRateLimit(
  instagramAccountId: string
): Promise<void> {
  const client = getRedis();
  const key = `rate:dm:${instagramAccountId}`;
  await client.del(key);
}

// Export constants for use in tests
export {
  RATE_LIMIT_MAX,
  RATE_LIMIT_WINDOW,
  REQUEUE_DELAY_MS,
  MAX_REQUEUE_ATTEMPTS,
  RECIPIENT_MAX_PER_HOUR,
  RECIPIENT_MAX_PER_DAY,
};
