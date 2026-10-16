/**
 * Comment reconciliation — ad copies of a boosted post.
 *
 * Comments left on an ad carry the ad's own media id, so the sweep has to look
 * at those media too or a webhook Meta never delivers is lost for good.
 */

import { beforeEach, describe, expect, it, vi } from "vitest";

const { mockPrisma } = vi.hoisted(() => ({
  mockPrisma: { $queryRaw: vi.fn() },
}));

vi.mock("@/lib/db/client", () => ({ prisma: mockPrisma }));

import { adMediaFor, sweepStartMs } from "../lib/polling/comment-reconciler";

const POST = "18023946917554990";
const AD = "17899788633163100";

describe("adMediaFor", () => {
  beforeEach(() => {
    mockPrisma.$queryRaw.mockReset();
  });

  it("returns the ad media ids seen for the post", async () => {
    mockPrisma.$queryRaw.mockResolvedValue([{ mediaId: AD }]);
    await expect(adMediaFor(POST)).resolves.toEqual([AD]);
  });

  it("never returns the post itself, so it is not swept twice", async () => {
    mockPrisma.$queryRaw.mockResolvedValue([{ mediaId: AD }, { mediaId: POST }]);
    await expect(adMediaFor(POST)).resolves.toEqual([AD]);
  });

  it("drops rows without a media id", async () => {
    mockPrisma.$queryRaw.mockResolvedValue([{ mediaId: null }, { mediaId: AD }]);
    await expect(adMediaFor(POST)).resolves.toEqual([AD]);
  });

  it("returns nothing when the post was never boosted", async () => {
    mockPrisma.$queryRaw.mockResolvedValue([]);
    await expect(adMediaFor(POST)).resolves.toEqual([]);
  });

  it("swallows a query failure, leaving the post itself still swept", async () => {
    mockPrisma.$queryRaw.mockRejectedValue(new Error("connection lost"));
    await expect(adMediaFor(POST)).resolves.toEqual([]);
  });
});

describe("sweepStartMs", () => {
  const lookbackStart = Date.parse("2026-10-13T00:00:00Z");

  it("ignores comments left before the account was reconnected", () => {
    const reconnected = new Date("2026-10-16T01:00:00Z");
    expect(sweepStartMs(lookbackStart, reconnected)).toBe(reconnected.getTime());
  });

  it("keeps the normal lookback when the account was connected earlier", () => {
    const connected = new Date("2026-09-25T00:00:00Z");
    expect(sweepStartMs(lookbackStart, connected)).toBe(lookbackStart);
  });
});
