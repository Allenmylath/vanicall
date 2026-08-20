/**
 * Browser-safe control plane for vanicall: rooms and ingest credentials.
 *
 * Deliberately contains NO Node imports, so this file can be bundled into a web app. The ffmpeg
 * helpers that actually push a stream live in `ffmpeg.ts` and are re-exported only from the Node
 * entry point (`index.ts`) — pulling `node:child_process` into a browser bundle breaks the build.
 *
 * A copy of this file is vendored into the web app at `frontend/src/vanicall-sdk/`. Keep the two
 * in sync when changing it.
 */

/** The deployed vanicall API. Used unless the caller passes their own `baseUrl`. */
export const DEFAULT_API_URL = "https://vanicall.fly.dev";

/** The deployed vanicall web app, used to build room links. Override with `appUrl`. */
export const DEFAULT_APP_URL = "https://vanicall.shis.ai";

export type VanicallOptions = {
  /** Origin of the vanicall API. Defaults to `DEFAULT_API_URL`. */
  baseUrl?: string;
  /** A vanicall JWT. Omit and call `login()` instead. */
  token?: string;
  /** Where the web app is served, used to build `joinUrl`. Defaults to `DEFAULT_APP_URL`. */
  appUrl?: string;
};

export type Room = {
  roomId: string;
  name: string;
  /**
   * Whether the room is end-to-end encrypted. Ingest requires `false`: an encoder cannot join the
   * room's MLS group, and browsers drop frames they can't decrypt.
   */
  e2ee: boolean;
};

export type RoomSummary = Room & {
  createdAt: string;
  sessionCount: number;
  lastActive: string | null;
};

export type IngestSummary = {
  ingestId: string;
  name: string;
  trackName: string;
  createdAt: string;
  expiresAt: string;
  /** Whether an encoder is publishing on this ingest right now. */
  publishing: boolean;
};

export class VanicallError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "VanicallError";
  }
}

export class VanicallClient {
  private readonly baseUrl: string;
  private readonly appUrl: string;
  private token: string | null;

  constructor(opts: VanicallOptions = {}) {
    this.baseUrl = (opts.baseUrl ?? DEFAULT_API_URL).replace(/\/+$/, "");
    this.appUrl = (opts.appUrl ?? DEFAULT_APP_URL).replace(/\/+$/, "");
    this.token = opts.token ?? null;
  }

  /**
   * Exchange a display name for a JWT and remember it for subsequent calls.
   *
   * Note that vanicall's `/login` is name-only — it creates the user if the name is unknown and
   * asks for no secret. Anything published under a given name is billed to that name, so pick a
   * dedicated one for a service and don't reuse a person's.
   */
  async login(name: string): Promise<{ token: string; userId: string }> {
    const res = await this.request<{ token: string; user_id: string }>("POST", "/login", { name }, false);
    this.token = res.token;
    return { token: res.token, userId: res.user_id };
  }

  /**
   * Create a room.
   *
   * `e2ee` defaults to `false` here, unlike the API itself — this library exists to get external
   * sources into rooms, and an encrypted room cannot accept one. Pass `e2ee: true` for a room you
   * only want people in.
   */
  async createRoom(opts: { name: string; e2ee?: boolean }): Promise<Room> {
    const e2ee = opts.e2ee ?? false;
    const res = await this.request<{ room_id: string; e2ee: boolean }>("POST", "/rooms", {
      name: opts.name,
      e2ee,
    });
    return { roomId: res.room_id, name: opts.name, e2ee: res.e2ee };
  }

  /** Read a room's public configuration. Needs no auth. */
  async getRoom(roomId: string): Promise<Room> {
    const res = await this.request<{ room_id: string; name: string; e2ee: boolean }>(
      "GET",
      `/rooms/${encodeURIComponent(roomId)}/config`,
      undefined,
      false,
    );
    return { roomId: res.room_id, name: res.name, e2ee: res.e2ee };
  }

  /** List the rooms owned by the authenticated user. */
  async listRooms(): Promise<RoomSummary[]> {
    const res = await this.request<{
      rooms: Array<{
        room_id: string;
        name: string;
        e2ee: boolean;
        created_at: string;
        session_count: number;
        last_active: string | null;
      }>;
    }>("GET", "/rooms");
    return res.rooms.map((r) => ({
      roomId: r.room_id,
      name: r.name,
      e2ee: r.e2ee,
      createdAt: r.created_at,
      sessionCount: r.session_count,
      lastActive: r.last_active,
    }));
  }

  /** Soft-delete a room. Owner only. */
  async deleteRoom(roomId: string): Promise<void> {
    await this.request("DELETE", `/rooms/${encodeURIComponent(roomId)}`);
  }

  /**
   * Mint a publish credential for a room and return a handle that can drive an encoder.
   *
   * Fails with a 409 if the room is end-to-end encrypted — create it with `e2ee: false`.
   * The returned token is shown once and stored only as a digest server-side; keep it if you need
   * to reconnect an encoder later, or mint a new ingest.
   */
  async createIngest(
    roomId: string,
    opts: { name: string; trackName?: "cam" | "screen" } = { name: "External source" },
  ): Promise<IngestHandle> {
    const res = await this.request<{
      ingest_id: string;
      room_id: string;
      name: string;
      track_name: string;
      whip_url: string;
      token: string;
      expires_at: string;
    }>("POST", `/rooms/${encodeURIComponent(roomId)}/ingests`, {
      name: opts.name,
      track_name: opts.trackName ?? "cam",
    });

    return new IngestHandle({
      ingestId: res.ingest_id,
      roomId: res.room_id,
      name: res.name,
      trackName: res.track_name,
      whipUrl: res.whip_url,
      token: res.token,
      expiresAt: res.expires_at,
      joinUrl: `${this.appUrl}/?room=${encodeURIComponent(res.room_id)}`,
    });
  }

  /** List a room's un-revoked ingests, including whether each is publishing right now. */
  async listIngests(roomId: string): Promise<IngestSummary[]> {
    const res = await this.request<{
      ingests: Array<{
        ingest_id: string;
        name: string;
        track_name: string;
        created_at: string;
        expires_at: string;
        publishing: boolean;
      }>;
    }>("GET", `/rooms/${encodeURIComponent(roomId)}/ingests`);
    return res.ingests.map((i) => ({
      ingestId: i.ingest_id,
      name: i.name,
      trackName: i.track_name,
      createdAt: i.created_at,
      expiresAt: i.expires_at,
      publishing: i.publishing,
    }));
  }

  /** Revoke an ingest. Also cuts off any publish it currently has in flight. */
  async revokeIngest(ingestId: string): Promise<void> {
    await this.request("DELETE", `/ingests/${encodeURIComponent(ingestId)}`);
  }

  private async request<T = unknown>(
    method: string,
    path: string,
    body?: unknown,
    auth = true,
  ): Promise<T> {
    if (auth && !this.token) {
      throw new VanicallError("not authenticated — pass `token` or call login() first", 401);
    }
    const headers: Record<string, string> = {};
    if (auth && this.token) headers.Authorization = `Bearer ${this.token}`;
    if (body !== undefined) headers["Content-Type"] = "application/json";

    const res = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });

    const text = await res.text();
    let parsed: any = null;
    try {
      parsed = text ? JSON.parse(text) : null;
    } catch {
      // Non-JSON body; the raw text is the best error message available.
    }
    if (!res.ok) {
      throw new VanicallError(parsed?.error ?? text ?? `${method} ${path} failed`, res.status);
    }
    return parsed as T;
  }
}

/**
 * A minted publish credential. Plain data — everything here is safe to hold in a browser except
 * `token`, which is a publish secret: treat it like a password and don't render it anywhere you
 * wouldn't render one.
 *
 * The Node entry point returns a subclass of this that adds ffmpeg helpers.
 */
export class IngestHandle {
  readonly ingestId: string;
  readonly roomId: string;
  readonly name: string;
  readonly trackName: string;
  /** POST an SDP offer here to publish. This is what you give OBS, GStreamer, or a camera. */
  readonly whipUrl: string;
  /** Bearer token for `whipUrl`. Returned once; the server keeps only its digest. */
  readonly token: string;
  readonly expiresAt: string;
  /** Where a person opens the room in a browser to watch the feed. */
  readonly joinUrl: string;

  constructor(fields: {
    ingestId: string;
    roomId: string;
    name: string;
    trackName: string;
    whipUrl: string;
    token: string;
    expiresAt: string;
    joinUrl: string;
  }) {
    this.ingestId = fields.ingestId;
    this.roomId = fields.roomId;
    this.name = fields.name;
    this.trackName = fields.trackName;
    this.whipUrl = fields.whipUrl;
    this.token = fields.token;
    this.expiresAt = fields.expiresAt;
    this.joinUrl = fields.joinUrl;
  }
}
