/**
 * Control-plane client for pushing external video into vanicall rooms.
 *
 * The flow is two calls and a URL: create a room, mint an ingest for it, then point any WHIP
 * encoder at the returned `whipUrl`. Media goes encoder -> Cloudflare directly; this library only
 * ever talks to the vanicall API.
 *
 *     const vc = new Vanicall({ baseUrl: "https://vanicall.fly.dev" });
 *     await vc.login("camera-service");
 *
 *     const room = await vc.createRoom({ name: "Front door" });
 *     const ingest = await vc.createIngest(room.roomId, { name: "Front door cam" });
 *
 *     console.log("join at", ingest.joinUrl);
 *     await ingest.pushRtsp("rtsp://192.168.1.9/stream").done;
 *
 * `pushRtsp`/`pushFile` shell out to ffmpeg as a convenience. Nothing requires them — take
 * `ingest.whipUrl` and `ingest.token` and point OBS, GStreamer, or a camera's own WHIP client at
 * them instead.
 */
import { buildFfmpegArgs, spawnPublish, type EncodeOptions, type Publish } from "./ffmpeg.js";

export { buildFfmpegArgs, type EncodeOptions, type Publish } from "./ffmpeg.js";

export type VanicallOptions = {
  /** Origin of the vanicall API, e.g. `https://vanicall.fly.dev`. */
  baseUrl: string;
  /** A vanicall JWT. Omit and call `login()` instead. */
  token?: string;
  /**
   * Where the web app is served, used to build `joinUrl`. Defaults to `baseUrl`, which is right
   * only if the API and the app share an origin — pass it explicitly otherwise.
   */
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

export class Vanicall {
  private readonly baseUrl: string;
  private readonly appUrl: string;
  private token: string | null;

  constructor(opts: VanicallOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, "");
    this.appUrl = (opts.appUrl ?? opts.baseUrl).replace(/\/+$/, "");
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
  ): Promise<Ingest> {
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

    return new Ingest({
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

/** A minted publish credential, plus helpers that drive ffmpeg against it. */
export class Ingest {
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

  /**
   * Publish an RTSP camera. Defaults to TCP transport, which is what actually works across most
   * consumer cameras and NAT, and to no `-re` since the source is already live.
   */
  pushRtsp(url: string, opts: EncodeOptions & { onLog?: (line: string) => void } = {}): Publish {
    return this.push(url, {
      inputArgs: ["-rtsp_transport", "tcp", ...(opts.inputArgs ?? [])],
      realtime: false,
      ...opts,
    });
  }

  /** Publish a local file, paced at its native frame rate so it plays in real time. */
  pushFile(path: string, opts: EncodeOptions & { onLog?: (line: string) => void } = {}): Publish {
    return this.push(path, { realtime: true, ...opts });
  }

  /**
   * Publish any ffmpeg input string — a device (`"video=Webcam"`), a lavfi test pattern, an HLS
   * URL. The escape hatch when `pushRtsp`/`pushFile` don't fit.
   */
  push(input: string, opts: EncodeOptions & { onLog?: (line: string) => void } = {}): Publish {
    return spawnPublish(input, this.whipUrl, this.token, opts);
  }

  /** The equivalent ffmpeg command line, for pasting into a shell or a systemd unit. */
  ffmpegCommand(input: string, opts: EncodeOptions = {}): string {
    const args = buildFfmpegArgs(input, this.whipUrl, this.token, opts);
    return ["ffmpeg", ...args].map((a) => (/[\s"']/.test(a) ? JSON.stringify(a) : a)).join(" ");
  }
}
