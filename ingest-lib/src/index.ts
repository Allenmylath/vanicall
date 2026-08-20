/**
 * Node entry point for the vanicall ingest SDK.
 *
 * Everything in `client.ts` (rooms, ingest credentials) plus the ffmpeg helpers that actually push
 * a stream. Import from here in Node; import `./client.js` in a browser, which has no Node
 * dependencies. The web app vendors that file at `frontend/src/vanicall-sdk/`.
 *
 * URLs default to the live deployment, so the common case needs no configuration:
 *
 *     const vc = new Vanicall();
 *     await vc.login("camera-service");
 *
 *     const room   = await vc.createRoom({ name: "Front door" });
 *     const ingest = await vc.createIngest(room.roomId, { name: "Front door cam" });
 *
 *     console.log(ingest.joinUrl);          // https://vanicall.shis.ai/?room=<id>
 *     await ingest.pushRtsp("rtsp://192.168.1.9/stream").done;
 *
 * Point it elsewhere by passing `baseUrl` / `appUrl`.
 *
 * `pushRtsp`/`pushFile` shell out to ffmpeg as a convenience. Nothing requires them — take
 * `ingest.whipUrl` and `ingest.token` and point OBS, GStreamer, or a camera's own WHIP client at
 * them instead.
 */
import { VanicallClient, IngestHandle } from "./client.js";
import { buildFfmpegArgs, spawnPublish, type EncodeOptions, type Publish } from "./ffmpeg.js";

export {
  VanicallClient,
  IngestHandle,
  VanicallError,
  DEFAULT_API_URL,
  DEFAULT_APP_URL,
  type VanicallOptions,
  type Room,
  type RoomSummary,
  type IngestSummary,
} from "./client.js";
export { buildFfmpegArgs, type EncodeOptions, type Publish } from "./ffmpeg.js";

/** A minted publish credential, plus helpers that drive ffmpeg against it. */
export class Ingest extends IngestHandle {
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
   *
   * Takes a SINGLE input. For a synthetic source that needs both audio and video, use one lavfi
   * graph rather than two `-i` flags, e.g.
   * `"testsrc2=size=1280x720:rate=30[out0];sine=frequency=440[out1]"` with
   * `inputArgs: ["-f", "lavfi"]`.
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

/**
 * The control plane, with ffmpeg-capable ingests. Same API as `VanicallClient`, except
 * `createIngest` hands back an `Ingest` that can drive an encoder.
 */
export class Vanicall extends VanicallClient {
  override async createIngest(
    roomId: string,
    opts: { name: string; trackName?: "cam" | "screen" } = { name: "External source" },
  ): Promise<Ingest> {
    return new Ingest(await super.createIngest(roomId, opts));
  }
}
