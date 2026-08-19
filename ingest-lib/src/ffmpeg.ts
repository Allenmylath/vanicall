import { spawn } from "node:child_process";

/**
 * Encoder settings for a WHIP publish.
 *
 * The codec choice is not a preference: ffmpeg's WHIP muxer speaks H.264 and Opus only. The rest
 * of the defaults exist to keep latency low and to keep the stream decodable by every browser:
 *
 * - `baseline` profile and `-bf 0` — no B-frames, which is what WebRTC expects and what avoids
 *   reorder delay.
 * - `-g` (keyframe interval) at 2s — a new subscriber can't render anything until the next
 *   keyframe arrives, so this bounds how long a tile stays black after someone joins.
 * - `-tune zerolatency` — disables lookahead buffering.
 * - Opus at 48 kHz, which is the only sample rate WebRTC carries.
 */
export type EncodeOptions = {
  /** Video bitrate, e.g. `"1500k"`. Default `"1500k"`, roughly 720p30. */
  videoBitrate?: string;
  /** Audio bitrate. Default `"64k"`. */
  audioBitrate?: string;
  /** Frames per second. Default `30`. */
  fps?: number;
  /** Seconds between keyframes. Default `2`. */
  keyframeIntervalSec?: number;
  /** Drop audio entirely (video-only feeds such as most RTSP cameras). Default `false`. */
  noAudio?: boolean;
  /** Path to the ffmpeg binary. Default `"ffmpeg"` (found on `PATH`). */
  ffmpegPath?: string;
  /**
   * Extra arguments inserted immediately before the input, e.g.
   * `["-rtsp_transport", "tcp"]`. Use for source-specific flags.
   */
  inputArgs?: string[];
  /**
   * Read the input at its native frame rate. Correct for files and test patterns (which would
   * otherwise be pushed as fast as they decode); wrong for live sources, which already arrive in
   * real time. Defaults to `true` for `pushFile`, `false` for `pushRtsp`.
   */
  realtime?: boolean;
};

/** Build the full ffmpeg argument list for a WHIP publish. Exported for testing and debugging. */
export function buildFfmpegArgs(
  input: string,
  whipUrl: string,
  token: string,
  opts: EncodeOptions = {},
): string[] {
  const fps = opts.fps ?? 30;
  const gop = Math.max(1, Math.round(fps * (opts.keyframeIntervalSec ?? 2)));

  const args: string[] = ["-hide_banner", "-loglevel", "warning"];
  if (opts.realtime) args.push("-re");
  args.push(...(opts.inputArgs ?? []), "-i", input);

  args.push(
    "-c:v", "libx264",
    "-profile:v", "baseline",
    "-preset", "veryfast",
    "-tune", "zerolatency",
    "-bf", "0",
    "-g", String(gop),
    "-r", String(fps),
    "-b:v", opts.videoBitrate ?? "1500k",
    "-pix_fmt", "yuv420p",
  );

  if (opts.noAudio) {
    args.push("-an");
  } else {
    args.push("-c:a", "libopus", "-ar", "48000", "-ac", "2", "-b:a", opts.audioBitrate ?? "64k");
  }

  args.push("-f", "whip", "-authorization", token, whipUrl);
  return args;
}

/** A running ffmpeg publish. */
export type Publish = {
  /** Resolves when ffmpeg exits cleanly; rejects with its stderr tail on a non-zero exit. */
  done: Promise<void>;
  /** Ask ffmpeg to stop. It sends the WHIP `DELETE` on its way out. */
  stop: () => void;
};

/**
 * Spawn ffmpeg to publish `input` to `whipUrl`.
 *
 * stderr is streamed to the caller-supplied `onLog` (default: this process's stderr) and the last
 * few lines are kept, because a WHIP failure — a rejected offer, an ICE timeout — is reported by
 * ffmpeg there and nowhere else.
 */
export function spawnPublish(
  input: string,
  whipUrl: string,
  token: string,
  opts: EncodeOptions & { onLog?: (line: string) => void } = {},
): Publish {
  const args = buildFfmpegArgs(input, whipUrl, token, opts);
  const child = spawn(opts.ffmpegPath ?? "ffmpeg", args, { stdio: ["ignore", "ignore", "pipe"] });

  const tail: string[] = [];
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk: string) => {
    for (const line of chunk.split(/\r?\n/)) {
      if (!line.trim()) continue;
      tail.push(line);
      if (tail.length > 20) tail.shift();
      if (opts.onLog) opts.onLog(line);
      else process.stderr.write(`[ffmpeg] ${line}\n`);
    }
  });

  const done = new Promise<void>((resolve, reject) => {
    child.on("error", (e) =>
      reject(
        new Error(
          `could not start ffmpeg (${e.message}). Install ffmpeg 7.0 or newer — the WHIP muxer ` +
            `is not present in older builds.`,
        ),
      ),
    );
    child.on("close", (code, signal) => {
      // SIGINT/SIGTERM is how `stop()` ends a publish, so it is a clean exit here.
      if (code === 0 || signal === "SIGINT" || signal === "SIGTERM") resolve();
      else reject(new Error(`ffmpeg exited with code ${code}:\n${tail.join("\n")}`));
    });
  });

  return { done, stop: () => child.kill("SIGINT") };
}
