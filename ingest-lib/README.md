# @vanicall/ingest

Push external video — RTSP cameras, ffmpeg, OBS, files, broadcast feeds — into a vanicall room.

The room is the unit of addressing: create one, mint an ingest for it, and point any encoder at
the returned WHIP URL. Everyone who opens the room's join link sees the feed as another tile.

## Install

```
npm install @vanicall/ingest
```

Requires Node 18+. The `push*` helpers additionally need **ffmpeg 7.0 or newer** on `PATH` — the
WHIP muxer does not exist in older builds. You don't need ffmpeg at all if you drive your own
encoder from `whipUrl` / `token`.

## Use

```ts
import { Vanicall } from "@vanicall/ingest";

// URLs default to the live deployment — https://vanicall.fly.dev (API) and
// https://vanicall.shis.ai (app, used for joinUrl). Pass baseUrl/appUrl only to override.
const vc = new Vanicall();
await vc.login("camera-service");

const room = await vc.createRoom({ name: "Front door" });
const ingest = await vc.createIngest(room.roomId, { name: "Front door cam" });

console.log("watch at", ingest.joinUrl);
await ingest.pushRtsp("rtsp://192.168.1.9/stream").done;
```

`pushRtsp` returns a `Publish`: `await pub.done` to run until the stream ends, `pub.stop()` to end
it (which sends the WHIP `DELETE`, so the tile disappears immediately rather than waiting to be
reaped).

Other sources:

```ts
ingest.pushFile("./clip.mp4");                       // paced to real time
ingest.push("testsrc=size=1280x720:rate=30", {       // any ffmpeg input
  inputArgs: ["-f", "lavfi"],
});
```

## Without ffmpeg

`ingest.whipUrl` and `ingest.token` are all any WHIP client needs. For OBS 30+: Settings →
Stream → Service "WHIP", paste the URL and the token as the bearer token.

`ingest.ffmpegCommand(input)` prints the equivalent command line, for a systemd unit or a shell:

```
ffmpeg -hide_banner -loglevel warning -rtsp_transport tcp -i rtsp://... \
  -c:v libx264 -profile:v baseline -preset veryfast -tune zerolatency -bf 0 -g 60 -r 30 \
  -b:v 1500k -pix_fmt yuv420p -c:a libopus -ar 48000 -ac 2 -b:a 64k \
  -f whip -authorization <token> https://vanicall.fly.dev/whip/<ingest-id>
```

H.264 and Opus are not configurable — they are the only codecs ffmpeg's WHIP muxer emits.

## Encryption

**A room that accepts ingest is not end-to-end encrypted.** vanicall normally encrypts media
frames through an MLS group in the browser, and an encoder has no way to join one; browsers drop
frames they cannot decrypt, so a plaintext publisher would render as static in every tile.

So `createRoom` here defaults to `e2ee: false`, and the server refuses to mint an ingest for an
encrypted room. Participants in such a room see a "Not encrypted" badge in the call header and a
warning above the chat box — there is no way to end up in one without being told. Media is still
encrypted in transit (DTLS/SRTP to Cloudflare); it is the *end-to-end* property that is absent.

Pass `e2ee: true` for a room you only want people in.

## Credentials

`login(name)` is name-only — vanicall's auth creates the user if the name is unknown and asks for
no secret. Use a dedicated service name, not a person's: Cloudflare usage for anything published
under it is billed to that name.

Ingest tokens are narrower: one room, one track, and they expire (30 days). Only a digest is
stored server-side, so the plaintext is returned exactly once, at `createIngest`. Rotate by
minting a new ingest and revoking the old one with `revokeIngest`.

## API

| Method | Notes |
| --- | --- |
| `login(name)` | Mints and stores a JWT |
| `new Vanicall({ baseUrl?, appUrl?, token? })` | All optional; URLs default to the live deployment |
| `createRoom({ name, e2ee? })` | `e2ee` defaults to `false` |
| `getRoom(roomId)` | Public; no auth |
| `listRooms()` / `deleteRoom(roomId)` | Owner-scoped |
| `createIngest(roomId, { name, trackName? })` | `trackName` is `"cam"` (default) or `"screen"` |
| `listIngests(roomId)` | Includes `publishing` for each |
| `revokeIngest(ingestId)` | Also cuts off a publish in flight |
| `ingest.pushRtsp / pushFile / push` | Spawn ffmpeg; return `{ done, stop }` |
| `ingest.ffmpegCommand(input)` | The command line, as a string |

## Browser use

`Vanicall` spawns ffmpeg, so it is Node-only. The control plane on its own (rooms, ingest
credentials — everything except publishing) is browser-safe and lives in `src/client.ts`, which has
no Node imports. Import `VanicallClient` from there, or use the copy vendored into the web app at
`frontend/src/vanicall-sdk/`.

## Troubleshooting

**`ret=-11` / "UDP send blocked" / publish dies after a few seconds.** The muxer's send buffer is
too small for the bitrate. The SDK sets `-ts_buffer_size` to 1 MB by default, which covers 1500k;
raise it with `tsBufferSize` if you push higher bitrates or the link is lossy.
