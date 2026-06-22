# vanicall — Frontend Integration Guide

This document is the contract between the **frontend client** and the **vanicall
backend** (Rust/axum signaling server) for building a multi-party WebRTC calling
app on top of the **Cloudflare Realtime SFU**.

Read this top to bottom before writing code. The WebRTC/Cloudflare track flow
(§6) is the part most likely to trip you up.

---

## 1. Architecture in one picture

```
  Browser (you)                vanicall backend            Cloudflare Realtime SFU
  ┌───────────┐   REST + WS    ┌───────────────┐   HTTPS    ┌────────────────────┐
  │ RTCPeer   │◄──────────────►│  signaling +  │◄──────────►│  sessions / tracks  │
  │ Connection│   (signaling)  │  presence     │  (SDP)     │  (media lives here) │
  └─────┬─────┘                └───────────────┘            └─────────┬──────────┘
        │                                                             │
        └──────────────────── MEDIA (audio/video) ───────────────────┘
                         flows DIRECTLY browser ↔ Cloudflare
```

**Key facts that shape everything you build:**

1. **Media never touches the vanicall backend.** It flows directly between your
   browser and Cloudflare's edge. The backend only relays SDP (signaling) and
   tracks presence.
2. **Each client has exactly ONE `RTCPeerConnection` = one Cloudflare session.**
   You publish your local tracks on it and pull every remote track onto the same
   connection. Do **not** create a PeerConnection per remote peer.
3. **The backend owns your session id.** It creates the Cloudflare session for
   you and tells you its id. You never create or send your own session id — you
   only reference *other* peers' session ids when subscribing.
4. **You do NOT report bandwidth.** Billing is reconciled server-side from
   Cloudflare's analytics. Your only billing responsibility (optional) is to
   *display* usage via `GET /usage`.

---

## 2. Environment / base URLs

| | Local dev | Production |
|---|---|---|
| REST | `http://localhost:8080` | `https://<app>.fly.dev` |
| WebSocket | `ws://localhost:8080/ws` | `wss://<app>.fly.dev/ws` |

Put these behind a config/env var. CORS is open on the backend, so a Vite dev
server (`http://localhost:5173`) can call it directly.

---

## 3. REST API

All bodies are JSON. Send `Content-Type: application/json`.

### `POST /login`
Logs in or creates a user by name (no password — see §10 security note).
```jsonc
// request
{ "name": "alice" }
// response 200
{ "token": "<JWT>", "user_id": "<uuid>" }
```
Store the `token` (memory or `sessionStorage`). It is valid **24h**. Use it as a
`Bearer` token for `/usage` and as the **WebSocket subprotocol** for `/ws`.

### `POST /rooms`
Creates a logical room (just an id; no media is provisioned here).
```jsonc
// request
{ "name": "standup" }
// response 200
{ "room_id": "<uuid>" }
```
Rooms are how clients find each other. Share the `room_id` (link, lobby, etc.).

### `GET /usage`  *(auth: `Authorization: Bearer <JWT>`)*
Returns the signed-in user's billed usage (for a billing/account screen).
```jsonc
{ "user_id": "<uuid>", "total_bytes": 123456789, "total_gb": 0.123456789 }
```
Numbers update minutes *after* a call ends (analytics lag) — don't expect live
in-call values here.

### `GET /health`
`{ "status": "ok" }` — liveness only.

---

## 4. Connecting the WebSocket (auth quirk — read carefully)

The JWT is passed as the **WebSocket subprotocol**, not a header (browsers can't
set headers on `WebSocket`). The backend reads it from `Sec-WebSocket-Protocol`:

```js
const ws = new WebSocket(`${WS_BASE}/ws`, [token]); // <-- token as subprotocol
```

- A `401` close means the token is missing/invalid/expired → re-login.
- The server does not echo a selected subprotocol; that's expected and fine.
- All messages are JSON text frames: `ws.send(JSON.stringify(obj))` and
  `JSON.parse(event.data)`.

---

## 5. Message catalog (the full protocol)

Every message has an `action` (server errors instead carry an `error` field).

### You → server
| action | payload | when |
|---|---|---|
| `join` | `{ room_id }` | once, after `session_created` |
| `publish` | `{ sdp, tracks: [{ mid, trackName }] }` | to send your audio/video |
| `subscribe` | `{ tracks: [{ sessionId, trackName }] }` | to pull a peer's track |
| `renegotiate` | `{ sdp }` | to answer a `subscribe_offer` |

### Server → you
| action | payload | meaning |
|---|---|---|
| `session_created` | `{ cf_session_id }` | your Cloudflare session is ready |
| `roster` | `{ peers: [{ cf_session_id, user_name, tracks }] }` | who's already in the room (sent once on join) |
| `peer_joined` | `{ cf_session_id, user_name, tracks }` | someone joined |
| `peer_published` | `{ cf_session_id, tracks }` | a peer (re)published tracks → subscribe to them |
| `peer_left` | `{ cf_session_id }` | a peer disconnected → remove their tiles |
| `publish_answer` | `{ sessionDescription, tracks }` | CF's answer to your publish offer |
| `subscribe_offer` | `{ sessionDescription, requiresImmediateRenegotiation, tracks }` | CF's offer; you must answer via `renegotiate` |
| `renegotiated` | `{}` | renegotiation accepted |
| *(error)* | `{ error: "..." }` | something failed; surface/log it |

`sessionDescription` is always `{ "type": "offer" | "answer", "sdp": "..." }`.

---

## 6. The WebRTC flow (the important part)

Use one `RTCPeerConnection`. Cloudflare wants **`max-bundle`** and its STUN
server. **No TURN is needed.**

```js
const pc = new RTCPeerConnection({
  iceServers: [{ urls: "stun:stun.cloudflare.com:3478" }],
  bundlePolicy: "max-bundle",
});
```

### 6a. Publishing your local tracks
The tricky bit: Cloudflare needs the **`mid`** (the SDP media-line id) for each
track, and `mid` only exists **after** `setLocalDescription`. So the order is:
add transceivers → create+set local offer → read each transceiver's `mid` → send.

```js
const media = await navigator.mediaDevices.getUserMedia({ audio: true, video: true });

// addTransceiver(sendonly) per local track; keep a stable trackName per track.
const sending = media.getTracks().map((track) => ({
  track,
  tx: pc.addTransceiver(track, { direction: "sendonly" }),
  trackName: track.kind === "audio" ? "mic" : "cam",
}));

await pc.setLocalDescription(await pc.createOffer());

const tracks = sending.map(({ tx, trackName }) => ({ mid: tx.mid, trackName }));
ws.send(JSON.stringify({ action: "publish", sdp: pc.localDescription.sdp, tracks }));

// on "publish_answer":
await pc.setRemoteDescription(msg.sessionDescription); // CF answer
// You are now publishing. Other room members will get a `peer_published`.
```

> `trackName` is **your** identifier for the track and must be unique within your
> session (e.g. `"mic"`, `"cam"`, `"screen"`). Peers reference it when they
> subscribe, so keep it stable and predictable.

### 6b. Subscribing to a remote peer's track
When the roster (or a `peer_published`) tells you a peer has a track, pull it.
Adding a remote track triggers renegotiation: CF returns an **offer**, you answer.

```js
function subscribe(peerSessionId, trackName) {
  ws.send(JSON.stringify({
    action: "subscribe",
    tracks: [{ sessionId: peerSessionId, trackName }],
  }));
}

// on "subscribe_offer":
await pc.setRemoteDescription(msg.sessionDescription); // CF offer
await pc.setLocalDescription(await pc.createAnswer());
ws.send(JSON.stringify({ action: "renegotiate", sdp: pc.localDescription.sdp }));
```

### 6c. Receiving media (`ontrack`)
Incoming remote tracks arrive on the single PeerConnection. Correlate them to a
peer using the `mid` from the `subscribe_offer.tracks` array:

```js
const midToPeer = new Map(); // mid -> { sessionId, trackName }
// when handling subscribe_offer, record: msg.tracks.forEach(t => midToPeer.set(t.mid, t))

pc.ontrack = (event) => {
  const info = midToPeer.get(event.transceiver.mid);
  // info.sessionId tells you which peer this media belongs to.
  // Attach event.track (or event.streams[0]) to that peer's <video>/<audio>.
};
```

### 6d. Recommended end-to-end sequence
```
1. getUserMedia()                      // ask for cam/mic early
2. POST /login                         // get JWT (if not already)
3. open WebSocket(/ws, [token])
4. recv session_created                // store your cf_session_id (rarely needed)
5. send { action: "join", room_id }
6. recv roster                         // subscribe to everyone already publishing
7. publish local tracks (§6a)
8. recv peer_published / peer_joined   // subscribe to new peers as they appear
9. recv peer_left                      // tear down that peer's tiles
```

---

## 7. Presence-driven UI

Maintain a `Map<cf_session_id, Participant>` as the source of truth for the grid:

- `roster` → seed the map; subscribe to each peer's existing `tracks`.
- `peer_joined` → add a tile (may have no tracks yet; wait for publish).
- `peer_published` → subscribe to the newly listed `tracks`, attach video on `ontrack`.
- `peer_left` → remove the tile and detach its media.

Your own tile uses the local `media` stream directly (no subscribe needed).

---

## 8. Connection lifecycle & resilience

- **WS close/error:** stop media tiles, show "reconnecting", re-open the socket,
  and **rejoin from scratch** (new `session_created` → `join` → `publish` →
  resubscribe). Sessions are not resumable across socket drops.
- **JWT expiry (24h):** if `/ws` closes with 401 or `/usage` returns 401,
  re-run `POST /login` and reconnect.
- **`pc.onconnectionstatechange`:** on `failed`/`disconnected`, surface a warning;
  ICE restarts are not wired in the backend yet, so a full reconnect is simplest.
- **Cleanup on leave/unmount:** close the WebSocket and call `pc.close()`; stop
  every local `MediaStreamTrack` (`track.stop()`) to release the camera/mic.

---

## 9. What the backend does NOT do yet (so plan around it)

- **No "unsubscribe" / track-close action.** When a peer leaves you just drop
  their tiles; their media stops on its own. (Backend enhancement if needed.)
- **No mute/track-toggle signaling.** Toggling a local track's `enabled` works
  locally; if you need others to *know*, build it on top (e.g. a data message —
  not yet supported) or re-publish.
- **No TURN / no ICE restart.** Fine for most networks (Cloudflare has a public
  IP); locked-down corporate networks may fail to connect.
- **Presence is per backend instance.** Today that's a single machine, so it's
  fine. Don't assume cross-region presence guarantees.

---

## 10. Security notes

- **Auth is name-only right now** (no password). Treat the current `/login` as a
  dev/prototype identity. Don't ship real billing against it without adding real
  auth — flag this to the product owner.
- **Never log the JWT** or put it in URLs/analytics. Keep it in memory or
  `sessionStorage`; clear it on logout.
- You may freely share/handle **other peers' `cf_session_id` + `trackName`** —
  those are not secrets (they're meant to be pulled by others). Your *own*
  session is protected because only the server can act on it.

---

## 11. Suggested implementation shape (React example)

- `api.ts` — `login()`, `createRoom()`, `getUsage()` (fetch wrappers).
- `useSignaling.ts` — owns the WebSocket: connect with `[token]`, expose
  `join(roomId)`, `publish(tracks)`, `subscribe(sessionId, trackName)`,
  `sendRenegotiate(sdp)`, and an event emitter for server messages.
- `useWebRTC.ts` — owns the single `RTCPeerConnection`, the `midToPeer` map, the
  publish/subscribe/renegotiate handshakes, and `ontrack` → participant media.
- `useRoom.ts` — ties presence events to a `Map<sessionId, Participant>` and
  drives subscriptions.
- `CallGrid.tsx` / `ParticipantTile.tsx` — render local + remote `<video>`s.
- `Usage.tsx` — calls `GET /usage` for the account screen.

Keep all signaling in one place; keep all PeerConnection mutation in one place.
The #1 bug source is racing publish and subscribe renegotiations — serialize SDP
operations (a simple promise queue around `setLocalDescription`/
`setRemoteDescription`) to avoid "called in wrong state" errors.

---

## 12. Quick reference: minimal happy-path client

```js
const token = (await (await fetch(`${API}/login`, {
  method: "POST", headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ name }),
})).json()).token;

const pc = new RTCPeerConnection({
  iceServers: [{ urls: "stun:stun.cloudflare.com:3478" }],
  bundlePolicy: "max-bundle",
});
const midToPeer = new Map();
pc.ontrack = (e) => attachMedia(midToPeer.get(e.transceiver.mid), e.track);

const ws = new WebSocket(`${WS}/ws`, [token]);
ws.onmessage = async (ev) => {
  const m = JSON.parse(ev.data);
  switch (m.action) {
    case "session_created":
      ws.send(JSON.stringify({ action: "join", room_id: roomId }));
      break;
    case "roster":
      m.peers.forEach((p) => p.tracks.forEach((t) =>
        ws.send(JSON.stringify({ action: "subscribe",
          tracks: [{ sessionId: p.cf_session_id, trackName: t }] }))));
      await publishLocal();           // see §6a
      break;
    case "peer_published":
      m.tracks.forEach((t) => ws.send(JSON.stringify({ action: "subscribe",
        tracks: [{ sessionId: m.cf_session_id, trackName: t }] })));
      break;
    case "publish_answer":
      await pc.setRemoteDescription(m.sessionDescription);
      break;
    case "subscribe_offer":
      m.tracks.forEach((t) => midToPeer.set(t.mid, t));
      await pc.setRemoteDescription(m.sessionDescription);
      await pc.setLocalDescription(await pc.createAnswer());
      ws.send(JSON.stringify({ action: "renegotiate", sdp: pc.localDescription.sdp }));
      break;
    case "peer_left":
      removePeer(m.cf_session_id);
      break;
    default:
      if (m.error) console.error("signaling error:", m.error);
  }
};
```

> This is the skeleton, not production code — add the SDP operation queue (§11),
> error handling, reconnection (§8), and proper teardown.
