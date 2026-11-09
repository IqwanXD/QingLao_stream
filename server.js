// Relay voice sederhana: server meneruskan frame audio (binary) ke peserta lain di room.
// Pesan kontrol (join, peers, peer-joined, peer-left) berupa JSON teks.
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_BUFFERED = 64 * 1024; // lewati peer yang tertinggal agar tidak menumpuk
const MAX_PEERS = Number(process.env.MAX_PEERS) || 6; // batas peserta per room, sesuai CPU 0.1 vCPU
const wss = new WebSocketServer({ port: PORT, maxPayload: 64 * 1024 });
const rooms = new Map(); // roomName -> Map<peerId, { ws, name }>

function sendJson(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function broadcastJson(room, exceptId, obj) {
  const peers = rooms.get(room);
  if (!peers) return;
  for (const [id, p] of peers) {
    if (id !== exceptId) sendJson(p.ws, obj);
  }
}

wss.on('connection', (ws) => {
  let me = null; // { id, room, name }

  ws.on('error', () => {});

  ws.on('message', (data, isBinary) => {
    // Frame audio: teruskan dengan prefix id pengirim: [1 byte panjang id][id][payload]
    if (isBinary) {
      if (!me) return;
      const idBuf = Buffer.from(me.id, 'utf8');
      const frame = Buffer.concat([Buffer.from([idBuf.length]), idBuf, data]);
      const peers = rooms.get(me.room);
      if (!peers) return;
      for (const [pid, p] of peers) {
        if (pid !== me.id && p.ws.readyState === p.ws.OPEN && p.ws.bufferedAmount < MAX_BUFFERED) {
          p.ws.send(frame, { binary: true });
        }
      }
      return;
    }

    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type === 'join' && !me) {
      if (typeof msg.peerId !== 'string' || msg.peerId.length === 0 || msg.peerId.length > 100) return;
      const room = String(msg.room || 'default');
      if (!rooms.has(room)) rooms.set(room, new Map());
      const peers = rooms.get(room);
      if (peers.size >= MAX_PEERS) {
        sendJson(ws, { type: 'full' });
        ws.close();
        return;
      }

      me = { id: msg.peerId, room, name: String(msg.name || 'Anon').slice(0, 40) };

      const existing = [...peers.entries()].map(([id, p]) => ({ id, name: p.name }));
      peers.set(me.id, { ws, name: me.name });
      sendJson(ws, { type: 'peers', peers: existing });
      broadcastJson(room, me.id, { type: 'peer-joined', id: me.id, name: me.name });
      console.log(`[${room}] ${me.name} (${me.id}) join. Total: ${peers.size}`);
    }
  });

  ws.on('close', () => {
    if (!me) return;
    const peers = rooms.get(me.room);
    if (!peers) return;
    peers.delete(me.id);
    if (peers.size === 0) rooms.delete(me.room);
    broadcastJson(me.room, me.id, { type: 'peer-left', id: me.id });
    console.log(`[${me.room}] ${me.name} left.`);
  });
});

console.log(`Relay server berjalan di ws://localhost:${PORT}`);
