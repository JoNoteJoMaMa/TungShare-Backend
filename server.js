import http from 'http';
import { WebSocketServer } from 'ws';
import TrackerServer from 'bittorrent-tracker/server';

const PORT = process.env.PORT || 8080;
const MAX_ROOM_CAPACITY = 100;
const MAX_HISTORY = 100; // Max messages stored per room

// Create unified HTTP Server for single-port cloud deployment (Render.com compatibility)
const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('🚀 TungShare P2P Signaling & BitTorrent Tracker Server is Online!');
    return;
  }
  res.writeHead(404);
  res.end();
});

// 1. Initialize BitTorrent WebSocket Tracker
const tracker = new TrackerServer({
  udp: false,
  http: true,
  ws: true,
  stats: false
});

tracker.on('error', (err) => {
  console.error('[Tracker Error]:', err.message);
});

tracker.on('warning', (err) => {
  console.warn('[Tracker Warning]:', err.message);
});

// 2. Initialize WebRTC Signaling Server
const wss = new WebSocketServer({ noServer: true });

// ─── Heartbeat: detect dead connections within 10-20s instead of 30-90s TCP timeout ───
const HEARTBEAT_INTERVAL_MS = 10_000;

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; }); // Browser responds to ping automatically
});

const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) {
      // No pong since last ping — connection is dead, terminate immediately
      ws.terminate();
      return;
    }
    ws.isAlive = false; // reset flag; expect pong before next interval
    try { ws.ping(); } catch (e) {} // send protocol-level ping
  });
}, HEARTBEAT_INTERVAL_MS);

// Clean up heartbeat when server closes
wss.on('close', () => clearInterval(heartbeat));

const rooms = new Map();

function broadcastRoomStatus(roomName) {
  if (!rooms.has(roomName)) return;
  const room = rooms.get(roomName);
  const membersList = [];
  const seenPeers = new Set();

  room.clients.forEach((client) => {
    if (client.authenticated && client.peerId && !seenPeers.has(client.peerId)) {
      seenPeers.add(client.peerId);
      membersList.push({
        peerId: client.peerId,
        animalName: client.animalName,
        animalIcon: client.animalIcon
      });
    }
  });

  const uniquePeersCount = seenPeers.size;
  const hasOtherPeers = uniquePeersCount > 1;

  room.clients.forEach((client) => {
    if (client.readyState === 1 && client.authenticated) {
      client.send(JSON.stringify({
        type: 'room-status',
        peerCount: uniquePeersCount,
        hasOtherPeers: hasOtherPeers,
        members: membersList
      }));
    }
  });
}

// 3. Handle Unified HTTP Upgrade for Signaling (/chat) & Tracker (/announce or default)
server.on('upgrade', (req, socket, head) => {
  const urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (urlObj.pathname.startsWith('/chat')) {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req);
    });
  } else if (tracker.ws) {
    tracker.ws.handleUpgrade(req, socket, head, (ws) => {
      tracker.ws.emit('connection', ws, req);
    });
  } else {
    socket.destroy();
  }
});

wss.on('connection', (ws, req) => {
  const urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const roomName = urlObj.searchParams.get('room');
  const peerId = urlObj.searchParams.get('peerId');
  // animalName and animalIcon are resolved client-side from localStorage (no server identity storage)
  const animalName = urlObj.searchParams.get('animalName') ? decodeURIComponent(urlObj.searchParams.get('animalName')) : 'เพื่อนสมาชิก';
  const animalIcon = urlObj.searchParams.get('animalIcon') ? decodeURIComponent(urlObj.searchParams.get('animalIcon')) : '🐾';
  const providedPassword = urlObj.searchParams.get('password');

  if (!roomName || !peerId) {
    ws.close();
    return;
  }

  ws.peerId = peerId;
  ws.roomName = roomName;
  ws.animalName = animalName;
  ws.animalIcon = animalIcon;
  ws.authenticated = false;

  // Case 1: Room does not exist yet -> Prompt Creator to Set Password
  if (!rooms.has(roomName)) {
    ws.send(JSON.stringify({
      type: 'room-not-found',
      room: roomName
    }));
    
    ws.on('message', function initHandler(message) {
      let data;
      try { data = JSON.parse(message.toString()); } catch (e) { return; }

      if (data.type === 'create-room') {
        const setPassword = (data.password && data.password.trim().length === 4) ? data.password.trim() : null;
        
        const room = {
          clients: new Set(),
          peerIds: new Set(),
          password: setPassword,
          seederMap: new Map(),  // peerId → Set of magnetURIs they are seeding
        };
        rooms.set(roomName, room);

        ws.authenticated = true;
        room.clients.add(ws);
        room.peerIds.add(peerId);

        ws.send(JSON.stringify({
          type: 'room-joined',
          room: roomName,
          hasPassword: !!setPassword
        }));

        console.log(`[Signaling]: Room created [${roomName}] by ${ws.animalIcon} ${ws.animalName}`);
        ws.removeListener('message', initHandler);
        bindActiveRoomListeners(ws, roomName, peerId, false);
      }
    });
    return;
  }

  // Case 2: Room ALREADY exists -> check capacity & verify password
  const room = rooms.get(roomName);

  if (room.peerIds.size >= MAX_ROOM_CAPACITY && !room.peerIds.has(peerId)) {
    ws.send(JSON.stringify({
      type: 'room-full',
      reason: `ห้อง [${roomName}] มีสมาชิกเต็มโควต้า 100 คนแล้ว ไม่สามารถเข้าร่วมได้ในขณะนี้`
    }));
    ws.close();
    return;
  }

  if (room.password) {
    if (providedPassword === room.password) {
      ws.authenticated = true;
      completeRoomJoin(ws, roomName, peerId, room, providedPassword);
    } else {
      ws.send(JSON.stringify({
        type: 'auth-required',
        room: roomName,
        reason: providedPassword ? 'รหัสผ่านไม่ถูกต้อง กรุณาลองใหม่' : 'กรุณากรอกรหัสผ่าน 4 หลัก'
      }));

      ws.on('message', function authHandler(message) {
        let data;
        try { data = JSON.parse(message.toString()); } catch (e) { return; }

        if (data.type === 'auth-submit') {
          if (data.password === room.password) {
            ws.authenticated = true;
      ws.send(JSON.stringify({
        type: 'room-joined',
        room: roomName,
        hasPassword: true
      }));
      ws.removeListener('message', authHandler);
      completeRoomJoin(ws, roomName, peerId, room, data.password);
          } else {
            ws.send(JSON.stringify({
              type: 'auth-failed',
              reason: 'รหัสผ่าน 4 หลักไม่ถูกต้อง กรุณาลองใหม่อีกครั้ง'
            }));
          }
        }
      });
      return;
    }
  } else {
    ws.authenticated = true;
    ws.send(JSON.stringify({
      type: 'room-joined',
      room: roomName,
      hasPassword: false
    }));
    completeRoomJoin(ws, roomName, peerId, room, null);
  }
});

function completeRoomJoin(ws, roomName, peerId, room, password) {
  const isDuplicatePeer = room.peerIds.has(peerId);
  room.clients.add(ws);
  room.peerIds.add(peerId);

  // Save identity if not already saved
  if (ws.userId && !room.identities.has(ws.userId)) {
    room.identities.set(ws.userId, { animalName: ws.animalName, animalIcon: ws.animalIcon });
  }

  console.log(`[Signaling]: Joined room [${roomName}] (${room.peerIds.size}/${MAX_ROOM_CAPACITY}) | Peer: ${ws.animalIcon} ${ws.animalName}`);

  // Notify other clients that a new peer joined
  room.clients.forEach((client) => {
    if (client !== ws && client.readyState === 1 && client.peerId !== ws.peerId && client.authenticated) {
      client.send(JSON.stringify({
        type: 'peer-joined',
        peerId,
        animalName: ws.animalName,
        animalIcon: ws.animalIcon
      }));
    }
  });

  // Option B: Ask the oldest OTHER peer to relay history to the new joiner
  if (!isDuplicatePeer) {
    const existingPeers = Array.from(room.clients).filter(
      c => c !== ws && c.peerId !== peerId && c.readyState === 1 && c.authenticated
    );
    if (existingPeers.length > 0) {
      // Ask the first (oldest) existing peer to send their history to the new joiner
      existingPeers[0].send(JSON.stringify({
        type: 'request-history',
        targetPeerId: peerId
      }));
    }
  }

  broadcastRoomStatus(roomName);
  bindActiveRoomListeners(ws, roomName, peerId, isDuplicatePeer);
}

function bindActiveRoomListeners(ws, roomName, peerId, isDuplicatePeer) {
  ws.on('message', (message) => {
    let data;
    try { data = JSON.parse(message.toString()); } catch (e) { return; }

    if (data && data.type === 'request-init') {
      if (rooms.has(roomName)) {
        const targetRoom = rooms.get(roomName);
        const uniqueDevicesCount = targetRoom.peerIds.size;
        const hasOtherPeers = uniqueDevicesCount > 1;

        ws.send(JSON.stringify({
          type: 'system-init',
          isInitiator: !isDuplicatePeer && hasOtherPeers,
          hasOtherPeers: hasOtherPeers
        }));

        broadcastRoomStatus(roomName);
      }
      return;
    }

    if (!rooms.has(roomName)) return;
    const room = rooms.get(roomName);

    // Handle explicit client leave-room request for instant 0ms disconnect
    if (data && data.type === 'leave-room') {
      handlePeerDisconnect(ws, roomName, peerId);
      try { ws.close(); } catch (e) {}
      return;
    }

    // Track seeder magnetURIs per peerId
    if (data && data.type === 'torrent-meta' && data.magnetURI) {
      if (!room.seederMap.has(peerId)) room.seederMap.set(peerId, new Set());
      room.seederMap.get(peerId).add(data.magnetURI);
      // Ensure senderPeerId is in the outgoing message for client-side tracking
      data.senderPeerId = peerId;
    }

    // Unicast routing: if message has targetPeerId, send only to that peer
    if (data && data.targetPeerId) {
      const targetClient = Array.from(room.clients).find(
        c => c.peerId === data.targetPeerId && c.readyState === 1 && c.authenticated
      );
      if (targetClient) {
        targetClient.send(data.type === 'torrent-meta'
          ? JSON.stringify(data)
          : message.toString()
        );
      }
      return; // Don't broadcast unicast messages
    }

    // Broadcast to all other authenticated peers
    room.clients.forEach((client) => {
      if (client !== ws && client.readyState === 1 && client.peerId !== ws.peerId && client.authenticated) {
        client.send(data.type === 'torrent-meta' ? JSON.stringify(data) : message.toString());
      }
    });
  });

  ws.on('close', () => {
    handlePeerDisconnect(ws, roomName, peerId);
  });
}

function handlePeerDisconnect(ws, roomName, peerId) {
  if (!rooms.has(roomName)) return;
  const targetRoom = rooms.get(roomName);

  // Clean up this socket from clients Set
  targetRoom.clients.delete(ws);

  // Purge any dead or non-open sockets from targetRoom.clients Set immediately
  targetRoom.clients.forEach((c) => {
    if (c.readyState !== 1) targetRoom.clients.delete(c);
  });

  // Check if any OPEN connection remains for this peerId
  const stillHasTab = Array.from(targetRoom.clients).some(c => c.peerId === peerId && c.readyState === 1);
  if (!stillHasTab) {
    targetRoom.peerIds.delete(peerId);

    // Collect magnetURIs this peer was seeding and notify others
    const deadMagnets = targetRoom.seederMap.has(peerId)
      ? Array.from(targetRoom.seederMap.get(peerId))
      : [];
    targetRoom.seederMap.delete(peerId);

    if (targetRoom.clients.size === 0) {
      rooms.delete(roomName);
      console.log(`[Signaling]: Room [${roomName}] deleted (0 active clients)`);
      return;
    } else {
      targetRoom.clients.forEach((client) => {
        if (client.readyState === 1 && client.authenticated) {
          client.send(JSON.stringify({
            type: 'peer-left',
            peerId,
            animalName: ws.animalName,
            animalIcon: ws.animalIcon,
            deadMagnets // List of magnet URIs this peer was seeding (now unavailable)
          }));
        }
      });
    }
  }

  // Always broadcast updated room status immediately to all remaining open sockets
  if (rooms.has(roomName)) {
    broadcastRoomStatus(roomName);
  }
  console.log(`[Signaling]: Disconnected from room [${roomName}] | Peer: ${ws.animalIcon} ${ws.animalName}`);
}

server.listen(PORT, () => {
  console.log(`[TungShare Backend]: Unified server running on port ${PORT}`);
});
