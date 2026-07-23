import { WebSocketServer } from 'ws';
import TrackerServer from 'bittorrent-tracker/server';

const PORT = process.env.PORT || 8080;
const TRACKER_PORT = process.env.TRACKER_PORT || 8000;
const MAX_ROOM_CAPACITY = 100;

// 1. Initialize BitTorrent WebSocket Tracker Server
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

tracker.on('listening', () => {
  console.log(`[BitTorrent Tracker]: Online and listening on port ${TRACKER_PORT}`);
});

tracker.listen(TRACKER_PORT);

// 2. Initialize WebRTC Room Signaling Server with Animal Identity & 100-User Room Limit
const wss = new WebSocketServer({ port: PORT }, () => {
  console.log(`[Signaling Server]: Online on port ${PORT}`);
});

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
    if (client.readyState === 1 && client.authenticated) { // WebSocket.OPEN
      client.send(JSON.stringify({
        type: 'room-status',
        peerCount: uniquePeersCount,
        hasOtherPeers: hasOtherPeers,
        members: membersList
      }));
    }
  });
}

wss.on('connection', (ws, req) => {
  const urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const roomName = urlObj.searchParams.get('room');
  const peerId = urlObj.searchParams.get('peerId');
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
        
        rooms.set(roomName, {
          clients: new Set(),
          peerIds: new Set(),
          password: setPassword
        });

        ws.authenticated = true;
        const room = rooms.get(roomName);
        room.clients.add(ws);
        room.peerIds.add(peerId);

        ws.send(JSON.stringify({
          type: 'room-joined',
          room: roomName,
          hasPassword: !!setPassword
        }));

        console.log(`[Signaling]: Room created [${roomName}] by ${animalIcon} ${animalName}`);
        ws.removeListener('message', initHandler);
        bindActiveRoomListeners(ws, roomName, peerId, false);
      }
    });
    return;
  }

  // Case 2: Room ALREADY exists -> Check Capacity Limit (Max 100 users) & Verify Password if required
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

  console.log(`[Signaling]: Joined room [${roomName}] (${room.peerIds.size}/${MAX_ROOM_CAPACITY}) | Peer: ${ws.animalIcon} ${ws.animalName}`);

  // Notify other clients in the room that a new animal peer joined
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

    if (rooms.has(roomName)) {
      rooms.get(roomName).clients.forEach((client) => {
        if (client !== ws && client.readyState === 1 && client.peerId !== ws.peerId && client.authenticated) {
          client.send(message.toString());
        }
      });
    }
  });

  ws.on('close', () => {
    if (rooms.has(roomName)) {
      const targetRoom = rooms.get(roomName);
      targetRoom.clients.delete(ws);

      const stillHasTab = Array.from(targetRoom.clients).some(c => c.peerId === ws.peerId);
      if (!stillHasTab) {
        targetRoom.peerIds.delete(ws.peerId);
      }

      if (targetRoom.clients.size === 0) {
        rooms.delete(roomName);
        console.log(`[Signaling]: Room [${roomName}] deleted (0 active clients)`);
      } else {
        broadcastRoomStatus(roomName);
        targetRoom.clients.forEach((client) => {
          if (client.readyState === 1 && client.authenticated) {
            client.send(JSON.stringify({
              type: 'peer-left',
              peerId,
              animalName: ws.animalName,
              animalIcon: ws.animalIcon
            }));
          }
        });
      }
    }
    console.log(`[Signaling]: Disconnected from room [${roomName}] | Peer: ${ws.animalIcon} ${ws.animalName}`);
  });
}
