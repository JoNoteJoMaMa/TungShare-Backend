import { Server } from 'bittorrent-tracker';
import { WebSocketServer } from 'ws';

// ตั้งรหัสผ่านลับประจำกลุ่มของคุณเพื่อไม่ให้คนนอกแอบมาเนียนใช้เซิร์ฟเวอร์
const ROOM_KEY = "my_secret_p2p_group_2026";

// 1. เปิดระบบจับคู่ BitTorrent (WebTorrent Tracker)
const tracker = new Server({
  udp: false,
  http: false,
  ws: true
});

const port = process.env.PORT || 8080;

// 2. สร้างโครงข่าย WebSocket หลักของแอป
const wss = new WebSocketServer({ port }, () => {
  console.log(`[เซิร์ฟเวอร์]: ออนไลน์พร้อมใช้งานที่พอร์ต ${port}`);
});

// 3. จัดสรรห้องรับสัญญาณเมื่อคอมพิวเตอร์เพื่อน ๆ วิ่งเข้ามา
wss.on('connection', (ws, req) => {
  
  // 🔒 ตรวจความปลอดภัย: ถ้ารหัสห้องแชตไม่ถูกต้อง ให้ตัดสายทิ้งทันที
  if (!req.url.includes(`key=${ROOM_KEY}`)) {
    console.log('[ระบบความปลอดภัย]: ปฏิเสธการเชื่อมต่อเนื่องจากรหัสผ่านไม่ถูกต้อง');
    ws.close();
    return;
  }

  if (req.url.startsWith('/tracker')) {
    // สายที่ 1: ส่งต่อให้ระบบ WebTorrent จัดการส่งคู่สายบิทอัตโนมัติ
    tracker.onWebSocketConnection(ws, req);
  } 
  else if (req.url.startsWith('/chat')) {
    // สายที่ 2: ทำหน้าที่เป็นสะพานส่งสัญญาณ WebRTC แชตหากัน
    ws.on('message', (message) => {
      // ได้ข้อความจับคู่จากใคร ให้กระจายไปให้เพื่อนทุกคนที่ต่ออยู่ (Broadcast)
      wss.clients.forEach((client) => {
        if (client !== ws && client.readyState === ws.OPEN) {
          client.send(message);
        }
      });
    });
  }
});
