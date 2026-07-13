/**
 * Howard Kiosk - WebRTC Signaling Server (cloud version)
 *
 * Hosted on Render. One port for everything:
 *   - wss://  WebSocket signaling (kiosk + staff phones)
 *   - https:// serves reception.html (staff portal)
 *   - /api/*  proxied to the Pi's Flask via Cloudflare Tunnel (FLASK_API_BASE)
 *
 * TLS is terminated by Render — no local certs needed.
 */

const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const http = require('http');
const https = require('https');
const path = require('path');

const PORT = process.env.PORT || 8080;
const HOST = '0.0.0.0';

// Public URL of the Pi's Flask app (Cloudflare Tunnel), e.g. https://howard-api.example.com
const FLASK_API_BASE = process.env.FLASK_API_BASE || '';

const RECEPTION_PATH = path.resolve(__dirname, 'reception.html');

const clients = new Map();
const activeCalls = new Map();

/* ── HTTP: staff portal + API proxy + health ─────────────────── */

function proxyToFlask(req, res) {
  if (!FLASK_API_BASE) {
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'FLASK_API_BASE not configured' }));
    return;
  }

  const target = new URL(req.url, FLASK_API_BASE);
  const lib = target.protocol === 'https:' ? https : http;

  const proxyReq = lib.request(target, {
    method: req.method,
    headers: { ...req.headers, host: target.host }
  }, (proxyRes) => {
    res.writeHead(proxyRes.statusCode || 500, proxyRes.headers);
    proxyRes.pipe(res);
  });

  proxyReq.on('error', (err) => {
    console.error('[API proxy] Flask error:', err.message);
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Flask API not reachable' }));
  });

  req.pipe(proxyReq);
}

function requestHandler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const url = new URL(req.url, 'http://localhost');

  if (url.pathname.startsWith('/api/')) {
    proxyToFlask(req, res);
    return;
  }

  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      clients: clients.size,
      activeCalls: activeCalls.size
    }));
    return;
  }

  if (url.pathname === '/' || url.pathname === '/reception.html') {
    fs.readFile(RECEPTION_PATH, (err, html) => {
      if (err) {
        res.writeHead(404);
        res.end('reception.html not found');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(html);
    });
    return;
  }

  res.writeHead(404);
  res.end();
}

const server = http.createServer(requestHandler);
const wss = new WebSocket.Server({ server });

/* ── WebSocket signaling ─────────────────────────────────────── */

wss.on('connection', (ws) => {
  const clientId = uuidv4();
  ws.clientId = clientId;
  ws.isAlive = true;

  ws.on('pong', () => { ws.isAlive = true; });

  console.log(`[+] Connected: ${clientId}`);
  send(ws, { type: 'connected', payload: { clientId } });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      send(ws, { type: 'error', payload: { message: 'Invalid JSON' } });
      return;
    }
    handleMessage(clientId, ws, msg);
  });

  ws.on('close', () => handleDisconnect(clientId));
  ws.on('error', (e) => console.error(`[!] WS error ${clientId}:`, e.message));
});

// Heartbeat — Render/proxies kill idle sockets; ping every 30s keeps them open
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

function handleMessage(clientId, ws, msg) {
  const { type, payload } = msg;

  switch (type) {
    case 'register': {
      const { role, userId, label } = payload || {};

      if (!role || !userId) {
        send(ws, { type: 'error', payload: { message: 'register requires role + userId' } });
        return;
      }

      // Kick any stale/ghost registration for the same user+role
      // (phones suspend WebSockets when locked; without this, kiosks
      // end up calling dead sockets).
      for (const [oldId, c] of clients.entries()) {
        if (c.role === role && String(c.userId) === String(userId) && oldId !== clientId) {
          console.log(`[R] Replacing stale registration ${oldId} for ${role}/${userId}`);
          if (c.roomId) endCall(c.roomId, 'error');
          try { c.ws.terminate(); } catch {}
          clients.delete(oldId);
        }
      }

      clients.set(clientId, {
        ws,
        role,
        userId,
        label: label || role,
        roomId: null
      });

      console.log(`[R] Registered ${clientId} as ${role} label="${label}"`);

      send(ws, {
        type: 'registered',
        payload: {
          clientId,
          role,
          onlineReceptions: getOnlineReceptions()
        }
      });

      broadcastToKiosks({
        type: 'reception_list',
        payload: { receptions: getOnlineReceptions() }
      });

      break;
    }

    case 'list_receptions': {
      send(ws, {
        type: 'reception_list',
        payload: { receptions: getOnlineReceptions() }
      });
      break;
    }

    case 'call_request': {
      const caller = clients.get(clientId);
      if (!caller) return;

      const { targetClientId } = payload || {};
      const callee = clients.get(targetClientId);

      if (!callee) {
        send(ws, {
          type: 'call_error',
          payload: { message: 'Staff member not available' }
        });
        return;
      }

      const roomId = uuidv4();

      caller.roomId = roomId;
      callee.roomId = roomId;

      activeCalls.set(roomId, {
        callerId: clientId,
        calleeId: targetClientId,
        startedAt: new Date()
      });

      send(callee.ws, {
        type: 'incoming_call',
        payload: {
          roomId,
          callerId: clientId,
          callerLabel: caller.label,
          callerRole: caller.role
        }
      });

      send(ws, {
        type: 'call_initiated',
        payload: { roomId, targetClientId }
      });

      console.log(`[C] Call initiated room=${roomId} ${clientId} -> ${targetClientId}`);
      break;
    }

    case 'call_accept': {
      const { roomId } = payload || {};
      const call = activeCalls.get(roomId);
      if (!call) return;

      const caller = clients.get(call.callerId);
      if (caller) {
        send(caller.ws, { type: 'call_accepted', payload: { roomId } });
      }

      console.log(`[C] Accepted room=${roomId}`);
      break;
    }

    case 'call_reject': {
      const { roomId } = payload || {};
      endCall(roomId, 'rejected');
      break;
    }

    case 'offer':
    case 'answer': {
      routeToRoom(clientId, type, payload);
      break;
    }

    case 'ice_candidate': {
      // Cloud version: pass ALL candidates through.
      // STUN/host/relay filtering is the browsers' job now that
      // kiosk and staff can be on different networks.
      routeToRoom(clientId, 'ice_candidate', payload);
      break;
    }

    case 'hang_up': {
      const { roomId } = payload || {};
      endCall(roomId, 'completed');
      break;
    }

    default:
      send(ws, {
        type: 'error',
        payload: { message: `Unknown type: ${type}` }
      });
  }
}

/* ── Helpers ─────────────────────────────────────────────────── */

function send(ws, msg) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

function routeToRoom(fromId, type, payload) {
  const sender = clients.get(fromId);
  if (!sender || !sender.roomId) return;

  const call = activeCalls.get(sender.roomId);
  if (!call) return;

  const peerId = call.callerId === fromId ? call.calleeId : call.callerId;
  const peer = clients.get(peerId);

  if (peer) {
    send(peer.ws, { type, payload, from: fromId });
  }
}

function getOnlineReceptions() {
  const out = [];
  for (const [id, c] of clients.entries()) {
    if (c.role === 'receptionist') {
      out.push({ clientId: id, userId: c.userId, label: c.label });
    }
  }
  return out;
}

function broadcastToKiosks(msg) {
  for (const [, c] of clients.entries()) {
    if (c.role === 'kiosk') send(c.ws, msg);
  }
}

function endCall(roomId, reason = 'completed') {
  const call = activeCalls.get(roomId);
  if (!call) return;

  for (const peerId of [call.callerId, call.calleeId]) {
    const peer = clients.get(peerId);
    if (peer) {
      peer.roomId = null;
      send(peer.ws, { type: 'call_ended', payload: { roomId, reason } });
    }
  }

  activeCalls.delete(roomId);
  console.log(`[C] Ended room=${roomId} reason=${reason}`);

  broadcastToKiosks({
    type: 'reception_list',
    payload: { receptions: getOnlineReceptions() }
  });
}

function handleDisconnect(clientId) {
  const client = clients.get(clientId);
  if (!client) return;

  console.log(`[-] Disconnected: ${clientId} (${client.role})`);

  if (client.roomId) {
    endCall(client.roomId, 'error');
  }

  clients.delete(clientId);

  broadcastToKiosks({
    type: 'reception_list',
    payload: { receptions: getOnlineReceptions() }
  });
}

/* ── Start ───────────────────────────────────────────────────── */

server.listen(PORT, HOST, () => {
  console.log('\n Howard Kiosk - Signaling Server (cloud)');
  console.log(` Listening on port ${PORT}`);
  console.log(` Flask proxy  : ${FLASK_API_BASE || 'NOT CONFIGURED'}`);
});
