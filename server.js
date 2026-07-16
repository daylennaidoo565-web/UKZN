/** Howard cloud signaling server with authenticated, single-device staff registration. */
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const http = require('http');
const https = require('https');
const path = require('path');

const PORT = process.env.PORT || 8080;
const HOST = '0.0.0.0';
const FLASK_API_BASE = process.env.FLASK_API_BASE || '';
const RECEPTION_PATH = path.resolve(__dirname, 'reception.html');
const clients = new Map();
const activeCalls = new Map();

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
  }, proxyRes => {
    res.writeHead(proxyRes.statusCode || 500, proxyRes.headers);
    proxyRes.pipe(res);
  });
  proxyReq.on('error', error => {
    console.error('[API proxy]', error.message);
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Flask API not reachable' }));
  });
  req.pipe(proxyReq);
}

function requestHandler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname.startsWith('/api/')) return proxyToFlask(req, res);
  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', clients: clients.size, activeCalls: activeCalls.size }));
    return;
  }
  if (url.pathname === '/' || url.pathname === '/reception.html') {
    fs.readFile(RECEPTION_PATH, (error, html) => {
      if (error) { res.writeHead(404); res.end('reception.html not found'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(html);
    });
    return;
  }
  res.writeHead(404); res.end();
}

function validateStaffSession(token, userId) {
  return new Promise(resolve => {
    if (!FLASK_API_BASE || !token) return resolve(null);
    const target = new URL('/api/auth/session', FLASK_API_BASE);
    const lib = target.protocol === 'https:' ? https : http;
    const req = lib.request(target, {
      method: 'GET',
      headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
      timeout: 12000
    }, res => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { body += chunk; });
      res.on('end', () => {
        try {
          const data = JSON.parse(body);
          if (res.statusCode === 200 && data.success && String(data.user.USER_ID) === String(userId)) resolve(data.user);
          else resolve(null);
        } catch (_) { resolve(null); }
      });
    });
    req.on('timeout', () => req.destroy());
    req.on('error', () => resolve(null));
    req.end();
  });
}

const server = http.createServer(requestHandler);
const wss = new WebSocket.Server({ server });

wss.on('connection', ws => {
  const clientId = uuidv4();
  ws.clientId = clientId;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  send(ws, { type: 'connected', payload: { clientId } });
  ws.on('message', async raw => {
    let message;
    try { message = JSON.parse(raw); }
    catch (_) { send(ws, { type: 'error', payload: { message: 'Invalid JSON' } }); return; }
    try { await handleMessage(clientId, ws, message); }
    catch (error) { console.error('[WS message]', error); }
  });
  ws.on('close', () => handleDisconnect(clientId));
  ws.on('error', error => console.error(`[WS ${clientId}]`, error.message));
});

setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

async function handleMessage(clientId, ws, message) {
  const { type, payload = {} } = message;
  if (type === 'register') {
    const { role, userId, label, token } = payload;
    if (!role || !userId) return send(ws, { type: 'error', payload: { message: 'register requires role and userId' } });
    if (role !== 'kiosk') {
      const user = await validateStaffSession(token, userId);
      if (!user || String(user.ROLE || '').toLowerCase() === 'admin') {
        send(ws, { type: 'registration_denied', payload: { message: 'Your staff session is invalid or has expired.' } });
        return;
      }
    }
    for (const [oldId, client] of clients.entries()) {
      if (client.role === role && String(client.userId) === String(userId) && oldId !== clientId) {
        send(client.ws, { type: 'session_replaced', payload: { message: 'This connection was replaced by a newer connection for the same account.' } });
        if (client.roomId) endCall(client.roomId, 'replaced');
        clients.delete(oldId);
        setTimeout(() => { try { client.ws.terminate(); } catch (_) {} }, 150);
      }
    }
    clients.set(clientId, { ws, role, userId, label: label || role, roomId: null });
    send(ws, { type: 'registered', payload: { clientId, role, onlineReceptions: getOnlineReceptions() } });
    broadcastToKiosks({ type: 'reception_list', payload: { receptions: getOnlineReceptions() } });
    return;
  }
  if (type === 'unregister') {
    const client = clients.get(clientId);
    if (client && client.roomId) endCall(client.roomId, 'logout');
    clients.delete(clientId);
    broadcastToKiosks({ type: 'reception_list', payload: { receptions: getOnlineReceptions() } });
    return;
  }
  if (type === 'list_receptions') return send(ws, { type: 'reception_list', payload: { receptions: getOnlineReceptions() } });
  if (type === 'call_request') {
    const caller = clients.get(clientId);
    const callee = clients.get(payload.targetClientId);
    if (!caller || !callee) return send(ws, { type: 'call_error', payload: { message: 'Staff member not available' } });
    const roomId = uuidv4();
    caller.roomId = roomId; callee.roomId = roomId;
    activeCalls.set(roomId, { callerId: clientId, calleeId: payload.targetClientId, startedAt: new Date() });
    send(callee.ws, { type: 'incoming_call', payload: { roomId, callerId: clientId, callerLabel: caller.label, callerRole: caller.role } });
    send(ws, { type: 'call_initiated', payload: { roomId, targetClientId: payload.targetClientId } });
    return;
  }
  if (type === 'call_accept') {
    const call = activeCalls.get(payload.roomId);
    if (call) { const caller = clients.get(call.callerId); if (caller) send(caller.ws, { type: 'call_accepted', payload: { roomId: payload.roomId } }); }
    return;
  }
  if (type === 'call_reject') return endCall(payload.roomId, 'rejected');
  if (type === 'offer' || type === 'answer' || type === 'ice_candidate') return routeToRoom(clientId, type, payload);
  if (type === 'hang_up') return endCall(payload.roomId, 'completed');
  send(ws, { type: 'error', payload: { message: `Unknown type: ${type}` } });
}

function send(ws, message) { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message)); }

function routeToRoom(fromId, type, payload) {
  const sender = clients.get(fromId);
  if (!sender || !sender.roomId) return;
  const call = activeCalls.get(sender.roomId);
  if (!call) return;
  const peerId = call.callerId === fromId ? call.calleeId : call.callerId;
  const peer = clients.get(peerId);
  if (peer) send(peer.ws, { type, payload, from: fromId });
}

function getOnlineReceptions() {
  const unique = new Map();
  for (const [clientId, client] of clients.entries()) {
    if (client.role === 'receptionist') unique.set(String(client.userId), { clientId, userId: client.userId, label: client.label });
  }
  return [...unique.values()];
}

function broadcastToKiosks(message) {
  for (const client of clients.values()) if (client.role === 'kiosk') send(client.ws, message);
}

function endCall(roomId, reason = 'completed') {
  const call = activeCalls.get(roomId);
  if (!call) return;
  for (const peerId of [call.callerId, call.calleeId]) {
    const peer = clients.get(peerId);
    if (peer) { peer.roomId = null; send(peer.ws, { type: 'call_ended', payload: { roomId, reason } }); }
  }
  activeCalls.delete(roomId);
  broadcastToKiosks({ type: 'reception_list', payload: { receptions: getOnlineReceptions() } });
}

function handleDisconnect(clientId) {
  const client = clients.get(clientId);
  if (!client) return;
  if (client.roomId) endCall(client.roomId, 'error');
  clients.delete(clientId);
  broadcastToKiosks({ type: 'reception_list', payload: { receptions: getOnlineReceptions() } });
}

server.listen(PORT, HOST, () => {
  console.log(`Howard signaling server listening on ${PORT}`);
  console.log(`Flask proxy: ${FLASK_API_BASE || 'NOT CONFIGURED'}`);
});
