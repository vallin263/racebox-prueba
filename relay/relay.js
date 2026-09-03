"use strict";
// Relé de telemetría (PRUEBA): el móvil del kart (página racebox-prueba en
// modo "emitir") manda muestras del RaceBox por WebSocket a una SALA, y los
// que abren la misma página con ?ver=SALA las reciben en directo.
//
//   node relay.js [--port 8445] [--data carpeta]
//
// WebSocket: /?room=SALA&role=send   (emisor: manda JSON {type:"samples", items:[...]})
//            /?room=SALA&role=view   (espectador: recibe lo mismo + la última muestra al entrar)
// Cada muestra recibida se guarda además en <data>/<sala>-<fecha>.ndjson.
// Sin cuentas ni contraseñas: la sala es el secreto (solo para pruebas).

const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { WebSocketServer } = require("ws");

const args = process.argv.slice(2);
function argVal(flag, def) { const i = args.indexOf(flag); return i >= 0 && args[i + 1] ? args[i + 1] : def; }
const PORT = Number(argVal("--port", 8445));
const DATA_DIR = argVal("--data", path.join(__dirname, "data"));
fs.mkdirSync(DATA_DIR, { recursive: true });

/** @type {Map<string, {viewers: Set<import("ws")>, senders: Set<import("ws")>, last: any, received: number}>} */
const rooms = new Map();
function room(code) {
  let r = rooms.get(code);
  if (!r) { r = { viewers: new Set(), senders: new Set(), last: null, received: 0 }; rooms.set(code, r); }
  return r;
}
function dayFile(code) {
  return path.join(DATA_DIR, `${code}-${new Date().toISOString().slice(0, 10)}.ndjson`);
}
function roomInfo(code, r) {
  return JSON.stringify({ type: "room", room: code, senders: r.senders.size, viewers: r.viewers.size, received: r.received });
}
function broadcastInfo(code, r) {
  const msg = roomInfo(code, r);
  for (const c of [...r.viewers, ...r.senders]) if (c.readyState === c.OPEN) c.send(msg);
}

const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/health") {
    const out = {};
    for (const [code, r] of rooms) out[code] = { senders: r.senders.size, viewers: r.viewers.size, received: r.received };
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8", "Access-Control-Allow-Origin": "*" });
    res.end(JSON.stringify({ ok: true, rooms: out }));
    return;
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server });
wss.on("connection", (ws, req) => {
  const url = new URL(req.url || "/", "http://relay");
  const code = (url.searchParams.get("room") || "").trim().toLowerCase();
  const role = url.searchParams.get("role");
  if (!/^[a-z0-9_-]{3,32}$/.test(code) || !["send", "view"].includes(role)) { ws.close(4400, "sala o rol no válidos"); return; }
  const r = room(code);
  const set = role === "send" ? r.senders : r.viewers;
  set.add(ws);
  console.log(`[${code}] ${role === "send" ? "emisor" : "espectador"} conectado (${r.senders.size} emisores, ${r.viewers.size} espectadores)`);
  if (role === "view" && r.last) ws.send(JSON.stringify({ type: "samples", items: [r.last], replay: true }));
  broadcastInfo(code, r);
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("message", (data) => {
    if (role !== "send") return;
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }
    if (msg.type !== "samples" || !Array.isArray(msg.items) || !msg.items.length) return;
    const items = msg.items.slice(0, 200);
    r.last = items[items.length - 1];
    r.received += items.length;
    const out = JSON.stringify({ type: "samples", items });
    for (const v of r.viewers) if (v.readyState === v.OPEN) v.send(out);
    fs.appendFile(dayFile(code), items.map((s) => JSON.stringify(s)).join("\n") + "\n", () => {});
    if (r.received % 500 < items.length) broadcastInfo(code, r);
  });
  ws.on("close", () => {
    set.delete(ws);
    console.log(`[${code}] ${role === "send" ? "emisor" : "espectador"} desconectado`);
    broadcastInfo(code, r);
    if (!r.senders.size && !r.viewers.size) rooms.delete(code);
  });
});
// Latido: cierra conexiones muertas (móvil sin cobertura) para que no cuenten como emisores.
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 15000);

server.listen(PORT, "127.0.0.1", () => console.log(`Relé de telemetría en http://127.0.0.1:${PORT} (datos en ${DATA_DIR})`));
