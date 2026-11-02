'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const TRUST_PROXY = !!process.env.TRUST_PROXY;
const TURN_MS = 20000, LIVES = 3, MAX_PLAYERS = 8, MAX_ROOMS = 500, MAX_PER_IP = TRUST_PROXY ? 20 : 6;

// Словарь: положи words.txt рядом (одно слово на строку, существительные в именительном падеже)
function norm(s) { return String(s).trim().toLowerCase().replace(/ё/g, 'е'); }
let DICT = null;
try {
  DICT = new Set(fs.readFileSync(path.join(__dirname, 'words.txt'), 'utf8').split(/\r?\n/).map(norm).filter(Boolean));
  console.log('Словарь загружен:', DICT.size);
} catch { console.log('words.txt не найден: проверка по словарю ОТКЛЮЧЕНА, принимается любое слово'); }

const lastLetter = w => { for (let i = w.length - 1; i >= 0; i--) if (!'ьъый'.includes(w[i])) return w[i]; return w.slice(-1); };

const HTML = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/ping') { res.writeHead(200); return res.end('ok'); }
  if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'X-Content-Type-Options': 'nosniff',
      'X-Frame-Options': 'DENY',
      'Referrer-Policy': 'no-referrer',
      'Content-Security-Policy': "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self' ws: wss:; img-src 'self' data:"
    });
    return res.end(HTML);
  }
  res.writeHead(404); res.end();
});

const rooms = new Map(), ipConns = new Map(), ipFails = new Map();
const wss = new WebSocketServer({ server, maxPayload: 512 });

const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const bcast = (r, o) => r.players.forEach(p => send(p.ws, o));
const pub = r => ({
  t: 'state', code: r.code, phase: r.phase, host: r.hostId,
  turn: r.phase === 'play' ? r.players[r.turn].id : null,
  letter: r.letter, left: Math.max(0, r.deadline - Date.now()), total: TURN_MS,
  count: r.used.size, hist: r.hist.slice(-6), winner: r.winner, note: r.note,
  players: r.players.map(p => ({ id: p.id, name: p.name, lives: p.lives, alive: p.alive }))
});
const sync = r => bcast(r, pub(r));

function genCode() { let c; do { c = String(crypto.randomInt(10000, 100000)); } while (rooms.has(c)); return c; }
function cleanName(s) {
  if (typeof s !== 'string') return null;
  s = s.trim().replace(/\s+/g, ' ');
  return /^[\p{L}\p{N}_ -]{2,16}$/u.test(s) ? s : null;
}

function checkWord(r, w) {
  if (!/^[а-я]{2,30}$/.test(w)) return 'Только русские буквы, минимум 2';
  if (r.letter && w[0] !== r.letter) return `Слово должно начинаться на «${r.letter.toUpperCase()}»`;
  if (r.used.has(w)) return 'Это слово уже было';
  if (DICT && !DICT.has(w)) return 'Такого слова нет в словаре';
  return null;
}

function startTurn(r) {
  clearTimeout(r.timer);
  const alive = r.players.filter(p => p.alive);
  if (alive.length < 2) {
    r.phase = 'over'; r.winner = alive[0] ? alive[0].name : null;
    bcast(r, { t: 'ev', k: 'over' }); return sync(r);
  }
  do { r.turn = (r.turn + 1) % r.players.length; } while (!r.players[r.turn].alive);
  r.deadline = Date.now() + TURN_MS;
  const cur = r.players[r.turn];
  r.timer = setTimeout(() => {
    cur.lives--; if (cur.lives <= 0) cur.alive = false;
    bcast(r, { t: 'ev', k: 'timeout', name: cur.name });
    startTurn(r);
  }, TURN_MS);
  sync(r);
}

function leave(ws) {
  const r = ws.ctx.room, p = ws.ctx.player;
  if (!r) return;
  ws.ctx.room = null; ws.ctx.player = null;
  p.ws = null;
  if (r.phase === 'lobby') r.players = r.players.filter(x => x !== p); else p.alive = false;
  const live = r.players.filter(x => x.ws);
  if (!live.length) { clearTimeout(r.timer); rooms.delete(r.code); return; }
  if (r.hostId === p.id) r.hostId = live[0].id;
  bcast(r, { t: 'ev', k: 'left', name: p.name });
  if (r.phase === 'play') {
    const alive = r.players.filter(x => x.alive);
    if (alive.length < 2) {
      clearTimeout(r.timer); r.phase = 'over'; r.winner = alive[0] ? alive[0].name : null; r.note = p.name + ' вышел';
      bcast(r, { t: 'ev', k: 'over' });
    } else if (r.players[r.turn] === p) return startTurn(r);
  }
  sync(r);
}

function handle(ws, m) {
  const c = ws.ctx;
  if (m.t === 'create' || m.t === 'join') {
    if (c.room) return;
    const name = cleanName(m.name);
    if (!name) return send(ws, { t: 'err', msg: 'Ник: 2-16 символов, буквы, цифры, пробел, _ -' });
    let r;
    if (m.t === 'create') {
      if (rooms.size >= MAX_ROOMS) return send(ws, { t: 'err', msg: 'Сервер переполнен, попробуй позже' });
      r = { code: genCode(), players: [], hostId: null, phase: 'lobby', turn: -1, letter: null, used: new Set(), hist: [], deadline: 0, timer: null, winner: null, note: null };
      rooms.set(r.code, r);
    } else {
      const f = ipFails.get(c.ip) || { n: 0, t: Date.now() };
      if (Date.now() - f.t > 60000) { f.n = 0; f.t = Date.now(); }
      if (f.n >= 10) return send(ws, { t: 'err', msg: 'Слишком много попыток, подожди минуту' });
      r = typeof m.code === 'string' && /^\d{5}$/.test(m.code) ? rooms.get(m.code) : null;
      if (!r) { f.n++; ipFails.set(c.ip, f); return send(ws, { t: 'err', msg: 'Комната не найдена' }); }
      if (r.phase !== 'lobby') return send(ws, { t: 'err', msg: 'Игра уже идет' });
      if (r.players.length >= MAX_PLAYERS) return send(ws, { t: 'err', msg: 'Комната заполнена' });
      if (r.players.some(p => p.name.toLowerCase() === name.toLowerCase())) return send(ws, { t: 'err', msg: 'Этот ник уже занят в комнате' });
    }
    const p = { id: crypto.randomBytes(4).toString('hex'), name, ws, lives: LIVES, alive: true };
    r.players.push(p); if (!r.hostId) r.hostId = p.id;
    c.room = r; c.player = p;
    send(ws, { t: 'joined', id: p.id }); sync(r);
    return;
  }
  const r = c.room, p = c.player;
  if (!r) return;
  if (m.t === 'start' || m.t === 'again') {
    if (p.id !== r.hostId) return;
    if (m.t === 'start' && r.phase === 'lobby') {
      if (r.players.length < 2) return send(ws, { t: 'err', msg: 'Нужно минимум 2 игрока' });
      r.players.forEach(x => { x.lives = LIVES; x.alive = true; });
      r.phase = 'play'; r.turn = -1; r.letter = null; r.used.clear(); r.hist = []; r.winner = null; r.note = null;
      bcast(r, { t: 'ev', k: 'start' }); startTurn(r);
    } else if (m.t === 'again' && r.phase === 'over') {
      r.players = r.players.filter(x => x.ws); r.phase = 'lobby'; sync(r);
    }
    return;
  }
  if (m.t === 'word') {
    if (r.phase !== 'play' || r.players[r.turn].id !== p.id || typeof m.w !== 'string' || m.w.length > 40) return;
    const w = norm(m.w), bad = checkWord(r, w);
    if (bad) return send(ws, { t: 'err', msg: bad });
    r.used.add(w); r.letter = lastLetter(w); r.hist.push({ n: p.name, w });
    bcast(r, { t: 'ev', k: 'ok', name: p.name, word: w });
    startTurn(r);
  }
}

wss.on('connection', (ws, req) => {
  const xff = String(req.headers['x-forwarded-for'] || '').split(',').pop().trim();
  const ip = (TRUST_PROXY && xff) || req.socket.remoteAddress;
  const n = (ipConns.get(ip) || 0) + 1;
  if (n > MAX_PER_IP) return ws.close(1008);
  ipConns.set(ip, n);
  ws.alive = true; ws.on('pong', () => { ws.alive = true; });
  ws.ctx = { ip, room: null, player: null, tokens: 10, last: Date.now() };
  ws.on('message', data => {
    const c = ws.ctx, now = Date.now();
    c.tokens = Math.min(10, c.tokens + (now - c.last) / 200); c.last = now; // 5 сообщений/сек, всплеск 10
    if (--c.tokens < 0) return ws.close(1008);
    let m; try { m = JSON.parse(data); } catch { return ws.close(1003); }
    if (!m || typeof m !== 'object' || typeof m.t !== 'string') return;
    try { handle(ws, m); } catch (e) { console.error(e); }
  });
  ws.on('close', () => {
    const k = (ipConns.get(ip) || 1) - 1; if (k <= 0) ipConns.delete(ip); else ipConns.set(ip, k);
    leave(ws);
  });
  ws.on('error', () => {});
});

setInterval(() => {
  wss.clients.forEach(w => { if (!w.alive) return w.terminate(); w.alive = false; w.ping(); });
  const now = Date.now(); ipFails.forEach((f, k) => { if (now - f.t > 60000) ipFails.delete(k); });
}, 15000);

server.listen(PORT, () => console.log('Словобой: http://localhost:' + PORT));
