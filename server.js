'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const TRUST_PROXY = !!process.env.TRUST_PROXY;
const TURN_MS = 20000, LIVES = 3, MAX_PLAYERS = 8, MAX_ROOMS = 500, MAX_PER_IP = TRUST_PROXY ? 20 : 6;

// Словарь: положи words.txt рядом (одно слово на строку, существительные в именительном падеже)
function norm(s) { return String(s).trim().toLowerCase().replace(/ё/g, 'е'); }
const VULGAR = /^(пизд|хуй|хуя|хуе|блят|бляд|залуп|муда[кч]|мудил|ебал|ебан|ебл|дроч|пидор|пидар|говн|шлюх)/;
let DICT = null;
try {
  DICT = new Set(fs.readFileSync(path.join(__dirname, 'words.txt'), 'utf8').split(/\r?\n/).map(norm).filter(w => w && !VULGAR.test(w)));
  console.log('Словарь загружен:', DICT.size);
} catch { console.log('words.txt не найден: проверка по словарю ОТКЛЮЧЕНА, принимается любое слово'); }

const lastLetter = w => { for (let i = w.length - 1; i >= 0; i--) if (!'ьъый'.includes(w[i])) return w[i]; return w.slice(-1); };

// ---------- профили, редкость слов, хранилище ----------
const UP_URL = process.env.UPSTASH_URL, UP_TOKEN = process.env.UPSTASH_TOKEN, GRACE_MS = 90000;
console.log(UP_URL ? 'Статистика: Upstash Redis' : 'Статистика только в памяти (задай UPSTASH_URL и UPSTASH_TOKEN)');
async function up(cmd) {
  if (!UP_URL) return null;
  try {
    const r = await fetch(UP_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + UP_TOKEN }, body: JSON.stringify(cmd) });
    return (await r.json()).result;
  } catch { return null; }
}
// ---------- аватарки: S3 (Filebase и др.) если заданы переменные S3_*, иначе Upstash, иначе память ----------
const S3 = require('./s3');
const S3C = process.env.S3_BUCKET && process.env.S3_KEY_ID && process.env.S3_SECRET
  ? S3.client({ endpoint: process.env.S3_ENDPOINT || 'https://s3.filebase.com', region: process.env.S3_REGION || 'us-east-1', bucket: process.env.S3_BUCKET, keyId: process.env.S3_KEY_ID, secret: process.env.S3_SECRET, vhost: !!process.env.S3_VHOST })
  : null;
const memAv = new Map(), avCache = new Map();
const imgType = b => b.length > 12 ? (b[0] === 0xff && b[1] === 0xd8 && b[2] === 0xff ? 'image/jpeg' : b.subarray(0, 4).toString('latin1') === '\x89PNG' ? 'image/png' : b.subarray(0, 4).toString('latin1') === 'RIFF' && b.subarray(8, 12).toString('latin1') === 'WEBP' ? 'image/webp' : null) : null;
async function avPut(id, buf) { avCache.delete(id); if (S3C) return S3C.put('a/' + id, buf, 'application/octet-stream'); if (UP_URL) return (await up(['SET', 'av:' + id, buf.toString('base64')])) === 'OK'; memAv.set(id, buf); return true; }
async function avGet(id) { if (S3C) return S3C.get('a/' + id); if (UP_URL) { const b = await up(['GET', 'av:' + id]); return b ? Buffer.from(b, 'base64') : null; } return memAv.get(id) || null; }
async function avDel(id) { try { if (S3C) return await S3C.del('a/' + id); if (UP_URL) return await up(['DEL', 'av:' + id]); memAv.delete(id); } catch {} }
const avUrl = f => f && f.av ? '/a/' + f.av + '?v=' + (f.avv || 1) : '';
console.log('Аватарки хранятся:', S3C ? 'в S3 (' + (process.env.S3_ENDPOINT || 'https://s3.filebase.com') + ')' : UP_URL ? 'в Upstash Redis' : 'только в памяти');
if (S3C) (async () => {
  try { await S3C.put('healthcheck', Buffer.from('ok'), 'text/plain'); const b = await S3C.get('healthcheck'); console.log('Проверка хранилища S3:', b && b.toString() === 'ok' ? 'OK' : 'ответ неожиданный'); }
  catch (e) { console.log('Проверка хранилища S3: ОШИБКА', e.message); }
})();
const online = new Map(); // ключ профиля -> набор сокетов
const tellKey = (key, msg) => (online.get(key) || []).forEach(w => send(w, msg));
function presenceOff(ws) { const k = ws.ctx.key; if (k && online.has(k)) { online.get(k).delete(ws); if (!online.get(k).size) online.delete(k); } }
const avRate = new Map(), friendRate = new Map(), inviteRate = new Map();

const profiles = new Map(), dirty = new Set();
// ---------- защита от спама и чужих сайтов ----------
const EXTRA_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(x => x.trim()).filter(Boolean);
function originOk(org, host) { try { return EXTRA_ORIGINS.includes(org) || new URL(org).host === host; } catch { return false; } }
const connRate = new Map(), authRate = new Map(), createRate = new Map(), nickRate = new Map();
function hit(map, k, max, ms) {
  const now = Date.now(); let e = map.get(k);
  if (!e || now - e.t > ms) { e = { n: 0, t: now }; map.set(k, e); }
  return ++e.n <= max;
}

// ---------- уникальные ники ----------
const nicks = new Map(); // ник в нижнем регистре -> ключ профиля
async function reserveNick(lc, key) {
  if (nicks.has(lc) && nicks.get(lc) !== key) return 'taken';
  if (UP_URL) {
    const r = await up(['SET', 'nk:' + lc, key, 'NX']);
    if (r !== 'OK') { if (r === null) { const o = await up(['GET', 'nk:' + lc]); if (o !== key) return o === undefined ? 'err' : 'taken'; } else return 'err'; }
  }
  nicks.set(lc, key); return 'ok';
}

// ---------- вход через Google ----------
const { OAuth2Client } = require('google-auth-library');
const GID = process.env.GOOGLE_CLIENT_ID || '';
const oc = GID ? new OAuth2Client(GID) : null;
if (!GID) console.log('ВНИМАНИЕ: GOOGLE_CLIENT_ID не задан, вход невозможен');
const sh = x => crypto.createHash('sha256').update(x).digest('hex');
const gkey = sub => sh('g:' + sub).slice(0, 32);
const sessions = new Map();
async function saveSession(sess, key) { sessions.set(sh(sess), key); await up(['SET', 's:' + sh(sess), key, 'EX', 2592000]); }
async function loadSession(sess) {
  const h = sh(sess); let k = sessions.get(h);
  if (!k) { k = await up(['GET', 's:' + h]); if (k) sessions.set(h, k); }
  return k || null;
}
// Сердцебиение для Upstash: раз в 6 часов пишем ключ, чтобы база не считалась неактивной
const beat = () => up(['SET', 'alive', String(Date.now())]);
setInterval(beat, 6 * 3600 * 1000); beat();
async function getProfile(key) {
  let p = profiles.get(key);
  if (p) return p;
  const raw = await up(['GET', 'p:' + key]);
  try { p = raw ? JSON.parse(raw) : null; } catch { p = null; }
  p = p || { name: '', games: 0, wins: 0, total: 0, t: [0, 0, 0, 0], m: [], w: {}, best: '' };
  p.nick = p.nick || ''; p.bo = p.bo || { f: 0, s: 0 };
  p.fr = p.fr || []; p.rq = p.rq || []; p.ac = p.ac || []; p.qd = p.qd || 0; p.rg = p.rg || 0; p.elo = p.elo ?? 800; p.av = p.av || ''; p.avv = p.avv || 0;
  if (p.nick) nicks.set(p.nick.toLowerCase(), key);
  while (p.t.length < 4) p.t.push(0);
  p.m = p.m || [];
  return profiles.get(key) || (profiles.set(key, p), p);
}
async function flush() {
  const keys = [...dirty]; dirty.clear();
  for (const k of keys) await up(['SET', 'p:' + k, JSON.stringify(profiles.get(k))]);
}
setInterval(flush, 20000);
process.on('SIGTERM', async () => { await flush(); process.exit(0); });
process.on('uncaughtException', e => console.error('uncaught', e));
process.on('unhandledRejection', e => console.error('unhandled', e));

const loadLines = f => {
  try {
    return fs.readFileSync(path.join(__dirname, 'data', f), 'utf8').split(/\r?\n/).map(w => norm(w).replace(/\s*\(.*?\)\s*/g, '')).filter(w => w.length >= 2 && /^[а-я]+([ -][а-я]+)*$/.test(w) && !VULGAR.test(w));
  } catch { return []; }
};
const MODE_DEF = [['words', 'Слова'], ['cities', 'Города'], ['names', 'Имена'], ['countries', 'Страны'], ['animals', 'Животные'], ['food', 'Еда']];
const SETS = { words: DICT };
['cities', 'names', 'countries', 'animals', 'food'].forEach(k => { const a = loadLines(k + '.txt'); SETS[k] = a.length ? new Set(a) : null; });
const ALL = new Set(); Object.values(SETS).forEach(x => x && x.forEach(w => ALL.add(w)));
const MODES_AVAIL = MODE_DEF.filter(([k]) => SETS[k]).map(([k]) => k);
if (MODES_AVAIL.length > 1) MODES_AVAIL.push('all');
const MODE_NAME = Object.fromEntries(MODE_DEF.concat([['all', 'Все вместе']]));
console.log('Режимы:', MODES_AVAIL.map(k => k + (SETS[k] ? ':' + SETS[k].size : '')).join(', '));
const { buildTiers } = require('./tiers');
const TIERS = DICT ? buildTiers([...DICT]) : null;
const tier = w => (TIERS && TIERS.map.get(w)) || 0;
if (TIERS) {
  const cnt = [0, 0, 0, 0, 0]; TIERS.map.forEach(t => cnt[t]++);
  console.log('Редкость: обычных', cnt[0], '| редких', cnt[1], '| эпических', cnt[2], '| легендарных', cnt[3], '| мифических', cnt[4], '| список частых слов:', TIERS.common ? TIERS.common : 'не загружен');
}
function addWord(p, w, t) {
  const f = p.prof, fresh = !f.w[w]; f.total++; f.w[w] = (f.w[w] || 0) + 1;
  let gain = '';
  if (t && fresh) { // редкости и бусты считаются только за новые для профиля слова
    f.t[t - 1]++; if (t === 4 && f.m.length < 12) f.m.push(w);
    if ((t === 2 || t === 4) && f.bo.f < 5) { f.bo.f++; gain += 'f'; }
    if (t >= 3 && f.bo.s < 5) { f.bo.s++; gain += 's'; }
  }
  if (w.length > f.best.length) f.best = w;
  const ks = Object.keys(f.w); let n = ks.length;
  if (n > 300) for (const k of ks) { if (n <= 250) break; if (f.w[k] === 1) { delete f.w[k]; n--; } }
  dirty.add(p.key);
  return { gain, fresh };
}
// ---------- уровни, лиги, достижения ----------
const LEAGUES = [[0, 'Бронза'], [900, 'Серебро'], [1100, 'Золото'], [1300, 'Платина'], [1500, 'Алмаз'], [1700, 'Легенда']];
const leagueOf = e => { let l = LEAGUES[0][1]; for (const x of LEAGUES) if (e >= x[0]) l = x[1]; return l; };
const xpOf = f => f.total + f.games * 5 + f.wins * 10 + f.qd * 20;
const levelOf = f => 1 + Math.floor(Math.sqrt(xpOf(f) / 25));
const ACH = [
  ['g1', 'Первая игра', f => f.games >= 1], ['g10', '10 игр', f => f.games >= 10], ['w1', 'Первая победа', f => f.wins >= 1],
  ['w10', '10 побед', f => f.wins >= 10], ['w50', '50 побед', f => f.wins >= 50], ['t100', '100 слов', f => f.total >= 100],
  ['t1000', '1000 слов', f => f.total >= 1000], ['r1', 'Редкое слово', f => f.t[0] >= 1], ['e1', 'Эпическое слово', f => f.t[1] >= 1],
  ['l1', 'Легендарное слово', f => f.t[2] >= 1], ['m1', 'Мифическое слово', f => f.t[3] >= 1], ['r50', '50 редких слов', f => f.t[0] >= 50],
  ['e20', '20 эпических слов', f => f.t[1] >= 20], ['q1', 'Первое задание дня', f => f.qd >= 1], ['q20', '20 заданий дня', f => f.qd >= 20],
  ['rt1', 'Первая рейтинговая игра', f => f.rg >= 1], ['pl', 'Платиновая лига', f => f.elo >= 1300], ['fr1', 'Первый друг', f => f.fr.length >= 1],
  ['av', 'Своя аватарка', f => !!f.av]
];
function checkAch(key) {
  const f = profiles.get(key); if (!f) return;
  const nw = [];
  for (const [id, name, fn] of ACH) if (!f.ac.includes(id) && fn(f)) { f.ac.push(id); nw.push(name); }
  if (nw.length) { dirty.add(key); nw.forEach(n => tellKey(key, { t: 'ach', name: n })); }
}

// ---------- рейтинг (Elo), очередь, таблица лидеров ----------
const lbSet = (key, elo) => { if (UP_URL) up(['ZADD', 'lb', String(elo), key]); };
async function lbTop() {
  let keys = [];
  if (UP_URL) { const r = await up(['ZREVRANGE', 'lb', '0', '9']); keys = Array.isArray(r) ? r : []; }
  else keys = [...profiles.entries()].filter(([, f]) => f.rg > 0).sort((a, b) => b[1].elo - a[1].elo).slice(0, 10).map(x => x[0]);
  const out = [];
  for (const k of keys) { const f = await getProfile(k); out.push({ nick: f.nick, av: avUrl(f), elo: f.elo, lg: leagueOf(f.elo) }); }
  return out;
}
function rate(r, win) {
  const ps = r.players; if (ps.length !== 2 || !win) return;
  const lose = ps.find(x => x !== win), A = win.prof, B = lose.prof;
  const E = 1 / (1 + Math.pow(10, (B.elo - A.elo) / 400)), d = Math.max(1, Math.round(32 * (1 - E)));
  A.elo += d; B.elo = Math.max(0, B.elo - d); A.rg++; B.rg++;
  dirty.add(win.key); dirty.add(lose.key); lbSet(win.key, A.elo); lbSet(lose.key, B.elo);
  tellKey(win.key, { t: 'rating', elo: A.elo, d, lg: leagueOf(A.elo) }); tellKey(lose.key, { t: 'rating', elo: B.elo, d: -d, lg: leagueOf(B.elo) });
  checkAch(win.key); checkAch(lose.key);
}
const queue = [];
const qRemove = ws => { const i = queue.findIndex(q => q.ws === ws); if (i >= 0) queue.splice(i, 1); };
function makeRoom(mode, rated) {
  const r = { code: genCode(), players: [], hostId: null, phase: 'lobby', turn: -1, letter: null, used: new Set(), hist: [], deadline: 0, timer: null, winner: null, note: null, last: Date.now(), mode, frz: false, rated };
  rooms.set(r.code, r); return r;
}
function addPlayer(ws, r) {
  const c = ws.ctx, prof = profiles.get(c.key);
  const p = { id: crypto.randomBytes(4).toString('hex'), name: prof.nick, ws, lives: LIVES, alive: true, key: c.key, prof, kick: null, gone: false };
  r.players.push(p); if (!r.hostId) r.hostId = p.id; c.room = r; c.player = p;
  send(ws, { t: 'joined', id: p.id, inv: { f: prof.bo.f, s: prof.bo.s } });
}
function startGame(r) {
  r.players.forEach(x => { x.lives = LIVES; x.alive = true; });
  r.phase = 'play'; r.turn = -1; r.letter = null; r.used.clear(); r.hist = []; r.winner = null; r.note = null;
  bcast(r, { t: 'ev', k: 'start' }); startTurn(r);
}
function matchmake() {
  for (let i = queue.length - 1; i >= 0; i--) if (queue[i].ws.readyState !== 1 || queue[i].ws.ctx.room) queue.splice(i, 1);
  const now = Date.now(); queue.sort((a, b) => a.elo - b.elo);
  for (let i = 0; i < queue.length - 1; i++) {
    const a = queue[i], b = queue[i + 1], wait = Math.max(now - a.since, now - b.since) / 1000;
    if (a.key === b.key || Math.abs(a.elo - b.elo) > 150 + 20 * wait) continue;
    queue.splice(i, 2); i--;
    const r = makeRoom('words', true);
    [a, b].forEach(q => { dropKey(q.key, r); addPlayer(q.ws, r); });
    startGame(r);
  }
}
setInterval(matchmake, 2000);

// ---------- друзья ----------
async function keyOfNick(lc) {
  if (nicks.has(lc)) return nicks.get(lc);
  const k = UP_URL ? await up(['GET', 'nk:' + lc]) : null;
  if (k) nicks.set(lc, k);
  return k || null;
}
function present(key) {
  const s = online.get(key); if (!s || !s.size) return { on: 0 };
  let room = '';
  for (const w of s) { const r = w.ctx.room; if (r && r.phase === 'lobby' && !r.rated && r.players.length < MAX_PLAYERS) room = r.code; }
  return { on: 1, room };
}
async function friendView(prof) {
  const fr = [], rq = [];
  for (const k of prof.fr) { const f = await getProfile(k); fr.push(Object.assign({ nick: f.nick, av: avUrl(f), lg: leagueOf(f.elo) }, present(k))); }
  for (const k of prof.rq) { const f = await getProfile(k); rq.push({ nick: f.nick, av: avUrl(f) }); }
  return { t: 'friends', fr, rq };
}
function link(a, b, ka, kb) {
  if (!a.fr.includes(kb)) a.fr.push(kb); if (!b.fr.includes(ka)) b.fr.push(ka);
  a.rq = a.rq.filter(x => x !== kb); b.rq = b.rq.filter(x => x !== ka);
  dirty.add(ka); dirty.add(kb); checkAch(ka); checkAch(kb);
}
async function friendsMsg(ws, m) {
  const c = ws.ctx; if (!c.key) return;
  const key = c.key, prof = await getProfile(key);
  if (!prof.nick) return send(ws, { t: 'err', msg: 'Сначала выбери ник' });
  const view = async () => send(ws, await friendView(prof));
  if (m.t === 'friends_get') return view();
  if (!hit(friendRate, key, 40, 3600000)) return send(ws, { t: 'err', msg: 'Слишком часто, подожди' });
  const nm = typeof m.nick === 'string' ? cleanName(m.nick) : null;
  const tk = nm ? await keyOfNick(nm.toLowerCase()) : null;
  if (!tk) return send(ws, { t: 'err', msg: 'Игрок не найден' });
  const tp = await getProfile(tk);
  if (m.t === 'friend_add') {
    if (tk === key) return send(ws, { t: 'err', msg: 'Это ты сам' });
    if (prof.fr.includes(tk)) return send(ws, { t: 'err', msg: 'Он уже у тебя в друзьях' });
    if (prof.fr.length >= 50) return send(ws, { t: 'err', msg: 'Максимум 50 друзей' });
    if (prof.rq.includes(tk)) { link(prof, tp, key, tk); send(ws, { t: 'info', msg: 'Теперь вы друзья' }); return view(); }
    if (!tp.rq.includes(key)) {
      if (tp.rq.length >= 30) return send(ws, { t: 'err', msg: 'У игрока слишком много заявок' });
      tp.rq.push(key); dirty.add(tk); tellKey(tk, { t: 'frq', nick: prof.nick });
    }
    return send(ws, { t: 'info', msg: 'Заявка отправлена' });
  }
  if (m.t === 'friend_resp') {
    if (!prof.rq.includes(tk)) return;
    prof.rq = prof.rq.filter(x => x !== tk); dirty.add(key);
    if (m.ok && prof.fr.length < 50 && tp.fr.length < 50) { link(prof, tp, key, tk); tellKey(tk, { t: 'info', msg: prof.nick + ' принял заявку в друзья' }); }
    return view();
  }
  if (m.t === 'friend_del') { prof.fr = prof.fr.filter(x => x !== tk); tp.fr = tp.fr.filter(x => x !== key); dirty.add(key); dirty.add(tk); return view(); }
  if (m.t === 'invite') {
    const r = c.room;
    if (!r || r.phase !== 'lobby' || r.rated) return send(ws, { t: 'err', msg: 'Приглашать можно из лобби' });
    if (!prof.fr.includes(tk)) return send(ws, { t: 'err', msg: 'Игрок не в друзьях' });
    if (!hit(inviteRate, key, 20, 600000)) return send(ws, { t: 'err', msg: 'Слишком много приглашений' });
    const so = online.get(tk); if (!so || !so.size) return send(ws, { t: 'info', msg: 'Друг не в сети' });
    so.forEach(w => send(w, { t: 'invite', from: prof.nick, code: r.code }));
    return send(ws, { t: 'info', msg: 'Приглашение отправлено' });
  }
}

// ---------- ежедневные задания (одинаковые для всех, меняются в полночь по Минску) ----------
const QPOOL = [
  { id: 'w8', text: 'Напиши 8 слов', target: 8, r: 'f', ev: e => e.k === 'word' },
  { id: 'w15', text: 'Напиши 15 слов', target: 15, r: 's', ev: e => e.k === 'word' },
  { id: 'rare2', text: 'Напиши 2 новых редких слова или выше', target: 2, r: 'f', ev: e => e.k === 'word' && e.fresh && e.tier >= 1 },
  { id: 'epic1', text: 'Напиши новое эпическое слово или выше', target: 1, r: 's', ev: e => e.k === 'word' && e.fresh && e.tier >= 2 },
  { id: 'long1', text: 'Напиши слово длиннее 9 букв', target: 1, r: 'f', ev: e => e.k === 'word' && e.len > 9 },
  { id: 'mode5', text: 'Напиши 5 слов в режиме не «Слова»', target: 5, r: 's', ev: e => e.k === 'word' && e.mode !== 'words' },
  { id: 'game2', text: 'Доиграй 2 игры до конца', target: 2, r: 'f', ev: e => e.k === 'game' },
  { id: 'win1', text: 'Выиграй игру', target: 1, r: 's', ev: e => e.k === 'win' }
];
const dayNum = () => Math.floor((Date.now() + 3 * 3600000) / 86400000);
function pick3(n) {
  let st = (n * 2654435761) >>> 0;
  const rnd = () => { st = (st + 0x6D2B79F5) >>> 0; let t = Math.imul(st ^ (st >>> 15), 1 | st); t ^= t + Math.imul(t ^ (t >>> 7), 61 | t); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
  const a = QPOOL.map((_, i) => i);
  for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(rnd() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; }
  return a.slice(0, 3);
}
function dailyOf(f) {
  const d = dayNum();
  if (!f.dq || f.dq.d !== d) f.dq = { d, q: pick3(d).map(i => QPOOL[i].id), p: [0, 0, 0], done: [0, 0, 0] };
  return f.dq;
}
const qList = f => { const dq = dailyOf(f); return dq.q.map((id, i) => { const d = QPOOL.find(x => x.id === id); return { text: d.text, target: d.target, p: Math.min(dq.p[i], d.target), done: !!dq.done[i], r: d.r }; }); };
function qEvent(p, e) {
  const f = p.prof, dq = dailyOf(f), newly = []; let changed = false;
  dq.q.forEach((id, i) => {
    if (dq.done[i]) return;
    const d = QPOOL.find(x => x.id === id);
    if (!d.ev(e)) return;
    changed = true; dq.p[i]++;
    if (dq.p[i] >= d.target) { dq.done[i] = 1; newly.push(i); if (f.bo[d.r] < 5) f.bo[d.r]++; }
  });
  if (!changed) return;
  dirty.add(p.key);
  send(p.ws, { t: 'dq', list: qList(f), newly });
  if (newly.length) { f.qd += newly.length; send(p.ws, { t: 'inv', f: f.bo.f, s: f.bo.s }); }
  checkAch(p.key);
}

function award(r, win) {
  if (r.rated) rate(r, win);
  if (r.hist.length < 4) return; // слишком короткие игры не идут в статистику (защита от накрутки)
  r.players.forEach(x => { x.prof.games++; dirty.add(x.key); });
  if (win) win.prof.wins++;
  r.players.forEach(x => qEvent(x, { k: 'game' }));
  if (win) qEvent(win, { k: 'win' });
  r.players.forEach(x => checkAch(x.key));
}

const HTML = fs.readFileSync(path.join(__dirname, 'public', 'index.html'), 'utf8').replace(/\r\n?/g, '\n').replace('__GID__', GID);
const SM = /<script>([\s\S]*?)<\/script>/.exec(HTML);
if (!SM) throw new Error('inline script not found');
const HASH = "'sha256-" + crypto.createHash('sha256').update(SM[1]).digest('base64') + "'"; // только наш скрипт может выполняться
const readPub = f => { try { return fs.readFileSync(path.join(__dirname, 'public', f), 'utf8'); } catch { return null; } };
const PAGES = { '/privacy': readPub('privacy.html'), '/terms': readPub('terms.html'), '/about': readPub('about.html') };
async function handleAvatar(req, res) {
  const end = (c, m) => { res.writeHead(c, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end(m || ''); };
  const org = req.headers.origin;
  if (org && !originOk(org, req.headers.host)) return end(403);
  const sess = String(req.headers.authorization || '').replace(/^Bearer /, '');
  if (!/^[a-f0-9]{32}$/.test(sess)) return end(401);
  const key = await loadSession(sess);
  if (!key) return end(401);
  if (!hit(avRate, key, 6, 3600000)) return end(429, 'Слишком часто, подожди');
  const chunks = []; let n = 0;
  try { for await (const ch of req) { n += ch.length; if (n > 65536) return end(413, 'Файл слишком большой'); chunks.push(ch); } } catch { return end(400); }
  const buf = Buffer.concat(chunks);
  if (!imgType(buf)) return end(415, 'Нужен JPEG, PNG или WebP');
  const prof = await getProfile(key);
  if (!prof.av) prof.av = crypto.randomBytes(6).toString('hex');
  let ok = false; try { ok = await avPut(prof.av, buf); } catch (e) { console.error('avatar put', e.message); }
  if (!ok) return end(500, 'Не удалось сохранить');
  prof.avv = (prof.avv || 0) + 1; dirty.add(key); tellKey(key, { t: 'me', av: avUrl(prof) }); checkAch(key);
  end(200, avUrl(prof));
}
async function serveAvatar(id, res) {
  let b = avCache.get(id);
  if (!b) { try { b = await avGet(id); } catch { b = null; } if (b && b.length) { avCache.set(id, b); if (avCache.size > 300) avCache.delete(avCache.keys().next().value); } }
  const t = b && imgType(b);
  if (!t) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': t, 'Cache-Control': 'public, max-age=31536000, immutable', 'X-Content-Type-Options': 'nosniff', 'Content-Security-Policy': "default-src 'none'" });
  res.end(b);
}

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/ping') { res.writeHead(200); return res.end('ok'); }
  const pth = (req.url || '').split('?')[0];
  if (req.method === 'GET' && PAGES[pth]) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'X-Content-Type-Options': 'nosniff', 'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; img-src data:" });
    return res.end(PAGES[pth]);
  }
  if (req.method === 'GET' && /^\/google[0-9a-f]{16}\.html$/.test(pth) && readPub(pth.slice(1))) { // файл подтверждения Google Search Console
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); return res.end(readPub(pth.slice(1)));
  }
  if (req.method === 'POST' && pth === '/avatar') return handleAvatar(req, res);
  if (req.method === 'GET' && /^\/a\/[0-9a-f]{12}$/.test(pth)) return serveAvatar(pth.slice(3), res);
  if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'X-Content-Type-Options': 'nosniff',
      'X-Frame-Options': 'DENY',
      'Strict-Transport-Security': 'max-age=31536000',
      'Permissions-Policy': 'camera=(), microphone=(), geolocation=()',
      'Referrer-Policy': 'no-referrer',
      'Content-Security-Policy': "default-src 'self'; script-src " + HASH + " https://accounts.google.com/gsi/client; style-src 'unsafe-inline' https://accounts.google.com/gsi/style https://fonts.googleapis.com; font-src https://fonts.gstatic.com; frame-src https://accounts.google.com/gsi/; connect-src 'self' ws: wss: https://accounts.google.com/gsi/; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'; object-src 'none'"
    });
    return res.end(HTML);
  }
  res.writeHead(404); res.end();
});

const rooms = new Map(), ipConns = new Map(), ipFails = new Map();
const wss = new WebSocketServer({ server, maxPayload: 4096 });

const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const bcast = (r, o) => r.players.forEach(p => send(p.ws, o));
const pub = r => ({
  t: 'state', code: r.code, phase: r.phase, host: r.hostId,
  turn: r.phase === 'play' ? r.players[r.turn].id : null,
  letter: r.letter, left: Math.max(0, r.deadline - Date.now()), total: TURN_MS + (r.frz ? 10000 : 0), mode: r.mode, rated: !!r.rated,
  count: r.used.size, hist: r.hist.slice(-6), winner: r.winner, note: r.note,
  players: r.players.map(p => ({ id: p.id, name: p.name, lives: p.lives, alive: p.alive, off: !p.ws, av: avUrl(p.prof) }))
});
const sync = r => bcast(r, pub(r));

function genCode() { let c; do { c = String(crypto.randomInt(10000, 100000)); } while (rooms.has(c)); return c; }
function cleanName(s) {
  if (typeof s !== 'string') return null;
  s = s.trim().replace(/\s+/g, ' ');
  return /^[\p{L}\p{N}_ -]{2,16}$/u.test(s) ? s : null;
}

function checkWord(r, w) {
  if (w.length < 2 || w.length > 40 || !/^[а-я]+([ -][а-я]+)*$/.test(w)) return 'Только русские буквы, минимум 2';
  if (r.letter && w[0] !== r.letter) return `Слово должно начинаться на «${r.letter.toUpperCase()}»`;
  if (r.used.has(w)) return 'Это слово уже было';
  const st = r.mode === 'all' ? ALL : SETS[r.mode];
  if (st && !st.has(w)) return r.mode === 'words' ? 'Такого слова нет в словаре' : 'Такого нет в категории «' + (MODE_NAME[r.mode] || '') + '»';
  return null;
}

function armTimer(r) {
  clearTimeout(r.timer);
  const cur = r.players[r.turn];
  r.timer = setTimeout(() => {
    cur.lives--; if (cur.lives <= 0) cur.alive = false;
    bcast(r, { t: 'ev', k: 'timeout', name: cur.name });
    startTurn(r);
  }, Math.max(0, r.deadline - Date.now()));
}

function startTurn(r) {
  clearTimeout(r.timer);
  const alive = r.players.filter(p => p.alive);
  if (alive.length < 2) {
    r.phase = 'over'; r.winner = alive[0] ? alive[0].name : null;
    award(r, alive[0]); bcast(r, { t: 'ev', k: 'over' }); return sync(r);
  }
  do { r.turn = (r.turn + 1) % r.players.length; } while (!r.players[r.turn].alive);
  r.deadline = Date.now() + TURN_MS; r.turnAt = Date.now(); r.frz = false;
  armTimer(r);
  sync(r);
}

function attach(ws, r, p) {
  if (p.ws && p.ws !== ws) { p.ws.ctx.room = null; p.ws.ctx.player = null; p.ws.close(); }
  clearTimeout(p.kick); p.kick = null; p.ws = ws; ws.ctx.room = r; ws.ctx.player = p;
  send(ws, { t: 'joined', id: p.id, inv: { f: p.prof.bo.f, s: p.prof.bo.s } });
  bcast(r, { t: 'ev', k: 'back', name: p.name }); sync(r);
}

function leave(ws) {
  const r = ws.ctx.room, p = ws.ctx.player;
  if (!r) return;
  ws.ctx.room = null; ws.ctx.player = null;
  if (p.ws !== ws) return;
  p.ws = null;
  bcast(r, { t: 'ev', k: 'off', name: p.name }); sync(r);
  p.kick = setTimeout(() => { p.kick = null; drop(r, p); }, GRACE_MS);
}

function drop(r, p) {
  clearTimeout(p.kick); p.kick = null;
  if (p.gone || !r.players.includes(p)) return;
  p.gone = true;
  if (r.phase === 'lobby') r.players = r.players.filter(x => x !== p); else p.alive = false;
  const pending = r.players.filter(x => !x.gone && (x.ws || x.kick));
  if (!pending.length) { clearTimeout(r.timer); rooms.delete(r.code); return; }
  if (r.hostId === p.id) r.hostId = (r.players.find(x => !x.gone && x.ws) || pending[0]).id;
  bcast(r, { t: 'ev', k: 'left', name: p.name });
  if (r.phase === 'play') {
    const alive = r.players.filter(x => x.alive);
    if (alive.length < 2) {
      clearTimeout(r.timer); r.phase = 'over'; r.winner = alive[0] ? alive[0].name : null; r.note = p.name + ' вышел';
      award(r, alive[0]); bcast(r, { t: 'ev', k: 'over' });
    } else if (r.players[r.turn] === p) return startTurn(r);
  }
  sync(r);
}

function dropKey(key, except) {
  rooms.forEach(r => {
    if (r === except) return;
    const p = r.players.find(x => x.key === key && !x.gone);
    if (!p) return;
    if (p.ws) { p.ws.ctx.room = null; p.ws.ctx.player = null; }
    p.ws = null; drop(r, p);
  });
}

async function handle(ws, m) {
  const c = ws.ctx;
  if (m.t === 'auth') {
    if (!hit(authRate, c.ip, 20, 60000)) return send(ws, { t: 'err', msg: 'Слишком часто, подожди минуту' });
    let key = null, sess = null, nick = '';
    if (typeof m.cred === 'string' && m.cred.length < 4096 && oc) {
      try {
        const pl = (await oc.verifyIdToken({ idToken: m.cred, audience: GID })).getPayload();
        key = gkey(pl.sub); nick = pl.given_name || pl.name || '';
        sess = crypto.randomBytes(16).toString('hex'); await saveSession(sess, key);
      } catch { return send(ws, { t: 'autherr' }); }
    } else if (typeof m.sess === 'string' && /^[a-f0-9]{32}$/.test(m.sess)) {
      key = await loadSession(m.sess);
      if (!key) return send(ws, { t: 'autherr' });
    } else return send(ws, { t: 'autherr' });
    c.key = key;
    (online.get(key) || online.set(key, new Set()).get(key)).add(ws);
    const prof = await getProfile(key);
    return send(ws, { t: 'authed', sess, nick: prof.nick, suggest: nick.slice(0, 16), modes: MODES_AVAIL, inv: { f: prof.bo.f, s: prof.bo.s }, dq: qList(prof), av: avUrl(prof), elo: prof.elo, lg: leagueOf(prof.elo), lv: levelOf(prof) });
  }
  if (m.t === 'setnick') {
    if (!c.key || c.room) return send(ws, { t: 'err', msg: 'Ник нельзя менять в комнате' });
    const nk = cleanName(m.nick);
    if (!nk || VULGAR.test(nk.toLowerCase())) return send(ws, { t: 'err', msg: 'Ник: 2-16 символов, буквы, цифры, пробел, _ -' });
    if (!hit(nickRate, c.key, 5, 3600000)) return send(ws, { t: 'err', msg: 'Слишком часто меняешь ник, подожди' });
    const prof = await getProfile(c.key), lc = nk.toLowerCase();
    if (prof.nick && prof.nick.toLowerCase() === lc) { prof.nick = nk; dirty.add(c.key); return send(ws, { t: 'nick', nick: nk }); }
    const res = await reserveNick(lc, c.key);
    if (res === 'taken') return send(ws, { t: 'err', msg: 'Этот ник уже занят' });
    if (res !== 'ok') return send(ws, { t: 'err', msg: 'Не получилось сохранить, попробуй еще раз' });
    if (prof.nick) { const old = prof.nick.toLowerCase(); nicks.delete(old); up(['DEL', 'nk:' + old]); }
    prof.nick = nk; dirty.add(c.key); await flush();
    return send(ws, { t: 'nick', nick: nk });
  }
  if (m.t === 'delav') {
    if (!c.key) return;
    const f = await getProfile(c.key);
    if (f.av) { const id = f.av; f.av = ''; avCache.delete(id); avDel(id); dirty.add(c.key); }
    return tellKey(c.key, { t: 'me', av: '' });
  }
  if (m.t === 'top') return send(ws, { t: 'top', list: await lbTop() });
  if (m.t === 'queue') {
    if (!c.key || c.room || queue.some(q => q.ws === ws)) return;
    const f = await getProfile(c.key);
    if (!f.nick) return send(ws, { t: 'err', msg: 'Сначала выбери ник' });
    queue.push({ ws, key: c.key, elo: f.elo, since: Date.now() });
    return send(ws, { t: 'queued' });
  }
  if (m.t === 'unqueue') { qRemove(ws); return send(ws, { t: 'unqueued' }); }
  if (/^(friends_get|friend_add|friend_resp|friend_del|invite)$/.test(m.t)) return friendsMsg(ws, m);
  if (m.t === 'dqget') {
    if (!c.key) return;
    return send(ws, { t: 'dq', list: qList(await getProfile(c.key)), newly: [] });
  }
  if (m.t === 'logout') {
    if (typeof m.sess === 'string' && /^[a-f0-9]{32}$/.test(m.sess)) { sessions.delete(sh(m.sess)); up(['DEL', 's:' + sh(m.sess)]); }
    presenceOff(ws); qRemove(ws); c.key = null; return;
  }
  if (m.t === 'create' || m.t === 'join') {
    if (c.room) return;
    qRemove(ws);
    const key = c.key;
    if (!key) return send(ws, { t: 'err', msg: 'Сначала войди через Google' });
    const prof = await getProfile(key);
    const name = prof.nick;
    if (!name) return send(ws, { t: 'err', msg: 'Сначала выбери ник' });
    let r;
    if (m.t === 'create') {
      if (rooms.size >= MAX_ROOMS) return send(ws, { t: 'err', msg: 'Сервер переполнен, попробуй позже' });
      if (!hit(createRate, key, 6, 600000)) return send(ws, { t: 'err', msg: 'Слишком много новых комнат, подожди несколько минут' });
      r = { code: genCode(), players: [], hostId: null, phase: 'lobby', turn: -1, letter: null, used: new Set(), hist: [], deadline: 0, timer: null, winner: null, note: null, last: Date.now(), mode: 'words', frz: false };
      rooms.set(r.code, r);
    } else {
      const f = ipFails.get(c.ip) || { n: 0, t: Date.now() };
      if (Date.now() - f.t > 60000) { f.n = 0; f.t = Date.now(); }
      if (f.n >= 10) return send(ws, { t: 'err', msg: 'Слишком много попыток, подожди минуту' });
      r = typeof m.code === 'string' && /^\d{5}$/.test(m.code) ? rooms.get(m.code) : null;
      if (!r) { f.n++; ipFails.set(c.ip, f); return send(ws, { t: 'err', msg: 'Комната не найдена' }); }
      const ex = r.players.find(x => x.key === key && !x.gone);
      if (ex) { dropKey(key, r); return attach(ws, r, ex); }
      if (r.rated) return send(ws, { t: 'err', msg: 'Комната не найдена' });
      if (r.phase !== 'lobby') return send(ws, { t: 'err', msg: 'Игра уже идет' });
      if (r.players.length >= MAX_PLAYERS) return send(ws, { t: 'err', msg: 'Комната заполнена' });
      if (r.players.some(p => p.name.toLowerCase() === name.toLowerCase())) return send(ws, { t: 'err', msg: 'Этот ник уже занят в комнате' });
    }
    dropKey(key, r);
    const p = { id: crypto.randomBytes(4).toString('hex'), name, ws, lives: LIVES, alive: true, key, prof, kick: null, gone: false };
    r.players.push(p); if (!r.hostId) r.hostId = p.id;
    c.room = r; c.player = p;
    send(ws, { t: 'joined', id: p.id, inv: { f: p.prof.bo.f, s: p.prof.bo.s } }); sync(r);
    return;
  }
  if (m.t === 'resume') {
    if (c.room) return;
    const key = c.key, rr = typeof m.code === 'string' ? rooms.get(m.code) : null;
    const ex = key && rr ? rr.players.find(x => x.key === key && !x.gone) : null;
    if (!ex) return send(ws, { t: 'noroom' });
    return attach(ws, rr, ex);
  }
  const r = c.room, p = c.player;
  if (!r) return;
  r.last = Date.now();
  if (m.t === 'mode') {
    if (p.id === r.hostId && r.phase === 'lobby' && MODES_AVAIL.includes(m.mode)) { r.mode = m.mode; sync(r); }
    return;
  }
  if (m.t === 'react') {
    if (!['😂', '🔥', '👏', '😮', '💀', '❤️'].includes(m.e) || Date.now() - (p.rt || 0) < 800) return;
    p.rt = Date.now(); return bcast(r, { t: 'ev', k: 'react', name: p.name, e: m.e });
  }
  if (m.t === 'boost') {
    if (r.phase !== 'play' || r.players[r.turn] !== p) return;
    if (r.rated) return send(ws, { t: 'err', msg: 'В рейтинговой игре бусты отключены' });
    const b = p.prof.bo;
    if (m.k === 'freeze') {
      if (b.f < 1) return send(ws, { t: 'err', msg: 'Нет заморозок' });
      if (r.frz) return send(ws, { t: 'err', msg: 'Заморозку можно один раз за ход' });
      b.f--; r.frz = true; r.deadline += 10000; armTimer(r);
    } else if (m.k === 'skip') {
      if (b.s < 1) return send(ws, { t: 'err', msg: 'Нет пропусков' });
      b.s--;
    } else return;
    dirty.add(p.key);
    send(ws, { t: 'inv', f: b.f, s: b.s });
    bcast(r, { t: 'ev', k: 'boost', name: p.name, b: m.k });
    if (m.k === 'skip') return startTurn(r);
    return sync(r);
  }
  if (m.t === 'quit') { c.room = null; c.player = null; p.ws = null; drop(r, p); return; }
  if (m.t === 'profile') {
    const x = r.players.find(y => y.id === m.id); if (!x) return;
    const f = x.prof, top = Object.entries(f.w).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([w, n]) => [w, n, tier(w)]);
    return send(ws, { t: 'profile', id: x.id, name: x.name, games: f.games, wins: f.wins, total: f.total, tiers: f.t, best: f.best, top, myth: f.m, lv: levelOf(f), elo: f.elo, lg: leagueOf(f.elo), rg: f.rg, av: avUrl(f), ach: ACH.map(([id, n]) => [n, f.ac.includes(id) ? 1 : 0]) });
  }
  if (m.t === 'start' || m.t === 'again') {
    if (p.id !== r.hostId || r.rated) return;
    if (m.t === 'start' && r.phase === 'lobby') {
      if (r.players.length < 2) return send(ws, { t: 'err', msg: 'Нужно минимум 2 игрока' });
      r.players.forEach(x => { x.lives = LIVES; x.alive = true; });
      r.phase = 'play'; r.turn = -1; r.letter = null; r.used.clear(); r.hist = []; r.winner = null; r.note = null;
      bcast(r, { t: 'ev', k: 'start' }); startTurn(r);
    } else if (m.t === 'again' && r.phase === 'over') {
      r.players = r.players.filter(x => !x.gone); r.phase = 'lobby'; sync(r);
    }
    return;
  }
  if (m.t === 'word') {
    if (r.phase !== 'play' || r.players[r.turn].id !== p.id || typeof m.w !== 'string' || m.w.length > 40) return;
    // анти-бот: человек не отвечает быстрее ~0.45 с после начала хода; повторы подряд блокируем
    const since = Date.now() - (r.turnAt || 0);
    if (since < 450) { p.fast = (p.fast || 0) + 1; if (p.fast >= 4) return ws.close(1008); return; }
    if (Date.now() - (p.lastW || 0) < 300) return;
    p.lastW = Date.now();
    const w = norm(m.w), bad = checkWord(r, w);
    if (bad) { p.bad = (p.bad || 0) + 1; if (p.bad > 40) return ws.close(1008); return send(ws, { t: 'err', msg: bad }); }
    const tr = tier(w), aw = addWord(p, w, tr), gain = aw.gain;
    r.used.add(w); r.letter = lastLetter(w); r.hist.push({ n: p.name, w, t: tr });
    bcast(r, { t: 'ev', k: 'ok', name: p.name, word: w, tier: tr });
    startTurn(r);
    if (gain) send(ws, { t: 'inv', f: p.prof.bo.f, s: p.prof.bo.s, gain });
    qEvent(p, { k: 'word', tier: tr, fresh: aw.fresh, len: w.replace(/[ -]/g, '').length, mode: r.mode });
    checkAch(p.key);
  }
}

wss.on('connection', (ws, req) => {
  const xff = String(req.headers['x-forwarded-for'] || '').split(',').pop().trim();
  const ip = (TRUST_PROXY && xff) || req.socket.remoteAddress;
  ws.on('error', () => {});
  const org = req.headers.origin;
  if (org && !originOk(org, req.headers.host)) return ws.close(1008);
  if (!hit(connRate, ip, 60, 60000)) return ws.close(1008);
  const n = (ipConns.get(ip) || 0) + 1;
  if (n > MAX_PER_IP) return ws.close(1008);
  ipConns.set(ip, n);
  ws.alive = true; ws.on('pong', () => { ws.alive = true; });
  ws.ctx = { ip, key: null, room: null, player: null, tokens: 10, last: Date.now() };
  ws.on('message', data => {
    const c = ws.ctx, now = Date.now();
    c.tokens = Math.min(10, c.tokens + (now - c.last) / 200); c.last = now; // 5 сообщений/сек, всплеск 10
    if (--c.tokens < 0) return ws.close(1008);
    let m; try { m = JSON.parse(data); } catch { return ws.close(1003); }
    if (!m || typeof m !== 'object' || typeof m.t !== 'string') return;
    if (c.busy) return; c.busy = true;
    Promise.resolve().then(() => handle(ws, m)).catch(e => console.error(e)).finally(() => { c.busy = false; });
  });
  ws.on('close', () => {
    const k = (ipConns.get(ip) || 1) - 1; if (k <= 0) ipConns.delete(ip); else ipConns.set(ip, k);
    presenceOff(ws); qRemove(ws);
    leave(ws);
  });
  ws.on('error', () => {});
});

setInterval(() => {
  wss.clients.forEach(w => { if (!w.alive) return w.terminate(); w.alive = false; w.ping(); });
  const now = Date.now(); ipFails.forEach((f, k) => { if (now - f.t > 60000) ipFails.delete(k); });
}, 15000);

setInterval(() => {
  const now = Date.now();
  rooms.forEach(r => {
    if (now - r.last < 30 * 60000) return;
    clearTimeout(r.timer);
    r.players.forEach(p => { clearTimeout(p.kick); if (p.ws) { p.ws.ctx.room = null; p.ws.ctx.player = null; send(p.ws, { t: 'noroom' }); } });
    rooms.delete(r.code);
  });
  [connRate, authRate, createRate, nickRate, avRate, friendRate, inviteRate].forEach(m => m.forEach((e, k) => { if (now - e.t > 600000) m.delete(k); }));
  if (profiles.size > 2000) {
    const live = new Set(); rooms.forEach(r => r.players.forEach(p => live.add(p.key)));
    for (const k of [...profiles.keys()]) if (!live.has(k) && !dirty.has(k)) profiles.delete(k);
  }
  if (sessions.size > 5000) sessions.clear();
}, 60000);

server.listen(PORT, () => console.log('Словобой: http://localhost:' + PORT));
