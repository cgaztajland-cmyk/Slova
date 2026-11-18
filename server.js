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
  DICT = new Set(fs.readFileSync(path.join(__dirname, 'words.txt'), 'utf8').split(/\r?\n/).map(norm).filter(Boolean));
  console.log('Словарь загружен:', DICT.size);
} catch { console.log('words.txt не найден: проверка по словарю ОТКЛЮЧЕНА, принимается любое слово'); }

const lastLetter = w => { for (let i = w.length - 1; i >= 0; i--) if (!'ьъый'.includes(w[i])) return w[i]; return w.slice(-1); };

// ---------- профили, редкость слов, хранилище ----------
const UP_URL = process.env.UPSTASH_URL, UP_TOKEN = process.env.UPSTASH_TOKEN, GRACE_MS = 90000;
async function up(cmd) {
  if (!UP_URL) return S3C ? kvS3(cmd) : null;
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
// ---------- ключ-значение поверх S3: профили, ники, сессии, рейтинг (без Redis) ----------
const KV = !!(UP_URL || S3C);
console.log('Профили, ники и сессии:', UP_URL ? 'Upstash Redis' : S3C ? 'в S3 (сохраняются между перезапусками)' : 'ТОЛЬКО В ПАМЯТИ (задай S3_BUCKET, S3_KEY_ID, S3_SECRET)');
const kvLock = new Map();
const DV = String(process.env.DATA_VERSION || '').replace(/[^a-zA-Z0-9_-]/g, ''); // новая версия данных = все профили начинаются с нуля
const PLAYERS_DIR = 'players' + DV + '/';
const kvKey = k => 'kv' + DV + '/' + Buffer.from(String(k)).toString('base64url');
function kvLocked(k, fn) { const prev = kvLock.get(k) || Promise.resolve(), run = prev.then(fn, fn); kvLock.set(k, run.catch(() => {})); return run; }
async function kvRead(k) { const b = await S3C.get(kvKey(k)); if (!b) return null; const o = JSON.parse(b.toString()); return o.e && o.e < Date.now() ? null : o.v; }
async function kvWrite(k, v, e) { await S3C.put(kvKey(k), Buffer.from(JSON.stringify({ v, e: e || 0 })), 'application/json'); }
async function kvS3(cmd) {
  const [op, k, v, ...rest] = cmd;
  try {
    if (op === 'GET') return await kvRead(k);
    if (op === 'SET') {
      const nx = rest.includes('NX'), ei = rest.indexOf('EX'), e = ei >= 0 ? Date.now() + Number(rest[ei + 1]) * 1000 : 0;
      return await kvLocked(k, async () => { if (nx && (await kvRead(k)) !== null) return null; await kvWrite(k, v, e); return 'OK'; });
    }
    if (op === 'DEL') return await kvLocked(k, async () => { await S3C.del(kvKey(k)); return 1; });
    if (op === 'ZADD') return await kvLocked(k, async () => { const o = (await kvRead(k)) || {}; o[rest[0]] = Number(v); await kvWrite(k, o); return 1; });
    if (op === 'ZREVRANGE') { const o = (await kvRead(k)) || {}; return Object.entries(o).sort((a, b) => b[1] - a[1]).slice(Number(v), Number(rest[0]) + 1).map(x => x[0]); }
  } catch (e) { console.log('KV ошибка', op, String(e.message || e).slice(0, 120)); }
  return undefined;
}
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
  if (KV) {
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
// ---------- ID игроков, роли, баны и ручные команды через Filebase ----------
const CREATOR_EMAILS = (process.env.CREATOR_EMAIL || '').toLowerCase().split(',').map(x => x.trim()).filter(Boolean);
const CREATOR_HASHES = CREATOR_EMAILS.map(e => sh('em:' + e));
const CREATOR_IDS = (process.env.CREATOR_ID || '').split(',').map(x => x.trim()).filter(Boolean);
const uidMap = new Map();
async function ensureUid(key, f) {
  if (f.uid) { uidMap.set(f.uid, key); return f.uid; }
  for (let i = 0; i < 8; i++) {
    const id = String(1000000 + crypto.randomInt(9000000));
    if (uidMap.has(id)) continue;
    if (KV) { const r = await up(['SET', 'uid:' + id, key, 'NX']); if (r !== 'OK') continue; }
    f.uid = id; uidMap.set(id, key); dirty.add(key); syncPlayerFile(key, f, true); return id;
  }
  return null;
}
async function keyOfUid(id) { if (uidMap.has(id)) return uidMap.get(id); if (!KV) return null; const k = await up(['GET', 'uid:' + id]); if (k) uidMap.set(id, k); return k || null; }
const ADM_HELP = '# Чтобы выдать команду, напиши её отдельной строкой БЕЗ решётки в начале. Строки с решёткой сервер игнорирует.\n# Примеры (скопируй нужную строку без решётки и больше ничего в ней не пиши):\n# PERM_BAN\n# BAN_3\n# UNBAN\n# LEVEL_20\n# RATING_1200\n# ROLE_CREATOR\n# Значение: PERM_BAN бан навсегда, BAN_3 бан на 3 дня, UNBAN снять бан, LEVEL_20 уровень 20, RATING_1200 рейтинг 1200, ROLE_CREATOR статус СОЗДАТЕЛЬ (ROLE_NONE убирает).\n# Сервер читает файл при входе игрока и раз в минуту, пока он онлайн.\n# Чтобы выполнить ту же команду ещё раз, добавь под ней любую строку (например: повтор 2).\n';
const ADM_HELP_OLD = '# Команды (по одной в строке, без решётки). Сервер читает файл при входе игрока и раз в минуту, пока он онлайн.\n# PERM_BAN      бан навсегда\n# BAN_3         бан на 3 дня\n# UNBAN         снять бан\n# LEVEL_20      поставить уровень 20\n# RATING_1200   поставить рейтинг 1200\n# ROLE_CREATOR  статус СОЗДАТЕЛЬ (ROLE_NONE убирает)\n# Чтобы выполнить ту же команду ещё раз, добавь под ней любую строку (например: повтор 2).\n';
function adminTemplate(f) {
  if (!S3C) return; const k = PLAYERS_DIR + f.uid + '.txt';
  S3C.get(k).then(b => { if (!b) return S3C.put(k, Buffer.from('# Игрок ' + f.uid + '\nNICK=' + (f.nick || '') + '\n' + ADM_HELP), 'text/plain; charset=utf-8'); }).catch(() => {});
}
const CMD_RE = /^(PERMBAN|BANPERM|PERMABAN|BAN\d{1,4}|UNBAN|LEVEL\d{1,4}|RATING\d{1,5}|ROLECREATOR|CREATOR|СОЗДАТЕЛЬ|РОЛЬСОЗДАТЕЛЬ|ROLENONE|NOROLE)$/;
function cmdOf(l) {
  const nz = t => t.toUpperCase().replace(/[^A-ZА-Я0-9]/g, ''), tk = l.split(/\s+/);
  for (const c of [nz(l), nz(tk[0] || ''), nz(tk.slice(0, 2).join('')), nz(tk.slice(0, 3).join(''))]) if (CMD_RE.test(c)) return c;
  return null;
}
function plKey(f) { return 'players/' + f.uid + ' (' + String(f.nick || 'без ника').replace(/[\\/]/g, '_') + ').txt'; }
function plText(f) {
  const tn = ['редких', 'эпических', 'легендарных', 'мифических', 'секретных'], t = f.t || [];
  const top = Object.entries(f.w || {}).sort((a, b) => b[1] - a[1]).slice(0, 3).map(([w, n]) => w + ' (' + n + ')').join(', ') || '-';
  const ach = achList(f), open = ach.filter(a => a[1]).map(a => a[0]).join(', ') || '-', closed = ach.filter(a => !a[1]).map(a => a[0]).join(', ') || '-';
  const ban = isBanned(f) ? (f.banUntil === -1 ? 'навсегда' : 'до ' + new Date(f.banUntil).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' })) + (f.banReason ? ' (причина: ' + f.banReason + ')' : '') : 'нет';
  return ['НИК: ' + (f.nick || '-'), 'ПОЧТА: ' + (f.email || '-'), 'ВХОД: ' + (f.lt === 'pass' ? 'ник и пароль' : 'Google'), 'ID: ' + f.uid, 'РОЛЬ: ' + (f.role === 'creator' ? 'СОЗДАТЕЛЬ' : 'игрок'),
    'РЕГИСТРАЦИЯ: ' + new Date(f.reg || Date.now()).toLocaleDateString('ru-RU'), 'ИГР СЫГРАНО: ' + f.games, 'ПОБЕД: ' + f.wins, 'СЛОВ НАПИСАНО: ' + f.total,
    'САМОЕ ДЛИННОЕ СЛОВО: ' + ((f.best && !SECRETS.has(f.best)) ? f.best : '-'), 'УРОВЕНЬ: ' + levelOf(f), 'РЕЙТИНГ: ' + f.elo + ' (' + leagueOf(f.elo) + ')',
    'СЛОВА ПО РЕДКОСТИ: ' + tn.map((n, i) => n + ' ' + (t[i] || 0)).join(', '), 'ЛЮБИМЫЕ СЛОВА: ' + top, 'ДОСТИЖЕНИЯ ОТКРЫТЫ: ' + open, 'ДОСТИЖЕНИЯ ЗАКРЫТЫ: ' + closed,
    'БАН: ' + ban, '', 'Команды пиши под строкой КОМАНДЫ, по одной в строке: PERM_BAN, BAN_3 (дней), UNBAN, LEVEL_20, RATING_1200, ROLE_CREATOR, ROLE_NONE.', 'После применения команды стираются, а статус выше обновляется сам.', 'КОМАНДЫ:', ''].join('\n');
}
function applyCommandsText(key, f, txt) {
  const raw = txt.split(/\r?\n/).map(l => l.trim()), mi = raw.findIndex(l => /^КОМАНДЫ\s*:/i.test(l));
  const lines = (mi >= 0 ? raw.slice(mi + 1) : raw.filter(l => !/^NICK=/i.test(l))).filter(l => l && !l.startsWith('#'));
  const ok = lines.filter(cmdOf); if (!ok.length) return false;
  for (const l of ok) {
    const u = cmdOf(l); let m;
    if (u === 'PERMBAN' || u === 'BANPERM' || u === 'PERMABAN') f.banUntil = -1;
    else if ((m = /^BAN(\d{1,4})$/.exec(u))) f.banUntil = Date.now() + Number(m[1]) * 86400000;
    else if (u === 'UNBAN') { f.banUntil = 0; f.banReason = ''; }
    else if ((m = /^LEVEL(\d{1,4})$/.exec(u))) f.xpAdj = 100 * (Math.max(1, Number(m[1])) - 1) - (f.total + f.wins * 10 + f.qd * 20);
    else if ((m = /^RATING(\d{1,5})$/.exec(u))) { f.elo = Number(m[1]); lbSet(key, f.elo); }
    else if (u === 'ROLECREATOR' || u === 'CREATOR' || u === 'СОЗДАТЕЛЬ' || u === 'РОЛЬСОЗДАТЕЛЬ') f.role = 'creator';
    else if (u === 'ROLENONE' || u === 'NOROLE') f.role = '';
  }
  console.log('файл игрока', f.uid, 'команды применены:', ok.join(' ; ')); dirty.add(key); return true;
}
// файл игрока в Filebase: players/<ID> (<ник>).txt, обновляется сразу при изменениях профиля
async function syncPlayerFile(key, f, force) {
  if (!S3C || !f || !f.uid) return;
  try {
    const nk = plKey(f), olds = [];
    if (f.pfile && f.pfile !== nk) olds.push(f.pfile);
    if (!f.pfile) olds.push('players/' + f.uid + '.txt');
    for (const k of [nk, ...olds]) { const b = await S3C.get(k).catch(() => null); if (b) applyCommandsText(key, f, b.toString('utf8')); }
    const txt = plText(f), hsh = sh(txt);
    if (force || f.pfh !== hsh || f.pfile !== nk) {
      await S3C.put(nk, Buffer.from(txt), 'text/plain; charset=utf-8'); f.pfh = hsh;
      for (const k of olds) if (k !== nk) await S3C.del(k).catch(() => {});
      if (f.pfile !== nk) { f.pfile = nk; dirty.add(key); }
    }
  } catch (e) { console.log('файл игрока: ошибка', String(e.message || e).slice(0, 100)); }
}
async function applyAdmin(key, f) { return syncPlayerFile(key, f, false); }
const isBanned = f => f.banUntil === -1 || (f.banUntil > Date.now());
const banMsg = f => (f.banUntil === -1 ? 'Аккаунт заблокирован навсегда' : 'Аккаунт заблокирован до ' + new Date(f.banUntil).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) + ' (МСК)') + (f.banReason ? '. Причина: ' + f.banReason : '');
const profExtra = (c, f) => { const me = c.key && profiles.get(c.key); return me && me.role === 'creator' ? { adm: true, ban: isBanned(f) ? { until: f.banUntil, reason: f.banReason || '' } : null } : {}; };
setInterval(async () => {
  for (const [key, set] of [...online]) {
    const f = profiles.get(key); if (!f) continue;
    await applyAdmin(key, f);
    [...set].forEach(w => { if (w.lastRole !== (f.role || '')) { w.lastRole = f.role || ''; send(w, { t: 'role', role: f.role || '' }); } });
    if (isBanned(f)) [...set].forEach(w => { send(w, { t: 'banned', msg: banMsg(f) }); try { w.close(1008); } catch {} });
  }
}, 60000);
async function getProfile(key) {
  let p = profiles.get(key);
  if (p) return p;
  const raw = await up(['GET', 'p:' + key]);
  try { p = raw ? JSON.parse(raw) : null; } catch { p = null; }
  p = p || { name: '', games: 0, wins: 0, total: 0, t: [0, 0, 0, 0], m: [], w: {}, best: '' };
  p.nick = p.nick || ''; p.bo = p.bo || { f: 0, s: 0 };
  p.fr = p.fr || []; p.rq = p.rq || []; p.ac = p.ac || []; p.qd = p.qd || 0; p.rg = p.rg || 0; p.elo = p.elo ?? 800; p.av = p.av || ''; p.avv = p.avv || 0; p.reg = p.reg || Date.now(); p.mail = p.mail || []; p.mseen = p.mseen || 0; p.bo = p.bo || { f: 0, s: 0 }; p.bo.c = p.bo.c || 0; p.bo.l = p.bo.l || 0;
  if (p.nick) nicks.set(p.nick.toLowerCase(), key);
  while (p.t.length < 5) p.t.push(0);
  p.m = p.m || [];
  return profiles.get(key) || (profiles.set(key, p), p);
}
async function flush() {
  const keys = [...dirty]; dirty.clear();
  for (const k of keys) await up(['SET', 'p:' + k, JSON.stringify(profiles.get(k))]);
  for (const k of keys) { const f = profiles.get(k); if (f) await syncPlayerFile(k, f); }
}
setInterval(flush, 5000);
process.on('SIGTERM', async () => { await flush(); process.exit(0); });
process.on('uncaughtException', e => console.error('uncaught', e));
process.on('unhandledRejection', e => console.error('unhandled', e));

const loadLines = f => {
  try {
    return fs.readFileSync(path.join(__dirname, 'data', f), 'utf8').split(/\r?\n/).map(w => norm(w).replace(/\s*\(.*?\)\s*/g, '')).filter(w => w.length >= 2 && /^[а-я]+([ -][а-я]+)*$/.test(w));
  } catch { return []; }
};
const MODE_DEF = [['words', ''], ['cities', ''], ['names', ''], ['countries', '']];
const SETS = { words: DICT };
['animals', 'food', 'verbs', 'vulgar', 'extra_words', 'secrets'].forEach(k => loadLines(k + '.txt').forEach(w => DICT.add(w)));
try { fs.readFileSync(path.join(__dirname, 'words-ext.txt'), 'utf8').split(/\r?\n/).forEach(l => { const w = norm(l); if (w.length >= 2 && w.length <= 40 && /^[а-я]+([ -][а-я]+)*$/.test(w)) DICT.add(w); }); } catch {}
const VERBSET = new Set([...loadLines('verbs.txt'), ...loadLines('vulgar.txt'), ...loadLines('extra_words.txt').filter(w => /(ть|ться|ти|тись|чь|чься)$/.test(w))]);
const SECRETS = new Set(loadLines('secrets.txt'));
const EXTRA = new Set(loadLines('extra_words.txt'));
['cities', 'names', 'countries'].forEach(k => { const a = loadLines(k + '.txt'); SETS[k] = a.length ? new Set(a) : null; });
const ALL = new Set(); Object.values(SETS).forEach(x => x && x.forEach(w => ALL.add(w)));
const MODES_AVAIL = MODE_DEF.filter(([k]) => SETS[k]).map(([k]) => k);
if (MODES_AVAIL.length > 1) MODES_AVAIL.push('all');
const MODE_NAME = Object.fromEntries(MODE_DEF.concat([['all', 'Все вместе']]));
console.log('Режимы:', MODES_AVAIL.map(k => k + (SETS[k] ? ':' + SETS[k].size : '')).join(', '));
const { buildTiers } = require('./tiers');
const TIERS = DICT ? buildTiers([...DICT]) : null;
if (TIERS) {
  const cnt = [0, 0, 0, 0, 0]; TIERS.map.forEach(t => cnt[t]++);
  console.log('Редкость: обычных', cnt[0], '| редких', cnt[1], '| эпических', cnt[2], '| легендарных', cnt[3], '| мифических', cnt[4], '| список частых слов:', TIERS.common ? TIERS.common : 'не загружен');
}
function addWord(p, w, t) {
  const f = p.prof, fresh = !f.w[w]; f.total++; f.w[w] = (f.w[w] || 0) + 1;
  let gain = '';
  if (t && fresh) { // редкости и бусты считаются только за новые для профиля слова
    f.t[t - 1]++; if (t === 4 && f.m.length < 12) f.m.push(w);
    if (t === 5) { f.sec = f.sec || []; if (!f.sec.includes(w)) f.sec.push(w); }
    if ((t === 2 || t === 4 || t === 5) && f.bo.f < 5) { f.bo.f++; gain += 'f'; }
    if (t >= 2 && (f.bo.c || 0) < 5 && Math.random() < .5) { f.bo.c = (f.bo.c || 0) + 1; gain += 'c'; }
    if (t >= 3 && (f.bo.l || 0) < 5 && Math.random() < .5) { f.bo.l = (f.bo.l || 0) + 1; gain += 'l'; }
    if (t >= 3 && f.bo.s < 5) { f.bo.s++; gain += 's'; }
  }
  if (t !== 5 && w.length > f.best.length) f.best = w;
  const ks = Object.keys(f.w); let n = ks.length;
  if (n > 300) for (const k of ks) { if (n <= 250) break; if (f.w[k] === 1) { delete f.w[k]; n--; } }
  dirty.add(p.key);
  return { gain, fresh };
}
// ---------- уровни, лиги, достижения ----------
const LEAGUES = [[0, 'Бронза'], [900, 'Серебро'], [1100, 'Золото'], [1300, 'Платина'], [1500, 'Алмаз'], [1700, 'Легенда']];
const leagueOf = e => { let l = LEAGUES[0][1]; for (const x of LEAGUES) if (e >= x[0]) l = x[1]; return l; };
const xpOf = f => f.total + f.wins * 10 + f.qd * 20 + (f.xpAdj || 0);
const levelOf = f => 1 + Math.floor(xpOf(f) / 100);
const ACH = [
  { id: 'g100', n: 'Ветеран', d: 'Сыграй 100 игр', goal: 100, cur: f => f.games },
  { id: 'w50', n: 'Победитель', d: 'Выиграй 50 игр', goal: 50, cur: f => f.wins },
  { id: 'w200', n: 'Чемпион', d: 'Выиграй 200 игр', goal: 200, cur: f => f.wins },
  { id: 'ws5', n: 'Неудержимый', d: 'Выиграй 5 игр подряд', goal: 5, cur: f => f.bws || 0 },
  { id: 't3000', n: 'Книгоед', d: 'Напиши 3000 слов', goal: 3000, cur: f => f.total },
  { id: 'r100', n: 'Коллекционер', d: 'Напиши 100 редких слов', goal: 100, cur: f => f.t[0] },
  { id: 'e50', n: 'Знаток эпиков', d: 'Напиши 50 эпических слов', goal: 50, cur: f => f.t[1] },
  { id: 'l15', n: 'Легенда слова', d: 'Напиши 15 легендарных слов', goal: 15, cur: f => f.t[2] },
  { id: 'm5', n: 'Мифолог', d: 'Напиши 5 мифических слов', goal: 5, cur: f => f.t[3] },
  { id: 'q50', n: 'Дисциплина', d: 'Выполни 50 заданий дня', goal: 50, cur: f => f.qd },
  { id: 'dia', n: 'Алмазная лига', d: 'Достигни рейтинга 1500', goal: 1500, cur: f => f.elo },
  { id: 'leg', n: 'Легенда рейтинга', d: 'Достигни рейтинга 1700', goal: 1700, cur: f => f.elo },
  { id: 'fr10', n: 'Душа компании', d: 'Добавь 10 друзей', goal: 10, cur: f => f.fr.length },
  { id: 'lv25', n: 'Уровень 25', d: 'Достигни 25 уровня', goal: 25, cur: f => levelOf(f) },
  { id: 'long', n: 'Словесный марафон', d: 'Напиши слово из 15 букв и больше', goal: 15, cur: f => (f.best || '').length },
  { id: 'star', n: 'Звёздный охотник', d: 'Набери 75 очков за партию в «Звёздном счёте»', goal: 75, cur: f => f.bsc || 0 },
  { id: 'sec', n: '???', d: 'Напиши 3 секретных слова', goal: 3, cur: f => (f.sec || []).length, secret: true }
];
const achList = f => ACH.map(a => { const got = f.ac.includes(a.id), hide = a.secret && !got; return [a.n, got ? 1 : 0, hide ? '?????? ? ????????? ?????' : a.d, hide ? 0 : Math.min(a.cur(f), a.goal), hide ? 0 : a.goal]; });
function mailAdd(f, k, x) { f.mail = f.mail || []; f.mail.unshift({ t: Date.now(), k, x }); if (f.mail.length > 25) f.mail.length = 25; }
function checkAch(key) {
  const f = profiles.get(key); if (!f) return;
  const nw = [];
  for (const a of ACH) if (!f.ac.includes(a.id) && a.cur(f) >= a.goal) { f.ac.push(a.id); nw.push(a.n); mailAdd(f, 'ach', a.n); }
  if (nw.length) { dirty.add(key); nw.forEach(n => tellKey(key, { t: 'ach', name: n })); }
}

// ---------- рейтинг (Elo), очередь, таблица лидеров ----------
const lbSet = (key, elo) => { if (KV) up(['ZADD', 'lb', String(elo), key]); };
async function lbTop() {
  let keys = [];
  if (KV) { const r = await up(['ZREVRANGE', 'lb', '0', '9']); keys = Array.isArray(r) ? r : []; }
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
// ---------- грубые слова (без эффектов), формы имён, редкость имён/городов/стран ----------
const NOFX = new Set([...loadLines('vulgar.txt'), ...loadLines('offensive.txt')]);
const BAD_STEM = /^(анус|анал$|пенис|вагин|влагалищ|клитор|мошонк|сперм|эякул|оргазм|порно|порнуха|эрот|секс(?!т)|минет|куннил|эрекц|гениталь|лобк|геморро|проститут|шлюх|стриптиз|мастурб|дрочи|эрогенн)/;
DICT.forEach(w => { if (VULGAR.test(w) || BAD_STEM.test(w)) NOFX.add(w); });
const NAMEGRP = new Map();
try { fs.readFileSync(path.join(__dirname, 'data', 'name_forms.txt'), 'utf8').split(/\r?\n/).forEach((ln, i) => ln.split(',').map(norm).filter(Boolean).forEach(w => NAMEGRP.set(w, 'g' + i))); } catch {}
let FAME = {}; try { FAME = JSON.parse(fs.readFileSync(path.join(__dirname, 'data', 'fame.json'), 'utf8')); } catch {}
const RARE_L = 'фщъцшжэюыйчх';
const hashW = t => { let h = 2166136261; for (let i = 0; i < t.length; i++) { h ^= t.charCodeAt(i); h = Math.imul(h, 16777619); } return h >>> 0; };
function distribute(set, famous) {
  const fam = new Set((famous || []).map(norm)), arr = [...set], m = new Map();
  const score = w => { let v = 0; for (const ch of w) if (RARE_L.includes(ch)) v += 1.2; v += Math.max(0, w.replace(/[ -]/g, '').length - 7) * .55; v += (hashW(w) % 1000) / 1000 * 2.2; return fam.has(w) ? -100 : v; };
  arr.sort((a, b) => score(a) - score(b));
  arr.forEach((w, i) => { const p = i / arr.length; m.set(w, p < .62 ? 0 : p < .82 ? 1 : p < .93 ? 2 : p < .98 ? 3 : 4); });
  return m;
}
const CATTIER = { names: distribute(SETS.names, FAME.names), cities: distribute(SETS.cities, FAME.cities), countries: distribute(SETS.countries, FAME.countries) };
const CATALL = new Map(); Object.values(CATTIER).forEach(mm => mm.forEach((t, w) => { if (t > (CATALL.get(w) || 0)) CATALL.set(w, t); }));
function tier(w, mode) {
  if (SECRETS.has(w)) return 5;
  if (NOFX.has(w)) return 0;
  let d = (TIERS && TIERS.map.get(w)) || 0;
  if (!d && (!mode || usesW(mode)) && !DICT.has(w)) d = w.length >= 14 ? 2 : w.length >= 11 ? 1 : 0;
  if (DICT.has(w)) { const n = w.replace(/[ -]/g, '').length; d = Math.max(d, n >= 18 ? 4 : n >= 15 ? 3 : n >= 13 ? 2 : 0, EXTRA.has(w) ? (n >= 13 ? 3 : n >= 10 ? 2 : 0) : 0); }
  if (!mode || mode === 'words') return d;
  if (mode === 'all') return Math.max(d, CATALL.get(w) || 0);
  if (mode.includes('+')) { let t = usesW(mode) ? d : 0; for (const k of mode.split('+')) if (k !== 'words') t = Math.max(t, (CATTIER[k] && CATTIER[k].get(w)) || 0); return t; }
  return (CATTIER[mode] && CATTIER[mode].get(w)) || 0;
}
// ---------- производные слова и формы одного слова ----------
const COMBO = ['агро', 'авиа', 'авто', 'аква', 'анти', 'био', 'вело', 'видео', 'гео', 'гидро', 'гипер', 'зоо', 'инфра', 'кибер', 'макро', 'мега', 'метео', 'микро', 'мини', 'мото', 'мульти', 'нано', 'нейро', 'пан', 'полу', 'пост', 'прото', 'псевдо', 'радио', 'сейсмо', 'социо', 'супер', 'теле', 'термо', 'техно', 'транс', 'ультра', 'фото', 'цифро', 'электро', 'энерго', 'эко', 'этно', 'нео', 'квази', 'само', 'гос', 'спец', 'мед', 'аэро', 'медиа', 'кино', 'космо', 'нефте', 'газо', 'водо', 'паро', 'хлебо', 'лесо', 'мясо', 'рыбо', 'судо', 'много', 'все', 'видео'];
const VPREF = ['по', 'за', 'на', 'вы', 'при', 'от', 'до', 'про', 'пере', 'раз', 'рас', 'под', 'об', 'с', 'у', 'вос', 'воз', 'ис', 'из', 'недо', 'пред', 'над', 'со', 'во', 'обо', 'подо', 'разо', 'ото'];
function verbOk(w) {
  if (!/(ть|ться|ти|тись|чь|чься)$/.test(w)) return false;
  const b = w.replace(/(ся|сь)$/, '');
  if (VERBSET.has(b)) return true;
  for (const p of VPREF) if (b.startsWith(p) && b.length - p.length >= 4 && VERBSET.has(b.slice(p.length))) return true;
  return false;
}
function nounDerived(w) { for (const p of COMBO) if (w.startsWith(p) && w.length - p.length >= 3 && DICT.has(w.slice(p.length))) return true; return false; }
const derivedOk = w => nounDerived(w) || verbOk(w);
function lemmaKey(w) { if (/ться$/.test(w)) return 'v:' + w.slice(0, -2); if (/ть$/.test(w)) return 'v:' + w; return null; }
function groupUsed(r, w) {
  const g = r.groups; if (!g) return '';
  if (usesN(r.mode) && NAMEGRP.has(w) && g.has(NAMEGRP.get(w))) return 'n';
  if (usesW(r.mode)) { const k = lemmaKey(w); if (k && g.has(k)) return 'v'; }
  return '';
}
function addGroups(r, w) { r.failSet = null; r.groups = r.groups || new Set(); if (NAMEGRP.has(w)) r.groups.add(NAMEGRP.get(w)); const k = lemmaKey(w); if (k) r.groups.add(k); }
// ---------- журнал игр в Filebase ----------
const MODE_RU = { words: 'Слова', cities: 'Города', names: 'Имена', countries: 'Страны', all: 'Все вместе' };
const BASE_MODES = ['words', 'cities', 'names', 'countries'];
function normMode(m) {
  m = String(m || 'words'); if (m === 'all') return 'all';
  const parts = [...new Set(m.split('+'))].filter(x => BASE_MODES.includes(x));
  if (!parts.length) return 'words';
  if (parts.length === BASE_MODES.length) return 'all';
  return BASE_MODES.filter(x => parts.includes(x)).join('+');
}
const usesW = m => m === 'all' || String(m).split('+').includes('words');
const usesN = m => m === 'all' || String(m).split('+').includes('names');
const COMPSET = new Map();
function setOf(mode) {
  if (mode === 'all') return ALL; if (!mode.includes('+')) return SETS[mode];
  let st = COMPSET.get(mode); if (!st) { st = new Set(); mode.split('+').forEach(k => SETS[k].forEach(w => st.add(w))); COMPSET.set(mode, st); } return st;
}
const modeRu = m => m === 'all' ? 'Все вместе' : String(m).split('+').map(x => MODE_RU[x]).join(' + ');
const TIERN = ['', 'редкое', 'эпическое', 'легендарное', 'мифическое', 'СЕКРЕТНОЕ'];
const hhmmss = () => new Date().toLocaleTimeString('ru-RU', { timeZone: 'Europe/Moscow', hour12: false });
const dayStr = () => new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Moscow' });
const who = p => p.bot ? 'бот ' + p.name : p.name + ' (ID ' + ((p.prof && p.prof.uid) || '?') + ')';
function glog(r, text) {
  if (!S3C) return;
  if (!r.log) { r.log = []; r.logKey = 'logs/' + dayStr() + '/' + r.code + '_' + hhmmss().replace(/:/g, '') + '.txt'; }
  r.log.push(hhmmss() + ' ' + text); r.logDirty = true;
}
function glogFlush(r) { if (!r.logDirty || !r.logKey || !S3C) return; r.logDirty = false; S3C.put(r.logKey, Buffer.from(r.log.join('\n') + '\n'), 'text/plain; charset=utf-8').catch(() => { r.logDirty = true; }); }
setInterval(() => rooms.forEach(glogFlush), 30000);
if (process.argv[2] === 'list-tiers') {
  const part = (t, a) => t + ' (' + a.length + '):\n' + a.sort().join(', ') + '\n', L = { 3: [], 4: [] };
  DICT.forEach(w => { if (SECRETS.has(w)) return; const t = tier(w, 'words'); if (t === 3 || t === 4) L[t].push(w); });
  const out = ['МИФИЧЕСКИЕ И ЛЕГЕНДАРНЫЕ СЛОВА', '', part('=== Слова: мифические', L[4]), part('=== Слова: легендарные', L[3])];
  for (const k of ['names', 'cities', 'countries']) { const a3 = [], a4 = []; CATTIER[k].forEach((t, w) => { if (t === 3) a3.push(w); if (t === 4) a4.push(w); }); out.push(part('=== ' + MODE_RU[k] + ': мифические', a4), part('=== ' + MODE_RU[k] + ': легендарные', a3)); }
  fs.writeFileSync(process.argv[3] || 'tiers-list.txt', out.join('\n') + '\n'); process.exit(0);
}
// ---------- боты ----------
const BOT_NAMES = ['Орион', 'Луна', 'Нова', 'Комета', 'Квазар', 'Мира', 'Сириус', 'Астра', 'Вега', 'Зенит', 'Галактика', 'Туманность'];
// know — какую долю словаря «знает» бот, think — пауза на раздумье (мс), cps — мс на букву, miss — шанс «не вспомнить» слово
const BOT_LVL = { easy: { know: 10, think: [2600, 4800], cps: 360, miss: .12, tire: .02 }, mid: { know: 22, think: [1800, 3800], cps: 280, miss: .14, tire: .018 }, hard: { know: 40, think: [1200, 2800], cps: 210, miss: .08, tire: .015 } };
const BYL = new Map();
function botIndex(st) { if (BYL.has(st)) return BYL.get(st); const m = new Map(); st.forEach(w => { const l = w[0]; if (!m.has(l)) m.set(l, []); m.get(l).push(w); }); BYL.set(st, m); return m; }
const hashN = s => { let h = 2166136261; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return h >>> 0; };
function addBots(r, n, lvl) {
  lvl = BOT_LVL[lvl] ? lvl : 'mid';
  const names = BOT_NAMES.slice().sort(() => Math.random() - .5);
  for (let i = 0; i < Math.min(3, Math.max(1, n | 0)); i++) r.players.push({ id: crypto.randomBytes(4).toString('hex'), name: names[i], ws: null, lives: LIVES, alive: true, key: null, prof: null, kick: null, gone: false, bot: true, lvl, seed: crypto.randomBytes(4).toString('hex') });
}
function botTurn(r, p) {
  const cfg = BOT_LVL[p.lvl], stamp = r.turnAt, live = () => r.phase === 'play' && r.players[r.turn] === p && r.turnAt === stamp;
  const idx = botIndex(setOf(r.mode) || DICT), letter = r.letter || [...idx.keys()][Math.floor(Math.random() * idx.size)];
  let cand = (idx.get(letter) || []).filter(w => !r.used.has(w) && !NOFX.has(w) && !groupUsed(r, w) && hashN(p.seed + w) % 100 < cfg.know);
  const missP = Math.min(.55, cfg.miss + cfg.tire * Math.floor(r.used.size / 2)); // бот устаёт: чем дольше игра, тем чаще не вспоминает слово
  if (!cand.length || Math.random() < missP) return; // «не вспомнил слово»: ход сгорит по таймеру
  const pick = () => cand[Math.floor(Math.random() * cand.length)];
  let w = pick();
  if (p.lvl === 'hard') { for (let i = 0; i < 9; i++) { const x = pick(); if (x.length > w.length) w = x; } }
  else if (p.lvl === 'easy') { for (let i = 0; i < 2; i++) { const x = pick(); if (x.length < w.length) w = x; } }
  const typeMs = w.length * cfg.cps, think = Math.min(cfg.think[0] + Math.random() * (cfg.think[1] - cfg.think[0]), Math.max(300, (r.turnMs || TURN_MS) - typeMs - 3000));
  let i = 0;
  const step = () => {
    if (!live()) return; i++; bcast(r, { t: 'typing', name: p.name, w: w.slice(0, i) });
    if (i < w.length) r.btimer = setTimeout(step, cfg.cps * (.6 + Math.random() * .8));
    else r.btimer = setTimeout(() => {
      if (!live() || checkWord(r, w)) return;
      const tr = tier(w, r.mode); r.used.add(w); addGroups(r, w); r.letter = lastLetter(w); r.hist.push({ n: p.name, w, t: tr });
      const pts = r.rules === 'score' ? (SC_PTS[tr] || 1) + (w.length >= 8 ? 1 : 0) : 0; if (pts) p.score = (p.score || 0) + pts;
      trackWord(p, tr);
      glog(r, who(p) + ' написал «' + w + '»' + (tr ? ' [' + TIERN[tr] + ']' : '') + (pts ? ', +' + pts + ' очков' : ''));
      bcast(r, { t: 'ev', k: 'ok', name: p.name, word: w, tier: tr, pts }); startTurn(r);
    }, 350 + Math.random() * 450);
  };
  r.btimer = setTimeout(step, think);
}
function makeRoom(mode, rated) {
  const r = { code: genCode(), players: [], hostId: null, phase: 'lobby', turn: -1, letter: null, used: new Set(), hist: [], deadline: 0, timer: null, winner: null, note: null, last: Date.now(), mode, frz: false, rated };
  rooms.set(r.code, r); return r;
}
function addPlayer(ws, r) {
  const c = ws.ctx, prof = profiles.get(c.key);
  const p = { id: crypto.randomBytes(4).toString('hex'), name: prof.nick, ws, lives: LIVES, alive: true, key: c.key, prof, kick: null, gone: false };
  r.players.push(p); if (!r.hostId) r.hostId = p.id; c.room = r; c.player = p;
  glog(r, r.players.length === 1 ? who(p) + ' создал комнату ' + r.code + ' (' + (r.rated ? 'рейтинговая игра' : 'обычная игра') + ', режим: ' + modeRu(r.mode) + ')' : who(p) + ' вошёл в комнату');
  send(ws, { t: 'joined', id: p.id, inv: { f: prof.bo.f, s: prof.bo.s, c: prof.bo.c || 0, l: prof.bo.l || 0 } });
}
const SC_PTS = [1, 3, 7, 15, 30, 100], SC_ROUNDS = 6;
const RP = [0, 1, 3, 7, 15, 40], CAP_MS = 45 * 60 * 1000;
const rarityPts = x => (x.rc || []).reduce((a, n, i) => a + n * (RP[i] || 0), 0);
const boardOf = r => r.players.map(p => ({ id: p.id, name: p.name, bot: !!p.bot, rc: p.rc || [0, 0, 0, 0, 0, 0], wc: p.wc || 0, pts: rarityPts(p), score: p.score || 0 })).sort((a, b) => b.pts - a.pts || b.wc - a.wc);
function trackWord(p, tr) { p.rc = p.rc || [0, 0, 0, 0, 0, 0]; p.rc[tr] = (p.rc[tr] || 0) + 1; p.wc = (p.wc || 0) + 1; }
function finishCap(r) {
  clearTimeout(r.timer); clearTimeout(r.btimer); clearTimeout(r.dtimer); r.dicing = false;
  const ps = r.players.filter(p => !p.gone), best = Math.max(...ps.map(rarityPts)); let win = ps.filter(p => rarityPts(p) === best);
  if (win.length > 1) { const mw = Math.max(...win.map(p => p.wc || 0)); win = win.filter(p => (p.wc || 0) === mw); }
  r.phase = 'over'; r.winner = win.length === 1 ? win[0].name : null; r.note = 'Прошло 45 минут. Победил тот, кто написал больше редких слов' + (win.length === 1 ? '' : ' (ничья)');
  glog(r, 'прошло 45 минут, игра завершена по редкости слов');
  award(r, win.length === 1 ? win[0] : null); bcast(r, { t: 'ev', k: 'over' }); sync(r);
}
setInterval(() => rooms.forEach(r => { if (r.phase === 'play' && r.startAt && Date.now() - r.startAt >= CAP_MS) finishCap(r); }), 10000);
function finishScore(r) {
  clearTimeout(r.timer); clearTimeout(r.btimer);
  const ps = r.players.filter(p => !p.gone), top = Math.max(...ps.map(p => p.score || 0)), win = ps.filter(p => (p.score || 0) === top);
  r.phase = 'over'; r.winner = win.length === 1 ? win[0].name : null; r.note = win.length === 1 ? null : 'Ничья по очкам';
  award(r, win.length === 1 ? win[0] : null); bcast(r, { t: 'ev', k: 'over' }); sync(r);
}
const MIN_TURN_MS = 8000;
function diceThenTurn(r, why, letter) {
  const alive = r.players.filter(p => p.alive && !p.gone);
  if (alive.length < 2) return startTurn(r);
  let cand = alive; const rounds = [];
  for (let k = 0; k < 6 && cand.length > 1; k++) {
    const rolls = cand.map(p => ({ id: p.id, name: p.name, v: 1 + crypto.randomInt(6) })); rounds.push(rolls);
    const mx = Math.max(...rolls.map(x => x.v)); cand = cand.filter(p => rolls.find(x => x.id === p.id).v === mx);
  }
  const win = cand[Math.floor(Math.random() * cand.length)];
  r.turn = r.players.indexOf(win); r.keepTurn = true; r.dicing = true; r.letter = null; r.mod = null; r.nextMod = null;
  glog(r, (why === 'skip' ? 'буква «' + (letter || '') + '» пропущена, кубик: ' : 'кубик: ') + rounds[rounds.length - 1].map(x => x.name + ' ' + x.v).join(', ') + '. Первым ходит ' + win.name);
  bcast(r, { t: 'ev', k: 'dice', why, letter: letter || null, rounds, winner: win.id, wname: win.name });
  sync(r);
  clearTimeout(r.timer); clearTimeout(r.btimer); clearTimeout(r.dtimer);
  r.dtimer = setTimeout(() => { r.dicing = false; startTurn(r); }, 2200 * rounds.length + 1500);
}
function startGame(r) {
  glog(r, 'игра началась. Режим: ' + modeRu(r.mode) + '. Правила: ' + (r.rules === 'score' ? 'Звёздный счёт' : 'Выживание') + '. Игроки: ' + r.players.map(who).join(', '));
  r.tcount = 0; r.started = false; r.groups = new Set(); r.startAt = Date.now(); r.players.forEach(x => { x.rc = [0, 0, 0, 0, 0, 0]; x.wc = 0; }); r.failSet = null; r.shrunk = 0; r.mod = null; r.nextMod = null; r.keepTurn = false; r.dicing = false; r.players.forEach(x => { x.score = 0; });
  r.players.forEach(x => { x.lives = LIVES; x.alive = true; });
  r.phase = 'play'; r.turn = -1; r.letter = null; r.used.clear(); r.hist = []; r.winner = null; r.note = null;
  bcast(r, { t: 'ev', k: 'start' }); diceThenTurn(r, 'start');
}
function matchmake() {
  for (let i = queue.length - 1; i >= 0; i--) if (queue[i].ws.readyState !== 1 || queue[i].ws.ctx.room) queue.splice(i, 1);
  const now = Date.now(), rm = q => queue.splice(queue.indexOf(q), 1);
  // быстрая игра без рейтинга: любые двое; если никого нет 12 сек и стоит галочка — добавляем ботов
  [...new Set(queue.map(q => q.mode || 'words'))].forEach(md => {
    const cas = queue.filter(q => q.casual && (q.mode || 'words') === md);
    for (let i = 0; i + 1 < cas.length; i += 2) {
      const a = cas[i], b = cas[i + 1]; if (a.key === b.key) continue; rm(a); rm(b);
      const r = makeRoom(md, false); [a, b].forEach(q => { dropKey(q.key, r); addPlayer(q.ws, r); }); startGame(r);
    }
  });
  queue.filter(q => q.casual && q.bots && now - q.since > 12000).forEach(q => {
    rm(q); const r = makeRoom(q.mode || 'words', false); r.bots = true; dropKey(q.key, r); addPlayer(q.ws, r); addBots(r, 2, 'mid'); startGame(r);
  });
  // рейтинговая игра: пары только в одном режиме
  [...new Set(queue.map(q => q.mode || 'words'))].forEach(md => {
    const rk = queue.filter(q => !q.casual && (q.mode || 'words') === md).sort((a, b) => a.elo - b.elo);
    for (let i = 0; i < rk.length - 1; i++) {
      const a = rk[i], b = rk[i + 1], wait = Math.max(now - a.since, now - b.since) / 1000;
      if (a.key === b.key || Math.abs(a.elo - b.elo) > 150 + 20 * wait) continue;
      rm(a); rm(b); rk.splice(i, 2); i--;
      const r = makeRoom(md, true); [a, b].forEach(q => { dropKey(q.key, r); addPlayer(q.ws, r); }); startGame(r);
    }
  });
}
setInterval(matchmake, 2000);

// ---------- друзья ----------
async function keyOfNick(lc) {
  if (nicks.has(lc)) return nicks.get(lc);
  const k = KV ? await up(['GET', 'nk:' + lc]) : null;
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
  for (const k of prof.rq) { const f = await getProfile(k); rq.push({ nick: f.nick, av: avUrl(f), lv: levelOf(f), lg: leagueOf(f.elo) }); }
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
  const tk = nm ? (/^\d{6,9}$/.test(nm) ? await keyOfUid(nm) : await keyOfNick(nm.toLowerCase())) : null;
  if (!tk) return send(ws, { t: 'err', msg: 'Игрок не найден' });
  const tp = await getProfile(tk);
  if (m.t === 'friend_add') {
    if (tk === key) return send(ws, { t: 'err', msg: 'Это ты сам' });
    if (prof.fr.includes(tk)) return send(ws, { t: 'err', msg: 'Он уже у тебя в друзьях' });
    if (prof.fr.length >= 50) return send(ws, { t: 'err', msg: 'Максимум 50 друзей' });
    if (prof.rq.includes(tk)) { link(prof, tp, key, tk); mailAdd(tp, 'friend', prof.nick); send(ws, { t: 'info', msg: 'Теперь вы друзья' }); return view(); }
    if (!tp.rq.includes(key)) {
      if (tp.rq.length >= 30) return send(ws, { t: 'err', msg: 'У игрока слишком много заявок' });
      tp.rq.push(key); dirty.add(tk); tellKey(tk, { t: 'frq', nick: prof.nick });
    }
    return send(ws, { t: 'info', msg: 'Заявка отправлена' });
  }
  if (m.t === 'friend_resp') {
    if (!prof.rq.includes(tk)) return;
    prof.rq = prof.rq.filter(x => x !== tk); dirty.add(key);
    if (m.ok && prof.fr.length < 50 && tp.fr.length < 50) { link(prof, tp, key, tk); mailAdd(tp, 'friend', prof.nick); tellKey(tk, { t: 'info', msg: prof.nick + ' принял заявку в друзья' }); }
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
    if (dq.p[i] >= d.target) { dq.done[i] = 1; newly.push(i); if (f.bo[d.r] < 5) f.bo[d.r]++; mailAdd(f, 'quest', d.text + '|' + d.r); }
  });
  if (!changed) return;
  dirty.add(p.key);
  send(p.ws, { t: 'dq', list: qList(f), newly });
  if (newly.length) { f.qd += newly.length; send(p.ws, { t: 'inv', c: f.bo.c || 0, l: f.bo.l || 0, f: f.bo.f, s: f.bo.s }); }
  checkAch(p.key);
}

function award(r, win) {
  glog(r, 'игра окончена. Победитель: ' + (win ? who(win) : 'нет') + (r.rules === 'score' ? '. Очки: ' + r.players.map(x => x.name + ' ' + (x.score || 0)).join(', ') : '') + (r.note ? '. ' + r.note : '')); glog(r, 'итоги: ' + boardOf(r).map((x, i) => (i + 1) + '. ' + x.name + ' — слов ' + x.wc + ' (редких ' + x.rc[1] + ', эпических ' + x.rc[2] + ', легендарных ' + x.rc[3] + ', мифических ' + x.rc[4] + ', секретных ' + x.rc[5] + ')').join('; ') + '. Всего слов в игре: ' + r.used.size); glogFlush(r);
  if (r.bots) return; // игры с ботами не дают рейтинг, победы и задания
  if (r.rated) rate(r, win);
  if (r.hist.length < 4) return; // слишком короткие игры не идут в статистику (защита от накрутки)
  r.players.forEach(x => { x.prof.games++; x.prof.bws = x === win ? (x.prof.bws || 0) + 1 : 0; if (r.rules === 'score') x.prof.bsc = Math.max(x.prof.bsc || 0, x.score || 0); dirty.add(x.key); });
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
const ASSETS = {};
[['manifest.webmanifest', 'application/manifest+json; charset=utf-8'], ['icon-192.png', 'image/png'], ['icon-512.png', 'image/png'], ['apple-touch-icon.png', 'image/png']].forEach(([f, t]) => { try { ASSETS['/' + f] = [t, fs.readFileSync(path.join(__dirname, 'public', f))]; } catch {} });
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
  if (req.method === 'GET' && ASSETS[pth]) { res.writeHead(200, { 'Content-Type': ASSETS[pth][0], 'Cache-Control': 'public, max-age=86400', 'X-Content-Type-Options': 'nosniff' }); return res.end(ASSETS[pth][1]); }
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
      'Cache-Control': 'no-cache',
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
  letter: r.letter, left: r.dicing ? (r.turnMs || TURN_MS) : Math.max(0, r.deadline - Date.now()), total: (r.turnMs || TURN_MS) + (r.frz ? 10000 : 0), dicing: !!r.dicing, mod: r.mod || null, capAt: r.phase === 'play' && r.startAt ? r.startAt + CAP_MS : null, capLeft: r.phase === 'play' && r.startAt ? Math.max(0, r.startAt + CAP_MS - Date.now()) : null, rounds: r.rounds || SC_ROUNDS, words: r.used.size, board: r.phase === 'over' ? boardOf(r) : null, mode: r.mode, rated: !!r.rated,
  count: r.used.size, rules: r.rules || 'survival', tleft: r.rules === 'score' ? Math.max(0, r.players.filter(p => !p.gone).length * (r.rounds || SC_ROUNDS) - (r.tcount || 0)) : null, hist: r.hist.slice(-6), winner: r.winner, note: r.note,
  players: r.players.map(p => ({ id: p.id, name: p.name, lives: p.lives, alive: p.alive, off: !p.ws && !p.bot, bot: !!p.bot, role: p.prof ? (p.prof.role || '') : '', score: p.score || 0, av: p.prof ? avUrl(p.prof) : '' }))
});
const sync = r => bcast(r, pub(r));

function genCode() { let c; do { c = String(crypto.randomInt(10000, 100000)); } while (rooms.has(c)); return c; }
function cleanName(s) {
  if (typeof s !== 'string') return null;
  s = s.trim().replace(/\s+/g, ' ');
  return /^[\p{L}\p{N}_ -]{2,16}$/u.test(s) ? s : null;
}

// определяем часть речи неизвестного слова, чтобы подсказать игроку
const ADJ_RE = /(ый|ая|ое|ые|ую|ого|ому|ым|ых|ыми|ейший|ейшая|ейшее|кий|гий|хий|жий|ший|чий|щий)$/;
function verbCandidates(w) {
  const base = w.replace(/(ся|сь)$/, ''), rf = base !== w ? 'ся' : '', out = [], add = (st, ends) => ends.forEach(e => out.push(st + e + rf));
  const rules = [[/^(.+)(аешь|аете|аем|ает|ают|аю|ал|ала|али|ало)$/, ['ать']], [/^(.+)(яешь|яете|яем|яет|яют|яю|ял|яла|яли|яло)$/, ['ять']],
    [/^(.+)(еешь|еете|еем|еет|еют|ею|ел|ела|ели|ело)$/, ['еть']], [/^(.+)(ишь|ите|им|ит|ят|ил|ила|или|ило)$/, ['ить', 'еть']],
    [/^(.+[^аеиоуыэюяь])ю$/, ['ить', 'еть']], [/^(.+)(уешь|уете|уем|ует|уют|ую)$/, ['овать', 'евать', 'уть']], [/^(.+)(овал|овала|овали|овало)$/, ['овать']],
    [/^(.+)(ул|ула|ули|уло)$/, ['уть']], [/^(.+)(ыл|ыла|ыли|ыло)$/, ['ыть']]];
  rules.forEach(([re, ends]) => { const m = re.exec(base); if (m) add(m[1], ends); });
  return out;
}
function wordKind(w) {
  if (w.length < 4 || /[ -]/.test(w)) return null;
  if (verbCandidates(w).some(c => VERBSET.has(c))) return 'глагол';
  if (/([аяеиуыо]ть|ться|йти|чь|чься)$/.test(w)) return 'глагол';
  if (w.length >= 5 && ADJ_RE.test(w)) return 'прилагательное';
  return null;
}
function checkWord(r, w) {
  if (r.mod === 'long' && w.replace(/[ -]/g, '').length < 7) return 'Тяжёлое слово: нужно слово от 7 букв';
  if (w.length < 2 || w.length > 40 || !/^[а-я]+([ -][а-я]+)*$/.test(w)) return 'Только русские буквы, минимум 2';
  if (r.letter && w[0] !== r.letter) return `Слово должно начинаться на «${r.letter.toUpperCase()}»`;
  if (r.used.has(w)) return 'Это слово уже было';
  { const gu = groupUsed(r, w); if (gu) return gu === 'n' ? 'Другая форма этого имени уже была использована' : 'Другая форма этого слова уже была использована'; }
  const st = setOf(r.mode);
  if (st && !st.has(w) && !(usesW(r.mode) && derivedOk(w))) { if (usesW(r.mode)) { const k = wordKind(w); if (k) return 'Это ' + k + ', а не существительное'; } }
  if (st && !st.has(w) && !(usesW(r.mode) && derivedOk(w))) return r.mode === 'words' ? 'Такого слова нет в словаре' : 'Такого нет в категории «' + modeRu(r.mode) + '»';
  return null;
}

function armTimer(r) {
  clearTimeout(r.timer);
  const cur = r.players[r.turn];
  r.timer = setTimeout(() => {
    if (r.rules !== 'score') { cur.lives--; if (cur.lives <= 0) cur.alive = false; }
    glog(r, who(cur) + ' не успел' + (r.rules !== 'score' ? ', осталось жизней: ' + Math.max(0, cur.lives) : ''));
    bcast(r, { t: 'ev', k: 'timeout', name: cur.name });
    // если все живые игроки подряд не ответили на букву, буква пропускается и снова кидается кубик
    if (!r.failSet || !r.failSet.size) { r.failSet = new Set(); r.failNeed = r.players.filter(p => !p.gone && (p.alive || p === cur)).length; }
    r.failSet.add(cur.id);
    if (r.letter && r.failSet.size >= r.failNeed && r.players.filter(p => p.alive).length >= 2) { const L = r.letter; r.failSet = null; bcast(r, { t: 'ev', k: 'skipletter', letter: L }); return diceThenTurn(r, 'skip', L); }
    startTurn(r);
  }, Math.max(0, r.deadline - Date.now()));
}

function startTurn(r) {
  clearTimeout(r.timer); clearTimeout(r.btimer); clearTimeout(r.dtimer); r.dicing = false;
  const alive = r.players.filter(p => p.alive);
  if (alive.length < 2) {
    r.phase = 'over'; r.winner = alive[0] ? alive[0].name : null;
    award(r, alive[0]); bcast(r, { t: 'ev', k: 'over' }); return sync(r);
  }
  if (r.rules === 'score') { if (r.started) r.tcount++; r.started = true; if (r.tcount >= r.players.filter(p => !p.gone).length * (r.rounds || SC_ROUNDS)) return finishScore(r); }
  if (r.keepTurn) r.keepTurn = false; else do { r.turn = (r.turn + 1) % r.players.length; } while (!r.players[r.turn].alive);
  // время хода сокращается на 2 секунды каждые 10 слов; усложнение от соперника действует на один ход
  const n10 = Math.floor(r.used.size / 10);
  r.turnMs = Math.max(MIN_TURN_MS, TURN_MS - 2000 * n10);
  if (n10 > (r.shrunk || 0)) { r.shrunk = n10; bcast(r, { t: 'ev', k: 'shrink', sec: Math.round(r.turnMs / 1000) }); }
  const nm = r.nextMod; r.nextMod = null; r.mod = nm ? nm.k : null;
  if (r.mod === 'short') r.turnMs = Math.min(r.turnMs, 7000);
  r.deadline = Date.now() + r.turnMs; r.turnAt = Date.now(); r.frz = false;
  armTimer(r);
  sync(r);
  const cp = r.players[r.turn]; if (cp && cp.bot) botTurn(r, cp);
}

function attach(ws, r, p) {
  if (p.ws && p.ws !== ws) { p.ws.ctx.room = null; p.ws.ctx.player = null; p.ws.close(); }
  clearTimeout(p.kick); p.kick = null; p.ws = ws; ws.ctx.room = r; ws.ctx.player = p;
  send(ws, { t: 'joined', id: p.id, inv: { f: p.prof.bo.f, s: p.prof.bo.s, c: p.prof.bo.c || 0, l: p.prof.bo.l || 0 } });
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
  p.gone = true; glog(r, who(p) + ' вышел');
  if (r.phase === 'lobby') r.players = r.players.filter(x => x !== p); else p.alive = false;
  const pending = r.players.filter(x => !x.gone && (x.ws || x.kick));
  if (!pending.length) { glog(r, 'комната закрыта'); glogFlush(r); clearTimeout(r.timer); clearTimeout(r.btimer); clearTimeout(r.dtimer); rooms.delete(r.code); return; }
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

const pwMem = new Map(), pwFail = new Map();
const pwHash = (p, salt) => crypto.scryptSync(p, salt, 32).toString('hex');
async function pwGet(lc) { if (KV) { const v = await up(['GET', 'pw:' + lc]); if (v) { try { return JSON.parse(v); } catch { return null; } } return pwMem.get(lc) || null; } return pwMem.get(lc) || null; }
async function pwSet(lc, obj, nx) { const v = JSON.stringify(obj); if (KV) { const r = await up(['SET', 'pw:' + lc, v, ...(nx ? ['NX'] : [])]); return r === 'OK'; } if (nx && pwMem.has(lc)) return false; pwMem.set(lc, obj); return true; }
function pwCheck(p, e) { try { return crypto.timingSafeEqual(Buffer.from(pwHash(p, e.s), 'hex'), Buffer.from(e.h, 'hex')); } catch { return false; } }
async function finishAuth(ws, key, sess, nick, email, how) {
  const c = ws.ctx;
  c.key = key;
  (online.get(key) || online.set(key, new Set()).get(key)).add(ws);
  const prof = await getProfile(key);
  await ensureUid(key, prof);
  if (email) { const eh = sh('em:' + email); if (prof.eh !== eh) { prof.eh = eh; dirty.add(key); } if (prof.email !== email) { prof.email = email; dirty.add(key); } }
  if (how && prof.lt !== how) { prof.lt = how; dirty.add(key); }
  if (((email && CREATOR_EMAILS.includes(email)) || (prof.eh && CREATOR_HASHES.includes(prof.eh)) || CREATOR_IDS.includes(prof.uid)) && prof.role !== 'creator') { prof.role = 'creator'; dirty.add(key); }
  await applyAdmin(key, prof);
  console.log('вход:', prof.nick, '| ID', prof.uid, '| роль:', prof.role || '-', '| почта из Google:', email ? 'получена' : 'нет (вход по сессии)', '| CREATOR_EMAIL задан:', CREATOR_EMAILS.length > 0, '| Filebase:', !!S3C);
  if (isBanned(prof)) { send(ws, { t: 'banned', msg: banMsg(prof) }); return ws.close(1008); }
  return send(ws, { t: 'authed', uid: prof.uid, role: prof.role || '', sess, nick: prof.nick, suggest: nick.slice(0, 16), modes: MODES_AVAIL, inv: { f: prof.bo.f, s: prof.bo.s, c: prof.bo.c || 0, l: prof.bo.l || 0 }, dq: qList(prof), av: avUrl(prof), elo: prof.elo, lg: leagueOf(prof.elo), lv: levelOf(prof) });
}
async function handle(ws, m) {
  const c = ws.ctx;
  if (m.t === 'reg' || m.t === 'login') {
    if (!hit(authRate, c.ip, 20, 60000)) return send(ws, { t: 'err', msg: 'Слишком часто, подожди минуту' });
    const nk = cleanName(m.nick), pass = typeof m.pass === 'string' ? m.pass : '';
    if (!nk) return send(ws, { t: 'err', msg: 'Ник: 2–16 символов, буквы, цифры, пробел, _ или -' });
    if (pass.length < 6 || pass.length > 64) return send(ws, { t: 'err', msg: 'Пароль: от 6 до 64 символов' });
    const lc = nk.toLowerCase();
    if (m.t === 'reg') {
      if (/^\d+$/.test(nk) || VULGAR.test(lc)) return send(ws, { t: 'err', msg: 'Этот ник нельзя использовать' });
      if (await pwGet(lc)) return send(ws, { t: 'err', msg: 'Этот ник уже занят' });
      const key = gkey('pw:' + lc), rs = await reserveNick(lc, key);
      if (rs === 'taken') return send(ws, { t: 'err', msg: 'Этот ник уже занят' });
      if (rs === 'err') return send(ws, { t: 'err', msg: 'Не удалось зарегистрироваться, попробуй ещё раз' });
      const salt = crypto.randomBytes(16).toString('hex');
      if (!(await pwSet(lc, { s: salt, h: pwHash(pass, salt), k: key }, true))) return send(ws, { t: 'err', msg: 'Этот ник уже занят' });
      const prof = await getProfile(key); prof.nick = nk; prof.lt = 'pass'; dirty.add(key);
      const sess = crypto.randomBytes(16).toString('hex'); await saveSession(sess, key);
      return finishAuth(ws, key, sess, nk, '', 'pass');
    }
    if (!hit(pwFail, lc, 8, 600000)) return send(ws, { t: 'err', msg: 'Слишком много попыток входа, подожди 10 минут' });
    const e = await pwGet(lc);
    if (!e || !pwCheck(pass, e)) return send(ws, { t: 'err', msg: 'Неверный ник или пароль' });
    const sess = crypto.randomBytes(16).toString('hex'); await saveSession(sess, e.k);
    return finishAuth(ws, e.k, sess, nk, '', 'pass');
  }
  if (m.t === 'join' && typeof m.code === 'string') { const rr = rooms.get(m.code.replace(/\D/g, '')); if (rr && rr.kicked && c.key && rr.kicked.has(c.key)) return send(ws, { t: 'err', msg: 'Тебя исключили из этой комнаты' }); }
  if (m.t === 'admban' || m.t === 'admunban') {
    const me = c.key && profiles.get(c.key);
    if (!me || me.role !== 'creator') return send(ws, { t: 'err', msg: 'Нет прав' });
    const tk = await keyOfUid(String(m.uid || '').trim());
    if (!tk) return send(ws, { t: 'err', msg: 'Игрок не найден' });
    const f = await getProfile(tk);
    if (f.role === 'creator') return send(ws, { t: 'err', msg: 'Создателя нельзя банить' });
    if (m.t === 'admunban') { f.banUntil = 0; f.banReason = ''; }
    else { const d = Math.max(0, Math.min(3650, Number(m.days) || 0)); f.banUntil = d === 0 ? -1 : Date.now() + d * 86400000; f.banReason = String(m.reason || '').slice(0, 200); f.banBy = me.nick; }
    dirty.add(tk); syncPlayerFile(tk, f, true);
    if (isBanned(f)) [...(online.get(tk) || [])].forEach(w => { send(w, { t: 'banned', msg: banMsg(f) }); try { w.close(1008); } catch {} });
    console.log('действие создателя:', me.nick, m.t, 'ID', f.uid, f.banReason || '');
    return send(ws, { t: 'info', msg: m.t === 'admunban' ? 'Бан снят' : 'Игрок забанен' });
  }
  if (m.t === 'mystat') {
    if (!c.key) return;
    const f = await getProfile(c.key), xp = xpOf(f), lv = levelOf(f);
    return send(ws, { t: 'mystat', lv, xp, a: 100 * (lv - 1), b: 100 * lv, frc: f.fr.length, role: f.role || '', unread: f.mail.filter(x => x.t > f.mseen).length + f.rq.length });
  }
  if (m.t === 'mail_get') {
    if (!c.key) return;
    const f = await getProfile(c.key), seen = f.mseen, fv = await friendView(f);
    const items = f.mail.map(x => ({ t: x.t, k: x.k, x: x.x, n: x.t > seen ? 1 : 0 }));
    f.mseen = Date.now(); dirty.add(c.key);
    return send(ws, { t: 'mail', items, rq: fv.rq });
  }
  if (m.t === 'userprofile') {
    if (!c.key) return;
    const nm = String(m.nick || '').trim(), tk = /^\d{6,9}$/.test(nm) ? await keyOfUid(nm) : await keyOfNick(nm.toLowerCase());
    if (!tk) return send(ws, { t: 'err', msg: 'Игрок не найден' });
    const f = await getProfile(tk), top = Object.entries(f.w).sort((a, b) => b[1] - a[1]).filter(([w]) => !SECRETS.has(w)).slice(0, 3).map(([w, n]) => [w, n, tier(w)]);
    return send(ws, { t: 'profile', id: -1, name: f.nick, games: f.games, wins: f.wins, total: f.total, tiers: f.t, best: SECRETS.has(f.best) ? '' : f.best, top, myth: f.m, lv: levelOf(f), elo: f.elo, lg: leagueOf(f.elo), rg: f.rg, reg: f.reg, frc: f.fr.length, uid: f.uid, role: f.role || '', ...profExtra(c, f), av: avUrl(f), ach: achList(f) });
  }
  if (m.t === 'myprofile') { // свой профиль из меню, без комнаты
    if (!c.key) return;
    const f = await getProfile(c.key), top = Object.entries(f.w).sort((a, b) => b[1] - a[1]).filter(([w]) => !SECRETS.has(w)).slice(0, 3).map(([w, n]) => [w, n, tier(w)]);
    return send(ws, { t: 'profile', id: -1, mine: true, name: f.nick, games: f.games, wins: f.wins, total: f.total, tiers: f.t, best: SECRETS.has(f.best) ? '' : f.best, top, myth: f.m, lv: levelOf(f), elo: f.elo, lg: leagueOf(f.elo), rg: f.rg, reg: f.reg, frc: f.fr.length, uid: f.uid, role: f.role || '', ...profExtra(c, f), av: avUrl(f), ach: achList(f) });
  }
  if (m.t === 'auth') {
    if (!hit(authRate, c.ip, 20, 60000)) return send(ws, { t: 'err', msg: 'Слишком часто, подожди минуту' });
    let key = null, sess = null, nick = '', email = '';
    if (typeof m.cred === 'string' && m.cred.length < 4096 && oc) {
      try {
        const pl = (await oc.verifyIdToken({ idToken: m.cred, audience: GID })).getPayload();
        key = gkey(pl.sub); nick = pl.given_name || pl.name || ''; email = pl.email_verified && pl.email ? String(pl.email).toLowerCase() : '';
        sess = crypto.randomBytes(16).toString('hex'); await saveSession(sess, key);
      } catch { return send(ws, { t: 'autherr' }); }
    } else if (typeof m.sess === 'string' && /^[a-f0-9]{32}$/.test(m.sess)) {
      key = await loadSession(m.sess);
      if (!key) return send(ws, { t: 'autherr' });
    } else return send(ws, { t: 'autherr' });
    return finishAuth(ws, key, sess, nick, email, 'google');
  }
  if (m.t === 'setnick') {
    if (!c.key || c.room) return send(ws, { t: 'err', msg: 'Ник нельзя менять в комнате' });
    const nk = cleanName(m.nick);
    if (!nk || /^\d+$/.test(nk) || VULGAR.test(nk.toLowerCase())) return send(ws, { t: 'err', msg: 'Ник: 2-16 символов, буквы, цифры, пробел, _ -' });
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
  if (m.t === 'botgame') {
    if (!c.key || c.room) return;
    const f = await getProfile(c.key);
    if (!f.nick) return send(ws, { t: 'err', msg: 'Сначала выбери ник' });
    if (rooms.size >= MAX_ROOMS) return send(ws, { t: 'err', msg: 'Сервер перегружен, попробуй позже' });
    if (!hit(createRate, c.key, 20, 600000)) return send(ws, { t: 'err', msg: 'Слишком часто, подожди немного' });
    const bmode = normMode(m.mode); qRemove(ws); const r = makeRoom(bmode, false); r.bots = true; dropKey(c.key, r); addPlayer(ws, r);
    r.rules = m.rules === 'score' ? 'score' : 'survival'; if ([6, 10, 15, 20, 30].includes(Number(m.rounds))) r.rounds = Number(m.rounds); addBots(r, +m.n, m.lvl); glog(r, 'добавлено ботов: ' + (r.players.length - 1) + ' (уровень: ' + (m.lvl || 'mid') + ')'); return startGame(r);
  }
  if (m.t === 'queue') {
    if (!c.key || c.room || queue.some(q => q.ws === ws)) return;
    const f = await getProfile(c.key);
    if (!f.nick) return send(ws, { t: 'err', msg: 'Сначала выбери ник' });
    queue.push({ ws, key: c.key, elo: f.elo, since: Date.now(), casual: !!m.casual, bots: !!m.bots, mode: normMode(m.mode) });
    return send(ws, { t: 'queued', casual: !!m.casual, bots: !!m.bots });
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
    send(ws, { t: 'joined', id: p.id, inv: { f: p.prof.bo.f, s: p.prof.bo.s, c: p.prof.bo.c || 0, l: p.prof.bo.l || 0 } }); sync(r);
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
  if (m.t === 'kick') {
    if (p.id !== r.hostId) return;
    const tp = r.players.find(x => x.id === m.id && !x.gone); if (!tp || tp === p) return;
    glog(r, who(p) + ' исключил игрока ' + who(tp));
    if (tp.key) (r.kicked || (r.kicked = new Set())).add(tp.key);
    if (tp.ws) { send(tp.ws, { t: 'kicked' }); tp.ws.ctx.room = null; tp.ws.ctx.player = null; tp.ws = null; }
    drop(r, tp); return;
  }
  if (m.t === 'rounds') { if (p.id === r.hostId && r.phase === 'lobby' && r.rules === 'score' && [6, 10, 15, 20, 30].includes(Number(m.n))) { r.rounds = Number(m.n); glog(r, who(p) + ' выбрал слов на игрока: ' + r.rounds); sync(r); } return; }
  if (m.t === 'rules') { if (p.id === r.hostId && r.phase === 'lobby' && !r.rated) { r.rules = m.rules === 'score' ? 'score' : 'survival'; glog(r, who(p) + ' выбрал правила: ' + (r.rules === 'score' ? 'Звёздный счёт' : 'Выживание')); sync(r); } return; }
  if (m.t === 'mode') {
    if (p.id === r.hostId && r.phase === 'lobby' && typeof m.mode === 'string') { r.mode = normMode(m.mode); glog(r, who(p) + ' выбрал режим: ' + modeRu(r.mode)); sync(r); }
    return;
  }
  if (m.t === 'react') {
    if (!['😂', '💀', '🔥', '😈', '🗿', '🤡'].includes(m.e) || Date.now() - (p.rt || 0) < 400) return;
    p.rt = Date.now(); return bcast(r, { t: 'ev', k: 'react', name: p.name, e: m.e });
  }
  if (m.t === 'boost') {
    if (r.phase !== 'play' || r.dicing || r.players[r.turn] !== p) return;
    if (r.rated) return send(ws, { t: 'err', msg: 'В рейтинговой игре бусты отключены' });
    const b = p.prof.bo;
    if (m.k === 'freeze') {
      if (b.f < 1) return send(ws, { t: 'err', msg: 'Нет заморозок' });
      if (r.frz) return send(ws, { t: 'err', msg: 'Заморозку можно один раз за ход' });
      b.f--; r.frz = true; r.deadline += 10000; armTimer(r);
    } else if (m.k === 'skip') {
      if (b.s < 1) return send(ws, { t: 'err', msg: 'Нет пропусков' });
      b.s--;
    } else if (m.k === 'short' || m.k === 'long') {
      const key = m.k === 'short' ? 'c' : 'l';
      if ((b[key] || 0) < 1) return send(ws, { t: 'err', msg: 'Нет такой способности' });
      if (r.nextMod) return send(ws, { t: 'err', msg: 'Следующий игрок уже получил усложнение' });
      b[key]--; r.nextMod = { k: m.k, by: p.name };
    } else return;
    dirty.add(p.key);
    send(ws, { t: 'inv', c: b.c || 0, l: b.l || 0, f: b.f, s: b.s });
    glog(r, who(p) + ' использовал ' + ({ freeze: 'заморозку', skip: 'пропуск хода', short: '«Сжать время»', long: '«Тяжёлое слово»' }[m.k]));
    bcast(r, { t: 'ev', k: 'boost', name: p.name, b: m.k });
    if (m.k === 'skip') return startTurn(r);
    return sync(r);
  }
  if (m.t === 'quit') { c.room = null; c.player = null; p.ws = null; drop(r, p); return; }
  if (m.t === 'profile') {
    const x = r.players.find(y => y.id === m.id); if (!x) return;
    if (x.bot) return send(ws, { t: 'info', msg: ':bot: ' + x.name + ' — бот' }); const f = x.prof, top = Object.entries(f.w).sort((a, b) => b[1] - a[1]).filter(([w]) => !SECRETS.has(w)).slice(0, 3).map(([w, n]) => [w, n, tier(w)]);
    return send(ws, { t: 'profile', id: x.id, name: x.name, games: f.games, wins: f.wins, total: f.total, tiers: f.t, best: SECRETS.has(f.best) ? '' : f.best, top, myth: f.m, lv: levelOf(f), elo: f.elo, lg: leagueOf(f.elo), rg: f.rg, reg: f.reg, frc: f.fr.length, uid: f.uid, role: f.role || '', ...profExtra(c, f), av: avUrl(f), ach: achList(f) });
  }
  if (m.t === 'start' || m.t === 'again') {
    if (p.id !== r.hostId || r.rated) return;
    if (m.t === 'start' && r.phase === 'lobby') {
      if (r.players.length < 2) return send(ws, { t: 'err', msg: 'Нужно минимум 2 игрока' });
      startGame(r);
    } else if (m.t === 'again' && r.phase === 'over') {
      r.players = r.players.filter(x => !x.gone); r.phase = 'lobby'; sync(r);
    }
    return;
  }
  if (m.t === 'word') {
    if (r.phase !== 'play' || r.dicing || r.players[r.turn].id !== p.id || typeof m.w !== 'string' || m.w.length > 40) return;
    // анти-бот: человек не отвечает быстрее ~0.45 с после начала хода; повторы подряд блокируем
    const since = Date.now() - (r.turnAt || 0);
    if (since < 450) { p.fast = (p.fast || 0) + 1; if (p.fast >= 4) return ws.close(1008); return; }
    if (Date.now() - (p.lastW || 0) < 300) return;
    p.lastW = Date.now();
    const w = norm(m.w), bad = checkWord(r, w);
    if (bad) { p.bad = (p.bad || 0) + 1; if (p.bad > 40) return ws.close(1008); if (/^Это (глагол|прилагательное)/.test(bad)) { p.nn = (p.nn || 0) + 1; if (p.nn % 3 === 0) return send(ws, { t: 'err', msg: 'Пользователь, в каком вы классе? :laugh:' }); }
    return send(ws, { t: 'err', msg: bad }); }
    const tr = tier(w, r.mode), aw = addWord(p, w, tr), gain = aw.gain;
    addGroups(r, w);
    const pts = r.rules === 'score' ? (SC_PTS[tr] || 1) + (w.length >= 8 ? 1 : 0) : 0; if (pts) p.score = (p.score || 0) + pts;
    r.used.add(w); r.letter = lastLetter(w); r.hist.push({ n: p.name, w, t: tr });
    trackWord(p, tr);
    glog(r, who(p) + ' написал «' + w + '»' + (tr ? ' [' + TIERN[tr] + ']' : '') + (pts ? ', +' + pts + ' очков' : ''));
    bcast(r, { t: 'ev', k: 'ok', name: p.name, word: w, tier: tr, pts });
    startTurn(r);
    if (gain) send(ws, { t: 'inv', c: p.prof.bo.c || 0, l: p.prof.bo.l || 0, f: p.prof.bo.f, s: p.prof.bo.s, gain });
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
// Render не засыпает, пока сервер сам обращается к своему публичному адресу
if (process.env.RENDER_EXTERNAL_URL) setInterval(() => { fetch(process.env.RENDER_EXTERNAL_URL + '/ping').catch(() => {}); }, 9 * 60 * 1000);
