'use strict';
// Редкость слов считается один раз при старте сервера, по самому словарю (words.txt).
// 0 обычное, 1 редкое, 2 эпическое, 3 легендарное, 4 мифическое.
// Логика: сначала "узнаваемость" (basic.txt и common.txt всегда обычные), остальные ранжируются
// по сложности: словообразование (приставки, суффиксы), научные корни, составные слова,
// редкие буквы и необычные сочетания букв. Длина влияет только у очень длинных слов.
const fs = require('fs'), path = require('path');
const norm = s => String(s).trim().toLowerCase().replace(/ё/g, 'е');
const loadList = name => {
  try { return new Set(fs.readFileSync(path.join(__dirname, name), 'utf8').split(/\s+/).map(norm).filter(Boolean)); } catch { return new Set(); }
};

const TECH_STRONG = /(логия|графия|метрия|скопия|фобия|филия|кратия|архия|номия|патия|терапия|генез|генезис|плазия|трофия|эктомия|остомия|фагия|мания|плазма|форез)$/;
const TECH_MID = /(ограф|метр|скоп|граф|логист)$/;
const TECH_MED = /(изм|ация|ификация|анс|аж|оид|аза|оз|ит|ол|ид|ат|ий|ема|ома|ика|ент|ант|ор|ер|ур|ум|ус|ез|ос)$/;
const PREF = /^(пере|раз|рас|под|над|пред|недо|само|полу|много|противо|псевдо|квази|анти|контр|меж|сверх|супер|гипер|гипо|микро|макро|мега|пост|транс|экс|интер|ультра|архи|обез|обес|воз|вос|низ|нис|пре|без|бес|взо|вне|около|пара|мета|мульти|нео|орто|поли|моно|био|гео|гидро|электро|термо|фото|нефте|водо|лесо|зоо|фило|психо|социо|авто|аэро|радио|тепло|горе|газо|паро|звуко|свето|кино|стале|нефть|хлебо|земле|судо|тракто)/;
const SUFF = /(ание|ение|ивание|ование|ирование|ыванье|ность|ость|ство|тель|щик|чик|ница|ушка|ишка|овка|евка|ировка|щина|изна|ота|атор|итор|ация|изация|фикация|ик|ка|ок|ек|ец|ье|ие)$/;
const DEEP = /(ировани|ивани|ыванье|изаци|ификаци|ователь|ательств|овательн|ност|остн)/;

function buildTiers(words) {
  const set = new Set(words);
  const basic = loadList('basic.txt'), common = loadList('common.txt');
  const tri = new Map(), bi = new Map();
  for (const w of words) {
    const s = '^^' + w + '$';
    for (let i = 2; i < s.length; i++) {
      const k2 = s.slice(i - 2, i), k3 = s.slice(i - 2, i + 1);
      bi.set(k2, (bi.get(k2) || 0) + 1); tri.set(k3, (tri.get(k3) || 0) + 1);
    }
  }
  const odd = w => {
    const s = '^^' + w + '$'; let t = 0;
    for (let i = 2; i < s.length; i++) {
      const k2 = s.slice(i - 2, i), k3 = s.slice(i - 2, i + 1);
      t -= Math.log(((tri.get(k3) || 0) + .1) / ((bi.get(k2) || 0) + 3.3));
    }
    return t / (s.length - 2);
  };
  const endings = ['', 'а', 'я', 'ь', 'о', 'е', 'ия'];
  const compound = w => {
    if (w.length < 8) return false;
    for (let i = 3; i <= w.length - 4; i++) {
      if (!set.has(w.slice(i))) continue;
      let left = w.slice(0, i); if (/[ое]$/.test(left)) left = left.slice(0, -1);
      if (left.length >= 3 && endings.some(e => set.has(left + e))) return true;
    }
    return false;
  };
  // необычность сочетаний букв считаем внутри своей длины, чтобы короткие слова не штрафовались за краткость
  const byLen = new Map();
  const raw = words.map(w => { const o = odd(w), L = Math.min(w.length, 16); (byLen.get(L) || byLen.set(L, []).get(L)).push(o); return o; });
  const st = new Map(); byLen.forEach((a, L) => { const m = a.reduce((x, y) => x + y, 0) / a.length; st.set(L, [m, Math.sqrt(a.reduce((x, y) => x + (y - m) ** 2, 0) / a.length) || 1]); });
  const rows = [];
  words.forEach((w, i) => {
    const [m, sd] = st.get(Math.min(w.length, 16));
    const z = Math.max(-1, Math.min(3, (raw[i] - m) / sd));
    let c = .55 * z;
    if (TECH_STRONG.test(w)) c += 1.4; else if (w.length >= 8 && TECH_MID.test(w)) c += .8; else if (w.length >= 8 && TECH_MED.test(w)) c += .35;
    const pref = PREF.test(w), suff = SUFF.test(w);
    if (pref && suff) c += .7; else if (pref && w.length >= 8) c += .4;
    if (DEEP.test(w)) c += .6;
    if (compound(w)) c += .5;
    for (const ch of w) if ('щъэфю'.includes(ch)) c += .45;
    c += Math.min(3, Math.max(0, w.length - 11) * .3);
    if (w.includes('-')) c += .2;
    rows.push({ w, c, known: basic.has(w) || common.has(w) });
  });
  const N = rows.length, rest = rows.filter(r => !r.known).sort((a, b) => a.c - b.c);
  const L = N - rest.length, listed = common.size >= 1000; // если список частых слов загрузился, обычными считаем в основном его слова
  const share = listed ? [.15, .42, .27, .12] : [.25, .38, .24, .10]; // доли словаря: обычные, редкие, эпические, легендарные; остаток мифические
  const cut = [Math.max(0, Math.round(share[0] * N) - L)];
  cut.push(cut[0] + Math.round(share[1] * N));
  cut.push(cut[1] + Math.round(share[2] * N));
  cut.push(cut[2] + Math.round(share[3] * N));
  const map = new Map();
  rows.forEach(r => { if (r.known) map.set(r.w, 0); });
  rest.forEach((r, i) => {
    let t = i < cut[0] ? 0 : i < cut[1] ? 1 : i < cut[2] ? 2 : i < cut[3] ? 3 : 4;
    const n = r.w.length; if (n <= 4) t = Math.min(t, 2); else if (n <= 6) t = Math.min(t, 3); // короткое слово не бывает мифическим
    map.set(r.w, t);
  });
  return { map, known: N - rest.length, common: common.size };
}
module.exports = { buildTiers, norm };
