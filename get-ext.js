// Большой словарь для игры: существительные и глаголы (начальные формы) из открытых источников.
// Результат: words-ext.txt рядом с сервером. Если ничего не скачалось, игра работает на встроенных списках.
const fs = require('fs'), path = require('path'), zlib = require('zlib');

function unzipFirst(buf) {
  let e = buf.length - 22; while (e >= 0 && buf.readUInt32LE(e) !== 0x06054b50) e--;
  if (e < 0) throw new Error('это не zip');
  const cd = buf.readUInt32LE(e + 16); if (buf.readUInt32LE(cd) !== 0x02014b50) throw new Error('повреждён zip');
  const method = buf.readUInt16LE(cd + 10), csize = buf.readUInt32LE(cd + 20), lho = buf.readUInt32LE(cd + 42);
  const start = lho + 30 + buf.readUInt16LE(lho + 26) + buf.readUInt16LE(lho + 28), data = buf.subarray(start, start + csize);
  return method === 0 ? data : zlib.inflateRawSync(data);
}

// формат OpenCorpora: блок на лемму; в блоке строки «форма<TAB>граммемы», первая строка с табом — сама лемма
function parseOpenCorpora(text) {
  const out = new Set(), BAD = /\b(Name|Surn|Patr|Geox|Orgn|Trad|Abbr|Init|Erro|Infr)\b/;
  for (const block of text.split(/\r?\n\s*\r?\n/)) {
    const lines = block.split(/\r?\n/), li = lines.findIndex(l => l.includes('\t')); if (li < 0) continue;
    const [form, tags] = lines[li].split('\t');
    if (!/(^|,)(NOUN|INFN)(,|\s|$)/.test(tags) || BAD.test(tags)) continue;
    const w = form.toLowerCase().replace(/ё/g, 'е').trim();
    if (/^[а-я]+(-[а-я]+)?$/.test(w) && w.length >= 2 && w.length <= 30) out.add(w);
  }
  return out;
}

async function get(url, bin) { const r = await fetch(url); if (!r.ok) throw new Error('HTTP ' + r.status); return bin ? Buffer.from(await r.arrayBuffer()) : r.text(); }

async function run() {
  const all = new Set();
  for (const url of ['https://opencorpora.org/files/export/dict/dict.opcorpora.txt.zip', 'http://opencorpora.org/files/export/dict/dict.opcorpora.txt.zip']) {
    try { const set = parseOpenCorpora(unzipFirst(await get(url, true)).toString('utf8')); set.forEach(w => all.add(w)); console.log('OpenCorpora: слов', set.size); break; } catch (e) { console.log('OpenCorpora не загрузился (' + e.message + ')'); }
  }
  try { const t = await get('https://raw.githubusercontent.com/Harrix/Russian-Nouns/main/dist/russian_nouns.txt'); let n = 0; t.split(/\r?\n/).forEach(l => { const w = l.trim().toLowerCase().replace(/ё/g, 'е'); if (/^[а-я]+(-[а-я]+)?$/.test(w) && w.length >= 2 && w.length <= 30) { all.add(w); n++; } }); console.log('Russian-Nouns: слов', n); } catch (e) { console.log('Russian-Nouns не загрузился (' + e.message + ')'); }
  if (all.size > 1000) { fs.writeFileSync(path.join(__dirname, 'words-ext.txt'), [...all].join('\n') + '\n'); console.log('Большой словарь записан: слов', all.size); }
  else console.log('Большой словарь не получен, остаёмся на встроенных списках');
}
module.exports = { unzipFirst, parseOpenCorpora, run };
if (require.main === module) run().catch(e => console.log('Большой словарь: ошибка', e.message));
