// Качает открытый список русских слов (danakt/russian-words) и собирает words.txt
const fs = require('fs'), path = require('path');
(async () => {
  const url = 'https://github.com/danakt/russian-words/raw/master/russian.txt';
  const res = await fetch(url);
  if (!res.ok) throw new Error('HTTP ' + res.status);
  const text = new TextDecoder('windows-1251').decode(Buffer.from(await res.arrayBuffer()));
  const set = new Set();
  for (const l of text.split(/\r?\n/)) {
    const w = l.trim().toLowerCase().replace(/ё/g, 'е');
    if (/^[а-я]{2,30}$/.test(w)) set.add(w);
  }
  fs.writeFileSync(path.join(__dirname, 'words.txt'), [...set].join('\n'));
  console.log('Готово, слов:', set.size);
})().catch(e => { console.error('Ошибка:', e.message); process.exit(1); });
