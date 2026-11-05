// Скачивает список частых слов в common.txt (улучшает определение редкости). words.txt НЕ трогает.
// Если не скачалось, ничего страшного: сервер работает и без него.
const fs = require('fs'), path = require('path');
(async () => {
  const r = await fetch('https://raw.githubusercontent.com/hingston/russian/master/10000-russian-words-cyrillic-only.txt');
  if (!r.ok) throw new Error('HTTP ' + r.status);
  const t = await r.text();
  fs.writeFileSync(path.join(__dirname, 'common.txt'), t);
  console.log('Список частых слов загружен:', t.split(/\s+/).filter(Boolean).length);
})().catch(e => console.log('Список частых слов не загрузился (' + e.message + '), работаем без него'));
