// Google Apps Script: пингует Render каждые 5 минут, чтобы бесплатный сервер не засыпал.
// Как поставить: script.google.com -> Новый проект -> вставь этот код -> один раз запусти функцию setup()
// (разреши доступ) -> готово. Проверка: меню "Выполнения" должна показывать запуски ping каждые 5 минут.
const PING_URL = 'https://slovoboy.onrender.com/ping';

function ping() {
  try {
    const r = UrlFetchApp.fetch(PING_URL, { muteHttpExceptions: true });
    console.log('ping', r.getResponseCode());
  } catch (e) {
    console.log('ошибка пинга: ' + e);
  }
}

function setup() {
  ScriptApp.getProjectTriggers().forEach(t => ScriptApp.deleteTrigger(t));
  ScriptApp.newTrigger('ping').timeBased().everyMinutes(5).create();
  ping();
}
