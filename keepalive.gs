// Google Apps Script: вставь свой адрес Space, потом Triggers -> Add Trigger -> ping -> Time-driven -> каждые 5 минут
function ping() {
  UrlFetchApp.fetch('https://ТВОЙ-НИК-slovoboy.hf.space/ping', { muteHttpExceptions: true });
}
