'use strict';
// Минимальный клиент S3 (AWS Signature V4) без зависимостей. Работает с Filebase, R2, B2 и любым S3-совместимым хранилищем.
const crypto = require('crypto');
const hm = (k, d) => crypto.createHmac('sha256', k).update(d).digest();
const sha = d => crypto.createHash('sha256').update(d).digest('hex');
const enc = s => encodeURIComponent(s).replace(/[!'()*]/g, c => '%' + c.charCodeAt(0).toString(16).toUpperCase());

function sign({ method, host, path, headers, payloadHash, region, keyId, secret, date }) {
  const day = date.slice(0, 8);
  const h = Object.assign({}, headers, { host, 'x-amz-content-sha256': payloadHash, 'x-amz-date': date });
  const names = Object.keys(h).map(x => x.toLowerCase()).sort();
  const low = {}; Object.keys(h).forEach(k => { low[k.toLowerCase()] = String(h[k]).trim(); });
  const canonical = [method, path.split('/').map(enc).join('/'), '', names.map(n => n + ':' + low[n] + '\n').join(''), names.join(';'), payloadHash].join('\n');
  const scope = day + '/' + region + '/s3/aws4_request';
  const sts = ['AWS4-HMAC-SHA256', date, scope, sha(canonical)].join('\n');
  const key = hm(hm(hm(hm('AWS4' + secret, day), region), 's3'), 'aws4_request');
  const signature = crypto.createHmac('sha256', key).update(sts).digest('hex');
  return 'AWS4-HMAC-SHA256 Credential=' + keyId + '/' + scope + ', SignedHeaders=' + names.join(';') + ', Signature=' + signature;
}

function client(cfg) {
  const u = new URL(cfg.endpoint);
  async function call(method, key, body, type) {
    const host = cfg.vhost ? cfg.bucket + '.' + u.host : u.host;
    const path = '/' + (cfg.vhost ? '' : cfg.bucket + '/') + key;
    const payload = body || Buffer.alloc(0);
    const date = new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d+/, '');
    const headers = type ? { 'content-type': type } : {};
    const authorization = sign({ method, host, path, headers, payloadHash: sha(payload), region: cfg.region, keyId: cfg.keyId, secret: cfg.secret, date });
    const r = await fetch(u.protocol + '//' + host + path.split('/').map(enc).join('/'), {
      method, headers: Object.assign({}, headers, { 'x-amz-content-sha256': sha(payload), 'x-amz-date': date, authorization }),
      body: method === 'PUT' ? payload : undefined, signal: AbortSignal.timeout(15000)
    });
    return r;
  }
  return {
    async put(key, buf, type) { const r = await call('PUT', key, buf, type); if (!r.ok) throw new Error('PUT ' + r.status + ' ' + (await r.text()).slice(0, 200)); return true; },
    async get(key) { const r = await call('GET', key); if (r.status === 404) return null; if (!r.ok) throw new Error('GET ' + r.status); return Buffer.from(await r.arrayBuffer()); },
    async del(key) { const r = await call('DELETE', key); return r.ok || r.status === 404; }
  };
}
module.exports = { sign, client, sha };
