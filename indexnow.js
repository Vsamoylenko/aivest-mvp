// IndexNow client — pings Yandex, Bing, Seznam, etc.
// Docs: https://www.indexnow.org/documentation
//
// Setup:
//   1. Set INDEXNOW_KEY env var to a UUID (e.g. `crypto.randomUUID()`).
//   2. The server auto-hosts the key at /<INDEXNOW_KEY>.txt (see server.js).
//   3. Call pingIndexNow([urls]) after publishing new content.

const https = require('https');

const HOST = 'aivest.ru';
const ENDPOINT = 'https://api.indexnow.org/IndexNow'; // common endpoint, relays to all engines

function pingIndexNow(urls) {
  return new Promise((resolve, reject) => {
    const key = process.env.INDEXNOW_KEY;
    if (!key) return reject(new Error('INDEXNOW_KEY env var not set'));
    if (!Array.isArray(urls) || !urls.length) return reject(new Error('No URLs to ping'));

    const body = JSON.stringify({
      host: HOST,
      key: key,
      keyLocation: `https://${HOST}/${key}.txt`,
      urlList: urls.slice(0, 10000), // API limit
    });

    const u = new URL(ENDPOINT);
    const req = https.request({
      hostname: u.hostname,
      port: 443,
      path: u.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Content-Length': Buffer.byteLength(body),
      },
    }, (resp) => {
      let chunks = '';
      resp.on('data', (c) => chunks += c);
      resp.on('end', () => {
        // IndexNow returns:
        //   200 — URLs submitted successfully
        //   202 — accepted (validated shortly)
        //   400 — bad request
        //   403 — key not valid (URL file not found or key mismatch)
        //   422 — unprocessable entity (e.g. URL not on this host)
        //   429 — too many requests
        if (resp.statusCode === 200 || resp.statusCode === 202) {
          resolve({ status: resp.statusCode, count: urls.length, response: chunks });
        } else {
          reject(new Error(`IndexNow returned ${resp.statusCode}: ${chunks}`));
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// Core URLs — homepage + geo landing pages + main anchors. Safe to call after every deploy.
function coreUrls() {
  const base = `https://${HOST}`;
  return [
    `${base}/`,
    `${base}/moscow`,
    `${base}/spb`,
    `${base}/sochi`,
    `${base}/#how-it-works`,
    `${base}/#methodology`,
    `${base}/#pricing`,
  ];
}

module.exports = { pingIndexNow, coreUrls };
