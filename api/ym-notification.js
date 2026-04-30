// Lightweight standalone Vercel serverless handler for Yandex.Market webhook.
//
// Why this file exists separately from server.js:
//   YM PING timeout = 1 second. The main Express bundle (server.js + scrapers
//   + lib-* modules) is ~10MB and cold-starts in 1–2s on Vercel — misses the
//   deadline every time the lambda is cold. This file keeps TOP-LEVEL imports
//   at zero, so cold start is ~100–200ms.
//
// Heavy modules (axios via lib-yandex-market, @upstash/redis) are required
// LAZILY — only after we've already responded to the PING. Real ORDER_*
// notifications still pay that cost, but their timeout is 10s so it fits.

module.exports = async (req, res) => {
  // First-line debug: see EXACTLY what reaches us.
  // CANT_GET_RESPONSE on Yandex side often means the auth check failed and
  // they got 401 — without logs we can't tell rewrite vs. body issue apart.
  console.log('[ym] hit', req.method, req.url, 'q=', JSON.stringify(req.query || {}));

  // Reachability probe (browser / uptime pinger / "Проверить URL" GET phase).
  if (req.method === 'GET') {
    return res.status(200).json({ status: 'OK' });
  }
  if (req.method !== 'POST') {
    return res.status(405).json({ status: 'ERROR', errors: [{ code: 'METHOD_NOT_ALLOWED' }] });
  }

  // ── Verify secret. Yandex always appends `/notification` to the configured
  // base URL, so our incoming path can take any of these shapes:
  //   /api/ym/notification                              (no secret → require ?s=… or X-YM-Secret hdr)
  //   /api/ym/notification/<SECRET>                     (recommended)
  //   /api/ym/notification/<SECRET>/notification        (if user typed full path themselves)
  const expected = (process.env.YM_WEBHOOK_SECRET || '').trim();
  const path     = (req.url || '').split('?')[0];
  const segments = path.split('/').filter(Boolean); // ['api','ym','notification', ...]
  const pathSecret = (segments[3] || '').trim();
  const fromQuery  = ((req.query && req.query.s) || '').toString().trim();
  const fromHdr    = (req.headers['x-ym-secret'] || '').toString().trim();

  let okSource = null;
  if (expected) {
    if      (pathSecret === expected) okSource = 'path';
    else if (fromQuery  === expected) okSource = 'query';
    else if (fromHdr    === expected) okSource = 'header';
  }

  if (!okSource) {
    console.warn('[ym] AUTH FAIL', JSON.stringify({
      expectedLen: expected.length,
      pathLen:  pathSecret.length,
      queryLen: fromQuery.length,
      hdrLen:   fromHdr.length,
      url: req.url,
    }));
    return res.status(401).json({
      status: 'ERROR',
      errors: [{ code: 'UNAUTHORIZED', message: 'bad secret' }],
    });
  }

  // ── Documented success body (PING and order acks share the same schema).
  const ackBody = JSON.stringify({
    version: '1.0.0',
    name:    'aivest-ym',
    time:    new Date().toISOString(),
  });
  const sendAck = () => {
    res.writeHead(200, {
      'Content-Type':   'application/json',
      'Content-Length': Buffer.byteLength(ackBody).toString(),
    });
    res.end(ackBody);
  };

  const body = req.body || {};
  const type = (body.notificationType || body.type || body.eventType || '').toString().toUpperCase();

  // PING: nothing to do — ack and return immediately. Yandex caps PING at 1s.
  if (type === 'PING' || !type) {
    sendAck();
    console.log(`[ym] PING ok (via ${okSource})`);
    return;
  }

  // ── REAL notification. We MUST await processing BEFORE res.end() —
  // Vercel's serverless runtime suspends the function the instant the
  // response goes out, killing any pending async work. Yandex allows 10s
  // for non-PING notifications, plenty of room for a Yandex API roundtrip.
  const orderId = body.orderId || (body.order && (body.order.id || body.order.orderId));
  console.log(`[ym] hit type=${type} orderId=${orderId || '(none)'} via=${okSource}`);

  if (!orderId) {
    // Some events (chat, dispute, review) carry no orderId — just ack.
    console.log(`[ym] type=${type} no orderId — body:`, JSON.stringify(body).slice(0, 400));
    sendAck();
    return;
  }

  // Process synchronously, then ack. If we exceed Yandex's 10s window the
  // ack fails and they retry — our idempotency guard prevents double-deliver.
  try {
    const ymLib = require('../lib-yandex-market');
    const { Redis } = require('@upstash/redis');
    const url   = process.env.UPSTASH_REDIS_REST_URL  || process.env.KV_REST_API_URL;
    const token = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
    if (url && token) {
      const redis = new Redis({ url, token });
      const ym = new ymLib.YandexMarket();
      const r = await ymLib.processOrder(orderId, ym, redis);
      console.log(`[ym] orderId=${orderId} type=${type} → ${JSON.stringify(r)}`);
    } else {
      console.warn('[ym] KV not configured — cannot process order', orderId);
    }
  } catch (e) {
    console.error(`[ym] orderId=${orderId} processOrder failed:`, (e.response && e.response.data) || e.message);
  } finally {
    // Ack no matter what — Yandex doesn't expect us to expose internal errors.
    sendAck();
  }
};
