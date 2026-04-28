// Yandex.Market Partner API integration for digital-goods delivery.
//
// Flow:
//   1. Marketplace pushes a webhook to /api/ym/notification when an order changes status.
//   2. We pull the order, verify it's DIGITAL + PROCESSING + not yet delivered.
//   3. For each item, pop one activation code from the per-SKU stock list in Upstash.
//   4. POST deliverDigitalGoods → Yandex emails the key to the buyer.
//   5. Persist delivery record (idempotency) + Telegram-notify admin.
//
// Env vars required (set in Vercel dashboard):
//   YM_OAUTH_TOKEN       — OAuth token from Yandex.Market Partner cabinet
//   YM_BUSINESS_ID       — your business ID (cabinet → Settings → Companies)
//   YM_CAMPAIGN_ID       — campaign ID (cabinet → Settings → Stores)
//   YM_WEBHOOK_SECRET    — random string you generate; pass to Yandex when configuring
//                          the webhook URL like https://aivest.ru/api/ym/notification?s=<secret>
//   TELEGRAM_BOT_TOKEN   — already set; used for admin notifications
//   YM_ADMIN_CHAT_ID     — your Telegram chat id (DM the bot, then look up)
//
// Inventory model in Upstash:
//   ym:keys:<offerId>   — Redis list. lpush adds, rpop pops one for delivery.
//   ym:delivered:<orderId>  — JSON string. Existence = already delivered (idempotency).
//   ym:log              — capped Redis list of recent events.

const axios = require('axios');

const YM_API = 'https://api.partner.market.yandex.ru';

function clean(v) { return (v == null ? '' : String(v)).trim(); }

class YandexMarket {
  constructor() {
    this.token      = clean(process.env.YM_OAUTH_TOKEN);
    this.businessId = clean(process.env.YM_BUSINESS_ID);
    this.campaignId = clean(process.env.YM_CAMPAIGN_ID);
  }
  isConfigured() {
    return !!(this.token && this.businessId && this.campaignId);
  }
  _headers() {
    return {
      // YM deprecated OAuth Bearer — auth now via `Api-Key: <token>`.
      // Token created in кабинете продавца → Настройки → API и модули → Токены.
      'Api-Key':       this.token,
      'Content-Type':  'application/json',
      'Accept':        'application/json',
    };
  }
  // POST /businesses/{businessId}/orders — list orders with filter body.
  async listOrders(filter = {}) {
    const url = `${YM_API}/businesses/${this.businessId}/orders`;
    const { data } = await axios.post(url, filter, { headers: this._headers(), timeout: 20000 });
    return data;
  }
  // GET /campaigns/{campaignId}/orders/{orderId} — full order detail (incl. items + delivery type).
  async getOrder(orderId) {
    const url = `${YM_API}/campaigns/${this.campaignId}/orders/${orderId}`;
    const { data } = await axios.get(url, { headers: this._headers(), timeout: 15000 });
    return data;
  }
  // POST /campaigns/{campaignId}/orders/{orderId}/deliverDigitalGoods
  // Body: { items: [{ id: <itemId>, code: [{ activationCode: "XXX" }] }] }
  async deliverDigitalGoods(orderId, items) {
    const url = `${YM_API}/campaigns/${this.campaignId}/orders/${orderId}/deliverDigitalGoods`;
    const { data } = await axios.post(url, { items }, { headers: this._headers(), timeout: 20000 });
    return data;
  }
}

// ── Inventory ops on Upstash ─────────────────────────────────────────────
async function addKeys(redis, sku, keys) {
  if (!Array.isArray(keys) || !keys.length) throw new Error('keys[] required');
  const trimmed = keys.map(clean).filter(Boolean);
  if (!trimmed.length) throw new Error('all keys empty after trim');
  // lpush + rpop = FIFO (oldest keys delivered first → fair)
  await redis.lpush(`ym:keys:${sku}`, ...trimmed);
  return await redis.llen(`ym:keys:${sku}`);
}

// Pop a RANDOM key from the per-SKU list (not FIFO). This avoids predictable
// "older keys go first" behaviour — buyers get a uniformly-random pick from
// current inventory. Implementation: LLEN → random index → LINDEX → LREM.
// Not atomic across two concurrent calls, but real traffic is low (a few orders
// per day) so the race window is negligible. If two callers ever pick the same
// idx, one of the LREMs will silently no-op and that caller will retry below.
async function popKey(redis, sku) {
  const list = `ym:keys:${sku}`;
  for (let attempt = 0; attempt < 3; attempt++) {
    const len = await redis.llen(list);
    if (!len) return null;
    const idx = Math.floor(Math.random() * len);
    const val = await redis.lindex(list, idx);
    if (val == null) continue;
    const removed = await redis.lrem(list, 1, val);
    if (removed > 0) return val; // we got it
    // someone else just took this one — retry with a fresh random pick
  }
  // Fallback: deterministic rpop so we never silently fail to deliver.
  return await redis.rpop(list);
}

async function inventoryStatus(redis) {
  // Upstash supports SCAN; for simplicity we KEYS — fine while under ~hundreds of SKUs.
  const skus = await redis.keys('ym:keys:*');
  const out = {};
  for (const k of skus) {
    out[k.replace('ym:keys:', '')] = await redis.llen(k);
  }
  return out;
}

async function logEvent(redis, event) {
  const entry = JSON.stringify({ at: new Date().toISOString(), ...event });
  await redis.lpush('ym:log', entry);
  await redis.ltrim('ym:log', 0, 199); // keep last 200
}

async function recentLog(redis, limit = 50) {
  const items = await redis.lrange('ym:log', 0, Math.max(0, limit - 1));
  return items.map(s => { try { return JSON.parse(s); } catch { return { raw: s }; } });
}

// ── Telegram admin notify (optional) ─────────────────────────────────────
async function notifyAdmin(text) {
  const token  = clean(process.env.TELEGRAM_BOT_TOKEN);
  const chatId = clean(process.env.YM_ADMIN_CHAT_ID);
  if (!token || !chatId) return;
  try {
    await axios.post(
      `https://api.telegram.org/bot${token}/sendMessage`,
      { chat_id: chatId, text, parse_mode: 'HTML', disable_web_page_preview: true },
      { timeout: 10000 }
    );
  } catch (e) {
    console.error('YM telegram notify failed:', e.response?.data || e.message);
  }
}

// ── Core: process a single order ─────────────────────────────────────────
// Returns { delivered: bool, reason?: string, items?: [...] }
async function processOrder(orderId, ym, redis) {
  if (!ym.isConfigured()) return { delivered: false, reason: 'YM not configured' };

  // Idempotency — never deliver twice.
  const dkey = `ym:delivered:${orderId}`;
  if (await redis.get(dkey)) {
    return { delivered: false, reason: 'already delivered' };
  }

  const orderResp = await ym.getOrder(orderId);
  const order = orderResp?.order || orderResp; // API shape: { order: {...} } or direct

  // Filter: must be DIGITAL + PROCESSING.
  const deliveryType = order?.delivery?.type;
  const status       = order?.status;
  if (deliveryType !== 'DIGITAL') return { delivered: false, reason: `delivery=${deliveryType}` };
  if (status       !== 'PROCESSING') return { delivered: false, reason: `status=${status}` };

  const items = order.items || [];
  if (!items.length) return { delivered: false, reason: 'no items' };

  // Pop a key per item. If ANY item has no stock, abort BEFORE calling YM
  // (partial delivery isn't supported; partial pops would lose keys).
  const popped = []; // { itemId, sku, key }
  for (const it of items) {
    // Yandex item identity: `id` (line-item id), `offerId` (SKU you control).
    const sku = String(it.offerId || it.shopSku || it.marketSku || '');
    const key = await popKey(redis, sku);
    if (!key) {
      // Roll back: lpush back any keys we already popped for previous items.
      for (const p of popped) await redis.lpush(`ym:keys:${p.sku}`, p.key);
      await notifyAdmin(`⚠️ <b>Я.Маркет: нет ключей</b>\nЗаказ <code>${orderId}</code>, SKU <code>${sku}</code>\nДобавь ключи через /api/admin/ym/keys`);
      await logEvent(redis, { type: 'out_of_stock', orderId, sku });
      return { delivered: false, reason: `no stock for ${sku}` };
    }
    popped.push({ itemId: it.id, sku, key });
  }

  // Deliver via Yandex API.
  // Per current Partner API docs, `code` is a plain string (not array of
  // objects). The legacy `[{activationCode: ...}]` form returns
  // BAD_REQUEST: "Illegal input at items[0].code".
  // Optional `activatedAt` (RFC3339) helps Yandex show the correct timestamp
  // to the buyer.
  // Field is `codes` (plural, array of strings) — `code` is the deprecated
  // legacy form. `slip` is optional brief activation instructions shown to
  // the buyer; HTML allowed (h1/br/ol/ul/li).
  // activateTill is required (YYYY-MM-DD). Steam keys don't expire — set 5 years
  // ahead so Yandex is happy. Field name is camelCase even though docs show
  // `activate_till` (snake_case); the API rejects null with the camelCase name.
  const fiveYears = new Date(); fiveYears.setFullYear(fiveYears.getFullYear() + 5);
  const activateTill = fiveYears.toISOString().slice(0, 10); // YYYY-MM-DD

  const payload = popped.map(p => ({
    id:           p.itemId,
    codes:        [p.key],
    activateTill,
    slip:         'Активируйте ключ в Steam: https://store.steampowered.com/account/registerkey<br>Если возникли вопросы — напишите в чат заказа.',
  }));

  try {
    await ym.deliverDigitalGoods(orderId, payload);
  } catch (e) {
    // Roll back keys to inventory if YM rejected.
    for (const p of popped) await redis.lpush(`ym:keys:${p.sku}`, p.key);
    const errBody = e.response?.data || e.message;
    await notifyAdmin(`❌ <b>Я.Маркет: ошибка доставки</b>\nЗаказ <code>${orderId}</code>\n<pre>${typeof errBody === 'string' ? errBody : JSON.stringify(errBody).slice(0,500)}</pre>`);
    await logEvent(redis, { type: 'deliver_error', orderId, error: String(errBody).slice(0, 500) });
    throw e;
  }

  // Mark delivered (90-day TTL — long enough to debug, short enough to clean up).
  await redis.set(dkey, JSON.stringify({
    at: new Date().toISOString(),
    items: popped.map(p => ({ itemId: p.itemId, sku: p.sku, keyTail: p.key.slice(-4) })),
  }), { ex: 86400 * 90 });

  await notifyAdmin(`✅ <b>Я.Маркет: ключи отправлены</b>\nЗаказ <code>${orderId}</code>\n${popped.map(p => `· ${p.sku} → ...${p.key.slice(-4)}`).join('\n')}`);
  await logEvent(redis, { type: 'delivered', orderId, items: popped.map(p => ({ sku: p.sku, keyTail: p.key.slice(-4) })) });

  return { delivered: true, items: popped.map(p => ({ sku: p.sku })) };
}

// Sweep: fetch PROCESSING orders, process digital ones we haven't delivered yet.
// Used by the cron fallback when a webhook is missed.
async function sweepProcessingDigital(ym, redis) {
  if (!ym.isConfigured()) return { swept: 0, results: [], note: 'YM not configured' };
  const list = await ym.listOrders({ status: 'PROCESSING' }).catch(e => {
    console.error('YM listOrders failed:', e.response?.data || e.message);
    return null;
  });
  const orders = list?.orders || list?.result?.orders || [];
  const results = [];
  for (const o of orders) {
    if (o.delivery?.type !== 'DIGITAL') continue;
    try {
      const r = await processOrder(o.id, ym, redis);
      results.push({ orderId: o.id, ...r });
    } catch (e) {
      results.push({ orderId: o.id, delivered: false, error: e.message });
    }
  }
  return { swept: results.length, results };
}

module.exports = {
  YandexMarket,
  processOrder,
  sweepProcessingDigital,
  addKeys,
  popKey,
  inventoryStatus,
  logEvent,
  recentLog,
  notifyAdmin,
};
