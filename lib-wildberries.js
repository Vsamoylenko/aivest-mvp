// Wildberries Marketplace API integration for digital-goods delivery.
//
// Flow (no webhook — WB has no push API for orders, so we poll):
//   1. Cron hits /api/cron/wb-deliver every ~2 min.
//   2. We pull /api/v3/orders/new for unfulfilled FBS orders.
//   3. For each order whose `article` (SKU) is one we sell digital keys for:
//      a) pop a random key from shared inventory `ym:keys:<sku>` (same pool
//         used by Yandex.Market — Steam keys are platform-agnostic);
//      b) try sending the key in the buyer chat (only works if buyer
//         already opened a chat — WB doesn't auto-create one);
//      c) close the order via supply-flow (create supply → add order →
//         deliver), so it leaves the "new" list and stops billing the timer;
//      d) mark `wb:delivered:<orderId>` for idempotency (24h ≪ WB's
//         24h-to-ship window, but enough to dedupe within a sweep cycle).
//
// Even if chat delivery fails (no chat yet), we still close the order and
// fire a Telegram alert with the key so admin can paste it manually until
// the buyer opens a chat. This guarantees we never miss WB's 24h SLA.
//
// Env vars:
//   WbToken (or WB_TOKEN) — WB API token with scopes: marketplace, chats.
//   ADMIN_KEY              — already used for YM admin endpoints.
//   YM_ADMIN_CHAT_ID       — Telegram chat for admin alerts.
//   TELEGRAM_BOT_TOKEN     — Telegram bot.

const axios = require('axios');

const WB_MARKETPLACE_API = 'https://marketplace-api.wildberries.ru';
const WB_CHAT_API        = 'https://buyer-chat-api.wildberries.ru';

// SKU aliases — map seller's WB articles (Артикул продавца) to the inventory
// pool key. Both WB cards sell the same Steam-key pool, so they share
// `ym:keys:MRKT-JU4L95I3`. Add new entries here when listing more cards.
//
// Override at deploy time via env WB_SKU_MAP (JSON), e.g.:
//   WB_SKU_MAP={"WBRANDOMRL":"MRKT-JU4L95I3","FOO":"MRKT-JU4L95I3"}
const DEFAULT_SKU_MAP = {
  'WBRANDOMRL':    'MRKT-JU4L95I3',
  'MRKT-JU4L95I3': 'MRKT-JU4L95I3',
};
function loadSkuMap() {
  const raw = (process.env.WB_SKU_MAP || '').trim();
  if (!raw) return DEFAULT_SKU_MAP;
  try {
    const parsed = JSON.parse(raw);
    return { ...DEFAULT_SKU_MAP, ...parsed };
  } catch {
    console.warn('[wb] WB_SKU_MAP env var is not valid JSON — using defaults');
    return DEFAULT_SKU_MAP;
  }
}
const SKU_MAP = loadSkuMap();

function clean(v) { return (v == null ? '' : String(v)).trim(); }
// Resolve WB seller-article → shared inventory pool SKU. If no alias exists,
// fall back to the article itself (so `MRKT-…` always works without mapping).
function resolveInventorySku(wbArticle) {
  const a = clean(wbArticle);
  return SKU_MAP[a] || a;
}

class WildberriesAPI {
  constructor() {
    // Support both naming conventions — user added "WbToken", convention is WB_TOKEN.
    this.token = clean(process.env.WbToken || process.env.WB_TOKEN);
  }
  isConfigured() { return !!this.token; }
  _headers(extra) {
    return Object.assign({
      'Authorization': this.token,
      'Content-Type':  'application/json',
      'Accept':        'application/json',
    }, extra || {});
  }

  // ── Marketplace (FBS orders) ─────────────────────────────────────────────
  // GET /api/v3/orders/new — orders awaiting fulfillment.
  async listNewOrders() {
    const url = `${WB_MARKETPLACE_API}/api/v3/orders/new`;
    const { data } = await axios.get(url, { headers: this._headers(), timeout: 20000 });
    return data?.orders || [];
  }

  // POST /api/v3/supplies — create new supply (поставка).
  async createSupply(name = 'aivest-digital') {
    const url = `${WB_MARKETPLACE_API}/api/v3/supplies`;
    const { data } = await axios.post(url, { name }, { headers: this._headers(), timeout: 15000 });
    return data?.id;
  }

  // PATCH /api/v3/supplies/{supplyId}/orders/{orderId} — attach order to supply.
  async addOrderToSupply(supplyId, orderId) {
    const url = `${WB_MARKETPLACE_API}/api/v3/supplies/${supplyId}/orders/${orderId}`;
    await axios.patch(url, {}, { headers: this._headers(), timeout: 15000 });
  }

  // PATCH /api/v3/supplies/{supplyId}/deliver — transfer supply to delivery.
  // For digital goods this just marks "shipped" — no real logistics.
  async deliverSupply(supplyId) {
    const url = `${WB_MARKETPLACE_API}/api/v3/supplies/${supplyId}/deliver`;
    await axios.patch(url, {}, { headers: this._headers(), timeout: 15000 });
  }

  // ── Buyer chat ───────────────────────────────────────────────────────────
  // GET /api/v1/seller/chats — list chats with buyers.
  async listChats() {
    const url = `${WB_CHAT_API}/api/v1/seller/chats`;
    const { data } = await axios.get(url, { headers: this._headers(), timeout: 15000 });
    // Schema: { result: { chats: [{id, userId, ...}, ...] } } — defensive parsing.
    return data?.result?.chats || data?.chats || [];
  }

  // POST /api/v1/seller/message — send text to existing chat.
  async sendChatMessage(chatId, text) {
    const url = `${WB_CHAT_API}/api/v1/seller/message`;
    const { data } = await axios.post(url, { chatId, text }, { headers: this._headers(), timeout: 15000 });
    return data;
  }
}

// ── Order processing ───────────────────────────────────────────────────────
// Returns { delivered, reason?, sku?, chatDelivered? }.
// Inventory is SHARED with Yandex.Market: pops from `ym:keys:<sku>` Redis list
// via lib-yandex-market.popKey (so randomization + race-safety match).
async function processOrder(orderId, wb, redis, allOrders) {
  if (!wb.isConfigured()) return { delivered: false, reason: 'WB not configured' };

  const ymLib = require('./lib-yandex-market');

  // Idempotency — skip if we've already shipped this order.
  const dkey = `wb:delivered:${orderId}`;
  if (await redis.get(dkey)) return { delivered: false, reason: 'already delivered' };

  // Find order in caller-supplied list (avoids re-fetching) or pull fresh.
  const orders = allOrders || await wb.listNewOrders();
  const order = orders.find(o => String(o.id) === String(orderId));
  if (!order) return { delivered: false, reason: 'order not in NEW list (already shipped or canceled)' };

  // WB order's `article` = Артикул продавца. We may sell the same Steam-key
  // pool under multiple seller-articles (e.g. WBRANDOMRL + MRKT-JU4L95I3 both
  // map to the shared MRKT-JU4L95I3 inventory). Resolve via SKU_MAP.
  const wbArticle    = clean(order.article);
  const inventorySku = resolveInventorySku(wbArticle);
  if (!inventorySku) return { delivered: false, reason: 'no article' };

  // Check stock first (don't pop blindly — we want a clean error if empty).
  const stockKey = `ym:keys:${inventorySku}`;
  const stock = await redis.llen(stockKey);
  if (!stock) {
    await ymLib.notifyAdmin(`⚠️ <b>WB: нет ключей</b>\nЗаказ <code>${orderId}</code>\nWB-артикул <code>${wbArticle}</code> → пул <code>${inventorySku}</code>\nЗалей через /api/admin/ym/keys`);
    await ymLib.logEvent(redis, { type: 'wb_out_of_stock', orderId, wbArticle, inventorySku });
    return { delivered: false, reason: `no stock for ${inventorySku}` };
  }

  // Random pop from shared pool.
  const key = await ymLib.popKey(redis, inventorySku);
  if (!key) return { delivered: false, reason: `popKey returned empty for ${inventorySku}` };
  // For backward compatibility with the rest of this function, expose the
  // resolved inventory SKU as `sku` (used in alerts/logs/Redis records).
  const sku = inventorySku;

  // Try sending via chat — won't work unless buyer opened chat first.
  let chatDelivered = false;
  let chatError = null;
  const slipText = `Ваш ключ Steam: ${key}\n\nАктивация:\n1) Откройте Steam → Игры → Активировать через Steam.\n2) Или вставьте ключ на странице https://store.steampowered.com/account/registerkey\n\nЕсли ключ не подошёл — ответьте в этом чате, заменим.`;
  try {
    const chats = await wb.listChats().catch(() => []);
    // WB chat schema doesn't directly link chat→order; we match by buyer
    // userId if the order carries it. Order shape on WB has `userId` for
    // some marketplaces; defensively try several names.
    const buyerId = order.userId || order.user?.id || order.customerId;
    let targetChat = null;
    if (buyerId) targetChat = chats.find(c => String(c.userId) === String(buyerId));
    // Fallback: if there's exactly one open chat, assume it's this order.
    // (Aggressive — only do it when buyerId match failed and there's just one.)
    if (!targetChat && chats.length === 1) targetChat = chats[0];

    if (targetChat) {
      await wb.sendChatMessage(targetChat.id || targetChat.chatId, slipText);
      chatDelivered = true;
    }
  } catch (e) {
    chatError = e.response?.data || e.message;
    console.error('[wb] chat send failed:', chatError);
  }

  // Close the order via supply-flow regardless of chat success — so the order
  // leaves the "new" queue and we don't blow WB's 24h SLA. If this throws,
  // roll the key back into inventory.
  let supplyId = null;
  try {
    supplyId = await wb.createSupply(`aivest-${new Date().toISOString().slice(0, 10)}`);
    await wb.addOrderToSupply(supplyId, orderId);
    await wb.deliverSupply(supplyId);
  } catch (e) {
    await redis.lpush(stockKey, key);
    const errBody = e.response?.data || e.message;
    await ymLib.notifyAdmin(`❌ <b>WB: ошибка закрытия заказа</b>\nЗаказ <code>${orderId}</code>\n<pre>${typeof errBody === 'string' ? errBody : JSON.stringify(errBody).slice(0,500)}</pre>`);
    await ymLib.logEvent(redis, { type: 'wb_supply_error', orderId, error: String(errBody).slice(0, 500) });
    throw e;
  }

  // Mark delivered (90-day TTL).
  await redis.set(dkey, JSON.stringify({
    at: new Date().toISOString(),
    wbArticle, sku, keyTail: key.slice(-4),
    supplyId, chatDelivered,
  }), { ex: 86400 * 90 });

  // Show both WB-article and inventory-pool SKU when they differ — useful when
  // multiple WB cards (e.g. WBRANDOMRL + MRKT-JU4L95I3) share the same pool.
  const skuLine = wbArticle === sku
    ? `SKU <code>${sku}</code>`
    : `WB-артикул <code>${wbArticle}</code> → пул <code>${sku}</code>`;

  // Telegram alert. If chat-delivered, just FYI. If not, include the full key
  // so admin can paste it the moment the buyer opens a chat or files a claim.
  const summary = chatDelivered
    ? `✅ <b>WB: ключ отправлен в чат</b>\nЗаказ <code>${orderId}</code>\n${skuLine} → ...${key.slice(-4)}`
    : `📨 <b>WB: заказ закрыт, чат пока не открыт</b>\nЗаказ <code>${orderId}</code>\n${skuLine}\n\n<b>Ключ для покупателя:</b>\n<code>${key}</code>\n\nКак только покупатель напишет в чат — отправь ему этот ключ. Бот тоже попробует автоматически на следующих циклах.`;
  await ymLib.notifyAdmin(summary);
  await ymLib.logEvent(redis, {
    type: 'wb_delivered', orderId, wbArticle, sku,
    keyTail: key.slice(-4), chatDelivered, supplyId,
    chatError: chatError ? String(chatError).slice(0, 200) : undefined,
  });

  return { delivered: true, wbArticle, sku, chatDelivered, supplyId };
}

// Sweep: list all NEW FBS orders, process digital ones we haven't delivered.
async function sweepNewOrders(wb, redis) {
  if (!wb.isConfigured()) return { swept: 0, results: [], note: 'WB not configured' };
  const orders = await wb.listNewOrders().catch(e => {
    console.error('[wb] listNewOrders failed:', e.response?.data || e.message);
    return null;
  });
  if (!orders) return { swept: 0, results: [], error: 'listNewOrders failed' };
  const results = [];
  for (const o of orders) {
    try {
      const r = await processOrder(o.id, wb, redis, orders);
      results.push({ orderId: o.id, ...r });
    } catch (e) {
      results.push({ orderId: o.id, delivered: false, error: e.message });
    }
  }
  return { swept: results.length, results };
}

module.exports = {
  WildberriesAPI,
  processOrder,
  sweepNewOrders,
};
