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

  // ── Warehouses & stock management ────────────────────────────────────────
  // GET /api/v3/warehouses — list seller's FBS warehouses.
  async listWarehouses() {
    const url = `${WB_MARKETPLACE_API}/api/v3/warehouses`;
    const { data } = await axios.get(url, { headers: this._headers(), timeout: 15000 });
    return Array.isArray(data) ? data : (data?.warehouses || []);
  }

  // POST /api/v3/stocks/{warehouseId} body: { skus: [<barcode>, ...] }
  // Returns current stocks for given barcodes.
  async getStocks(warehouseId, barcodes) {
    const url = `${WB_MARKETPLACE_API}/api/v3/stocks/${warehouseId}`;
    const { data } = await axios.post(url, { skus: barcodes }, { headers: this._headers(), timeout: 15000 });
    return data?.stocks || [];
  }

  // PUT /api/v3/stocks/{warehouseId} body: { stocks: [{ sku, amount }] }
  // Sets ABSOLUTE stock value (not delta). `sku` here = barcode.
  async setStocks(warehouseId, items) {
    const url = `${WB_MARKETPLACE_API}/api/v3/stocks/${warehouseId}`;
    await axios.put(url, { stocks: items }, { headers: this._headers(), timeout: 15000 });
  }

  // Convenience: bump stock for one barcode by `delta` (positive or negative).
  // If `warehouseId` is omitted and seller has exactly one warehouse, uses it.
  async adjustStock(barcode, delta, warehouseId) {
    if (!warehouseId) {
      const whs = await this.listWarehouses();
      if (whs.length !== 1) {
        throw new Error(`expected 1 warehouse, got ${whs.length} — pass warehouseId explicitly`);
      }
      warehouseId = whs[0].id;
    }
    const current = await this.getStocks(warehouseId, [barcode]);
    const cur = (current.find(s => String(s.sku) === String(barcode))?.amount) || 0;
    const next = Math.max(0, cur + delta);
    await this.setStocks(warehouseId, [{ sku: String(barcode), amount: next }]);
    return { warehouseId, barcode, before: cur, after: next, delta };
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

  // GET /api/v1/seller/events?next=N — incremental chat events poll.
  // WB returns { result: { events: [...], next: <cursor> } }. We track the
  // cursor in Redis (key `wb:chat:cursor`) so each cron tick only sees
  // messages we haven't processed yet.
  async getEvents(next = 0) {
    const url = `${WB_CHAT_API}/api/v1/seller/events`;
    const { data } = await axios.get(url, {
      headers: this._headers(),
      params: { next },
      timeout: 15000,
    });
    const result = data?.result || data || {};
    return {
      events: result.events || result.messages || [],
      next:   result.next ?? next,
    };
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

  // Try to LINK the order to a buyer chat (if any) and ask for receipt code
  // before delivering the key. We DON'T send the key here — that happens in
  // processChatReplies, only after the buyer sends the "ХХХ ХХХ" code.
  // If no chat exists yet, the key still goes to Telegram for admin reference.
  let chatLinked = false;
  let chatError = null;
  let linkedChatId = null;
  const askText =
    'Здравствуйте! Чтобы получить ключ, отправьте, пожалуйста, только код получения в формате ХХХ ХХХ (например: 123 456). Сразу после этого пришлю ключ.';
  try {
    const chats = await wb.listChats().catch(() => []);
    const buyerId = order.userId || order.user?.id || order.customerId;
    let targetChat = null;
    if (buyerId) targetChat = chats.find(c => String(c.userId) === String(buyerId));
    if (!targetChat && chats.length === 1) targetChat = chats[0];

    if (targetChat) {
      linkedChatId = targetChat.id || targetChat.chatId;
      // Stash the key + order context against this chatId — code-validation
      // cron will read this when buyer replies. 7-day TTL covers the buyer's
      // "couple of days to figure it out" case without keeping forever.
      await redis.set(`wb:pending:${linkedChatId}`, JSON.stringify({
        orderId, sku, wbArticle, key,
        buyerId: buyerId || null,
        createdAt: new Date().toISOString(),
      }), { ex: 86400 * 7 });
      // Also map buyerId → chatId so a chat that opens AFTER the order can be
      // matched by replies cron via order.userId.
      if (buyerId) {
        await redis.set(`wb:pending_buyer:${buyerId}`, String(linkedChatId), { ex: 86400 * 7 });
      }
      await wb.sendChatMessage(linkedChatId, askText);
      chatLinked = true;
    }
  } catch (e) {
    chatError = e.response?.data || e.message;
    console.error('[wb] chat link failed:', chatError);
  }
  // chatDelivered kept for back-compat in records — true ONLY after code-validation,
  // never set true at order time anymore.
  const chatDelivered = false;

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

  // Mark order processed (90-day TTL). `chatLinked` says whether the chat
  // is currently waiting on a code; `chatDelivered` is set later by the
  // chat-replies sweep when we actually send the key.
  await redis.set(dkey, JSON.stringify({
    at: new Date().toISOString(),
    wbArticle, sku, keyTail: key.slice(-4),
    supplyId, chatLinked, chatDelivered,
    linkedChatId,
  }), { ex: 86400 * 90 });

  // Show both WB-article and inventory-pool SKU when they differ.
  const skuLine = wbArticle === sku
    ? `SKU <code>${sku}</code>`
    : `WB-артикул <code>${wbArticle}</code> → пул <code>${sku}</code>`;

  // Telegram alert.
  const summary = chatLinked
    ? `📨 <b>WB: ждём код получения от покупателя</b>\nЗаказ <code>${orderId}</code>\n${skuLine}\n\nЧат открыт — попросил у покупателя код в формате ХХХ ХХХ. Ключ отправится автоматически после получения кода.\n\n<b>На случай ручной отправки:</b>\n<code>${key}</code>`
    : `📨 <b>WB: заказ закрыт, чат пока не открыт</b>\nЗаказ <code>${orderId}</code>\n${skuLine}\n\n<b>Ключ для покупателя:</b>\n<code>${key}</code>\n\nКак только покупатель напишет в чат — бот попросит код получения и отправит ключ автоматически.`;
  await ymLib.notifyAdmin(summary);
  await ymLib.logEvent(redis, {
    type: 'wb_delivered', orderId, wbArticle, sku,
    keyTail: key.slice(-4), chatLinked, chatDelivered, supplyId,
    chatError: chatError ? String(chatError).slice(0, 200) : undefined,
  });

  return { delivered: true, wbArticle, sku, chatLinked, chatDelivered, supplyId };
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

// ── Chat-replies processing ────────────────────────────────────────────────
// Buyer must send their "код получения" in format `ХХХ ХХХ` (3 digits + space
// + 3 digits) before we hand over the Steam key. This sweep:
//   1. polls /api/v1/seller/events from the last cursor (kept in Redis);
//   2. for each new buyer message, looks up `wb:pending:<chatId>` (created
//      at order time when the chat existed) — or, if the chat was opened
//      AFTER the order, looks up `wb:pending_buyer:<userId>` to find the
//      pending state;
//   3. if message contains `\d{3}\s\d{3}` → sends the key, finalises;
//      else → replies asking for the code in the right format.
//
// Idempotency:
//   - cursor in Redis advances after each successful poll
//   - per-message guard `wb:msgseen:<chatId>:<msgId>` (24h TTL)
const RECEIPT_CODE_RX = /\b\d{3}\s\d{3}\b/;
const PROMPT_TEXT     = 'Пожалуйста, отправьте только код получения в формате ХХХ ХХХ (например: 123 456). После этого я сразу пришлю ключ.';
const PROMPT_REPEAT_GUARD_TTL = 60; // seconds — don't spam the prompt more than once a minute per chat

async function processChatReplies(wb, redis) {
  if (!wb.isConfigured()) return { processed: 0, results: [], note: 'WB not configured' };
  const ymLib = require('./lib-yandex-market');

  let cursor = 0;
  try {
    cursor = Number((await redis.get('wb:chat:cursor')) || 0) || 0;
  } catch {}

  let events, nextCursor;
  try {
    const r = await wb.getEvents(cursor);
    events     = r.events || [];
    nextCursor = r.next ?? cursor;
  } catch (e) {
    const errBody = e.response?.data || e.message;
    console.error('[wb] getEvents failed:', errBody);
    return { processed: 0, results: [], error: String(errBody).slice(0, 400) };
  }

  const results = [];
  for (const ev of events) {
    try {
      // Defensive — different WB API shapes carry these under different names.
      const chatId    = ev.chatId || ev.chat?.id || ev.message?.chatId;
      const msgId     = ev.id || ev.messageId || ev.message?.id;
      const fromBuyer = ev.fromBuyer ?? (ev.sender === 'BUYER') ?? (ev.author === 'buyer') ?? true;
      const text      = (ev.text || ev.message?.text || ev.body || '').toString();

      if (!chatId || !text) { results.push({ skip: 'no chatId/text', ev }); continue; }
      if (!fromBuyer)        { results.push({ skip: 'not from buyer', chatId, msgId }); continue; }

      // Per-message idempotency.
      if (msgId) {
        const seenKey = `wb:msgseen:${chatId}:${msgId}`;
        if (await redis.get(seenKey)) { results.push({ skip: 'already seen', chatId, msgId }); continue; }
        await redis.set(seenKey, '1', { ex: 86400 });
      }

      // Look up pending order context for this chat.
      let pendingRaw = await redis.get(`wb:pending:${chatId}`);
      // Fallback: chat was opened after the order — look up by buyerId carried on event.
      if (!pendingRaw) {
        const buyerId = ev.userId || ev.user?.id || ev.buyerId || ev.message?.userId;
        if (buyerId) {
          const linked = await redis.get(`wb:pending_buyer:${buyerId}`);
          if (linked) pendingRaw = await redis.get(`wb:pending:${linked}`);
          // Also write the chat→buyer mapping forward for next time.
          if (pendingRaw) {
            await redis.set(`wb:pending:${chatId}`, pendingRaw, { ex: 86400 * 7 });
          }
        }
      }

      if (!pendingRaw) {
        // No order linked to this chat — ignore. (Probably a follow-up after
        // delivery, or a chat for a non-digital order on the same account.)
        results.push({ skip: 'no pending order for chat', chatId });
        continue;
      }

      const pending = typeof pendingRaw === 'string' ? JSON.parse(pendingRaw) : pendingRaw;
      const match   = RECEIPT_CODE_RX.exec(text);

      if (match) {
        // Code accepted — send the key.
        const keyMsg = `Ваш ключ Steam: ${pending.key}\n\nАктивация:\n1) Откройте Steam → Игры → Активировать через Steam.\n2) Или вставьте ключ на странице https://store.steampowered.com/account/registerkey\n\nЕсли ключ не подошёл — напишите в этом чате, заменим.`;
        try {
          await wb.sendChatMessage(chatId, keyMsg);
        } catch (e) {
          // Don't drop the pending state if WB chat send fails — try again next tick.
          const errBody = e.response?.data || e.message;
          console.error('[wb] sendChatMessage (key) failed:', errBody);
          results.push({ chatId, error: 'send key failed', detail: String(errBody).slice(0, 200) });
          continue;
        }
        // Finalise — clean state, update delivered record, log+notify.
        await redis.del(`wb:pending:${chatId}`);
        if (pending.buyerId) await redis.del(`wb:pending_buyer:${pending.buyerId}`);
        const dkey = `wb:delivered:${pending.orderId}`;
        const prev = await redis.get(dkey);
        if (prev) {
          const rec = typeof prev === 'string' ? JSON.parse(prev) : prev;
          rec.chatDelivered = true;
          rec.chatDeliveredAt = new Date().toISOString();
          rec.codeMatched = match[0];
          await redis.set(dkey, JSON.stringify(rec), { ex: 86400 * 90 });
        }
        await ymLib.logEvent(redis, {
          type: 'wb_chat_key_sent', orderId: pending.orderId, chatId, code: match[0],
          keyTail: pending.key.slice(-4),
        });
        await ymLib.notifyAdmin(`✅ <b>WB: ключ отправлен в чат</b>\nЗаказ <code>${pending.orderId}</code>\nКод покупателя: <code>${match[0]}</code>\nКлюч: ...${pending.key.slice(-4)}`);
        results.push({ chatId, orderId: pending.orderId, delivered: true, code: match[0] });
      } else {
        // No code yet — politely ask, but don't spam (1 prompt per chat per minute).
        const guardKey = `wb:promptsent:${chatId}`;
        if (await redis.get(guardKey)) {
          results.push({ chatId, skip: 'prompt rate-limited' });
          continue;
        }
        await redis.set(guardKey, '1', { ex: PROMPT_REPEAT_GUARD_TTL });
        try {
          await wb.sendChatMessage(chatId, PROMPT_TEXT);
          results.push({ chatId, prompted: true });
        } catch (e) {
          const errBody = e.response?.data || e.message;
          console.error('[wb] sendChatMessage (prompt) failed:', errBody);
          results.push({ chatId, error: 'prompt failed', detail: String(errBody).slice(0, 200) });
        }
      }
    } catch (e) {
      results.push({ error: e.message });
    }
  }

  // Advance cursor only if we got past getEvents (which we did).
  if (nextCursor != null && nextCursor !== cursor) {
    await redis.set('wb:chat:cursor', String(nextCursor));
  }

  return { processed: results.length, cursor: nextCursor, results };
}

module.exports = {
  WildberriesAPI,
  processOrder,
  sweepNewOrders,
  processChatReplies,
};
