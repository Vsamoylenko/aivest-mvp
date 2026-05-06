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

// nmID → inventory SKU map. WB DBS-digital flow gives us only `goodCard.nmID`
// (the WB article number) on chat events, NOT Артикул продавца. Map both of
// the seller's known WB cards to the shared MRKT-JU4L95I3 pool.
//   1011858308 = "Случайная игра"     (seller-art WBRANDOMRL)
//   1009613157 = "Silver-Gold Игра"   (seller-art MRKT-JU4L95I3)
const DEFAULT_NMID_MAP = {
  '1011858308': 'MRKT-JU4L95I3',
  '1009613157': 'MRKT-JU4L95I3',
};
function loadNmIdMap() {
  const raw = (process.env.WB_NMID_MAP || '').trim();
  if (!raw) return DEFAULT_NMID_MAP;
  try {
    return { ...DEFAULT_NMID_MAP, ...JSON.parse(raw) };
  } catch {
    console.warn('[wb] WB_NMID_MAP env var is not valid JSON — using defaults');
    return DEFAULT_NMID_MAP;
  }
}
const NMID_MAP = loadNmIdMap();

function clean(v) { return (v == null ? '' : String(v)).trim(); }
// Resolve WB seller-article → shared inventory pool SKU. If no alias exists,
// fall back to the article itself (so `MRKT-…` always works without mapping).
function resolveInventorySku(wbArticle) {
  const a = clean(wbArticle);
  return SKU_MAP[a] || a;
}
function resolveSkuByNmId(nmId) {
  return NMID_MAP[String(nmId)] || null;
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
  // Empirically (2026-05) WB stores incoming events as `message:{text:"..."}`
  // (a NESTED object). Sending with flat `text` / `message` / `messageText`
  // fields produces a 200 OK but the message is recorded as `message:{}` —
  // i.e. WB extracts text only when we mirror the event shape.
  // Final body shape: `{ chatID, replySign, message: { text: "..." } }`.
  async sendChatMessage(chatID, text, replySign) {
    const url = `${WB_CHAT_API}/api/v1/seller/message`;
    const body = {
      chatID,
      message: { text },
    };
    if (replySign) body.replySign = replySign;
    const { data } = await axios.post(url, body, { headers: this._headers(), timeout: 15000 });
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
      // We also record the actual WB receipt code here (if WB exposes it on
      // the order object) so processChatReplies can verify, not just regex.
      // Defensive read — different WB schema versions carry the receipt code
      // under different names. Falsy means "no real code, accept any XXX XXX".
      const realCode = clean(
        order.code || order.receiptCode || order.userInfo?.code ||
        order.confirmCode || order.deliveryCode || ''
      );
      await redis.set(`wb:pending:${linkedChatId}`, JSON.stringify({
        orderId, sku, wbArticle, key,
        buyerId: buyerId || null,
        realCode: realCode || null,
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

// ── Chat-replies processing (DBS-digital flow) ────────────────────────────
// Reality of WB DBS-digital goods: orders DON'T appear in /api/v3/orders/new.
// Instead, when a buyer pays, WB opens a buyer-seller chat and the buyer's
// "Задание" surfaces only as an event with a `goodCard.nmID` reference.
// The seller's "delivery" is sending the key in chat — that's it. No supply,
// no order ID. Idempotency keys off `goodCard.rid` (per-purchase identifier).
//
// Buyer flow:
//   1. Buyer pays, gets `код получения` in format `ХХХ ХХХ` from WB.
//   2. Buyer writes anything in the chat. WB pushes it as an event with
//      `eventType:"message"`, `sender:"client"`, `message.attachments.goodCard`,
//      `message.text` and `replySign`.
//   3. We poll /api/v1/seller/events:
//      - if message contains `\d{3}\s\d{3}` → resolve goodCard.nmID → SKU,
//        pop key, send via chat (with replySign), mark `wb:delivered_rid:<rid>`;
//      - else → reply asking for the code in correct format.
//
// Idempotency:
//   - cursor `wb:chat:cursor` advances after each successful poll
//   - per-rid guard `wb:delivered_rid:<rid>` (90-day TTL) — no double-delivery
//   - per-message guard `wb:msgseen:<chatID>:<eventID>` (24h TTL), set ONLY
//     after we've taken action so we can retry on transient failures.
const RECEIPT_CODE_RX = /\b\d{3}\s\d{3}\b/;
const PROMPT_TEXT     = 'Здравствуйте! Пришлите, пожалуйста, только код получения в формате ХХХ ХХХ (например: 123 456). Сразу после этого отправлю ключ Steam.';
const PROMPT_REPEAT_GUARD_TTL = 60; // seconds — don't spam the prompt more than once a minute per chat
const ACTIVATION_HELP =
  '\n\nАктивация ключа:\n' +
  '1) Откройте Steam → Игры → Активировать через Steam.\n' +
  '2) Или вставьте ключ на странице https://store.steampowered.com/account/registerkey\n\n' +
  'Если ключ не подошёл — напишите в этом чате, заменим.';

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
      // ── Real WB schema (as of 2026-05) ──────────────────────────────────
      //   ev.chatID, ev.eventID, ev.eventType, ev.replySign,
      //   ev.message: { text, addTimestamp, sender, clientName,
      //                 attachments: { goodCard: { nmID, rid, name } } }
      const chatID    = ev.chatID || ev.chatId || ev.chat?.id;
      const eventID   = ev.eventID || ev.id || ev.messageId;
      const eventType = ev.eventType || ev.type || 'message';
      const replySign = ev.replySign || ev.message?.replySign;
      const sender    = ev.sender || ev.message?.sender || (ev.fromBuyer ? 'client' : 'seller');
      const text      = (ev.message?.text || ev.text || ev.body || '').toString();
      const goodCard  = ev.message?.attachments?.goodCard || ev.goodCard || {};
      const nmID      = goodCard.nmID || ev.nmID;
      const rid       = goodCard.rid  || ev.rid;
      const clientName = ev.clientName || ev.message?.clientName || '';

      if (eventType !== 'message') { results.push({ skip: `eventType=${eventType}`, chatID }); continue; }
      if (!chatID)                 { results.push({ skip: 'no chatID', ev: JSON.stringify(ev).slice(0,300) }); continue; }
      if (sender !== 'client')     { results.push({ skip: `sender=${sender}`, chatID }); continue; }

      // Per-message idempotency check. Don't mark seen yet — only after action.
      const seenKey = eventID ? `wb:msgseen:${chatID}:${eventID}` : null;
      if (seenKey && await redis.get(seenKey)) {
        results.push({ skip: 'already seen', chatID, eventID });
        continue;
      }

      // Per-rid delivery guard — once a key is sent for this purchase, don't repeat.
      const ridDeliveredKey = rid ? `wb:delivered_rid:${rid}` : null;
      if (ridDeliveredKey && await redis.get(ridDeliveredKey)) {
        // Buyer is messaging after delivery — no-op (could be questions / thanks).
        if (seenKey) await redis.set(seenKey, '1', { ex: 86400 });
        results.push({ chatID, rid, skip: 'rid already delivered' });
        continue;
      }

      // Resolve product → inventory pool SKU.
      let sku = nmID ? resolveSkuByNmId(nmID) : null;

      // ── Follow-up handling ─────────────────────────────────────────────
      // If this message has no goodCard (typical for buyer's 2nd, 3rd... msgs
      // in the same chat), look up whether we've already delivered a key in
      // this chat. If yes — re-send the previously-issued key (so buyer who
      // missed/can't see the original gets it again) and don't pop a new one.
      // We track per-chat delivery in `wb:chat_last_key:<chatID>`.
      if (!sku && !nmID) {
        const priorRaw = await redis.get(`wb:chat_last_key:${chatID}`);
        if (priorRaw) {
          const prior = typeof priorRaw === 'string' ? JSON.parse(priorRaw) : priorRaw;
          const keyMsg = `Ваш ключ Steam (повторно): ${prior.key}` + ACTIVATION_HELP;
          try {
            await wb.sendChatMessage(chatID, keyMsg, replySign);
          } catch (e) {
            const errBody = e.response?.data || e.message;
            console.error('[wb] resend key failed:', errBody);
            results.push({ chatID, error: 'resend failed', detail: String(errBody).slice(0, 200) });
            continue;
          }
          if (seenKey) await redis.set(seenKey, '1', { ex: 86400 });
          await ymLib.logEvent(redis, {
            type: 'wb_chat_key_resent', chatID, sku: prior.sku, keyTail: prior.key.slice(-4),
          });
          await ymLib.notifyAdmin(`🔁 <b>WB: ключ отправлен повторно</b>\nЧат <code>${chatID}</code>\nКлюч: ...${prior.key.slice(-4)}\nПокупатель прислал follow-up — повторили выдачу.`);
          results.push({ chatID, resent: true, keyTail: prior.key.slice(-4) });
          continue;
        }
      }

      if (!sku) {
        // Unknown nmID and no prior delivery in this chat — alert admin once.
        if (rid) {
          const alertKey = `wb:unknown_nmid_alerted:${rid}`;
          if (!(await redis.get(alertKey))) {
            await redis.set(alertKey, '1', { ex: 86400 * 7 });
            await ymLib.notifyAdmin(`⚠️ <b>WB: неизвестный nmID</b>\nnmID <code>${nmID}</code>\nrid <code>${rid}</code>\nПокупатель: ${clientName}\nДобавь nmID в DEFAULT_NMID_MAP / WB_NMID_MAP и redeploy.`);
          }
        } else {
          // No goodCard AND no prior delivery — buyer messaging in a chat
          // we have no record of. Alert admin once per chat.
          const alertKey = `wb:no_context_alerted:${chatID}`;
          if (!(await redis.get(alertKey))) {
            await redis.set(alertKey, '1', { ex: 86400 });
            await ymLib.notifyAdmin(`⚠️ <b>WB: чат без контекста</b>\nЧат <code>${chatID}</code>\nПокупатель ${clientName} пишет: <i>${text.slice(0, 200)}</i>\nНет goodCard в сообщении и нет предыдущей выдачи. Возможно, надо ответить вручную.`);
          }
        }
        if (seenKey) await redis.set(seenKey, '1', { ex: 86400 });
        results.push({ chatID, nmID, skip: 'unknown nmID / no context' });
        continue;
      }

      const match = RECEIPT_CODE_RX.exec(text);

      if (match) {
        // ── Code OK → pop key from inventory and send. ────────────────────
        const stockKey = `ym:keys:${sku}`;
        const stock = await redis.llen(stockKey);
        if (!stock) {
          await ymLib.notifyAdmin(`⚠️ <b>WB: нет ключей для выдачи</b>\nПул <code>${sku}</code> пуст\nrid <code>${rid}</code>\nПокупатель ${clientName} прислал код <code>${match[0]}</code> — нужно срочно залить ключи.`);
          // Don't mark seen — retry next tick after admin tops up.
          results.push({ chatID, rid, error: 'out of stock', sku });
          continue;
        }
        const key = await ymLib.popKey(redis, sku);
        if (!key) {
          results.push({ chatID, rid, error: 'popKey returned empty', sku });
          continue;
        }
        const keyMsg = `Ваш ключ Steam: ${key}` + ACTIVATION_HELP;
        try {
          await wb.sendChatMessage(chatID, keyMsg, replySign);
        } catch (e) {
          // Restore the key — we'll retry next tick.
          await redis.lpush(stockKey, key);
          const errBody = e.response?.data || e.message;
          console.error('[wb] sendChatMessage (key) failed:', errBody);
          results.push({ chatID, rid, error: 'send key failed', detail: String(errBody).slice(0, 200) });
          continue;
        }
        // Mark delivered (90d TTL) — by rid (per-purchase) and by chat
        // (so future buyer messages in same chat can re-send if needed).
        const deliveredRecord = JSON.stringify({
          at: new Date().toISOString(), chatID, sku, nmID,
          keyTail: key.slice(-4), code: match[0], clientName,
          key, // full key — needed for resend
        });
        if (ridDeliveredKey) {
          await redis.set(ridDeliveredKey, deliveredRecord, { ex: 86400 * 90 });
        }
        await redis.set(`wb:chat_last_key:${chatID}`, deliveredRecord, { ex: 86400 * 90 });
        if (seenKey) await redis.set(seenKey, '1', { ex: 86400 });
        await ymLib.logEvent(redis, {
          type: 'wb_chat_key_sent', chatID, rid, nmID, sku,
          code: match[0], keyTail: key.slice(-4), clientName,
        });
        await ymLib.notifyAdmin(
          `✅ <b>WB: ключ отправлен в чат</b>\n` +
          `Покупатель: ${clientName}\n` +
          `Карточка: nmID <code>${nmID}</code> → пул <code>${sku}</code>\n` +
          `Код: <code>${match[0]}</code>\n` +
          `Ключ: ...${key.slice(-4)}`
        );
        results.push({ chatID, rid, delivered: true, code: match[0], sku, keyTail: key.slice(-4) });
      } else {
        // ── No code → ask politely. ──────────────────────────────────────
        const guardKey = `wb:promptsent:${chatID}`;
        if (await redis.get(guardKey)) {
          if (seenKey) await redis.set(seenKey, '1', { ex: 86400 });
          results.push({ chatID, skip: 'prompt rate-limited' });
          continue;
        }
        await redis.set(guardKey, '1', { ex: PROMPT_REPEAT_GUARD_TTL });
        try {
          await wb.sendChatMessage(chatID, PROMPT_TEXT, replySign);
          if (seenKey) await redis.set(seenKey, '1', { ex: 86400 });
          results.push({ chatID, prompted: true });
        } catch (e) {
          const errBody = e.response?.data || e.message;
          console.error('[wb] sendChatMessage (prompt) failed:', errBody);
          results.push({ chatID, error: 'prompt failed', detail: String(errBody).slice(0, 200) });
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
