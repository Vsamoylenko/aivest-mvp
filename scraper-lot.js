// AIvest — Lot-online.ru (РАД) Scraper
// Парсит государственные аукционы недвижимости с api1.lot-online.ru
// Usage: node scraper-lot.js          → merge auction lots into properties.json
//        node scraper-lot.js --dry    → print stats without saving

require('dotenv').config();
// Force synchronous stdout/stderr so progress lines aren't buffered when piped
if (process.stdout._handle && process.stdout._handle.setBlocking) process.stdout._handle.setBlocking(true);
if (process.stderr._handle && process.stderr._handle.setBlocking) process.stderr._handle.setBlocking(true);
const axios   = require('axios');
const cheerio = require('cheerio');
const fs      = require('fs');
const path    = require('path');
const https   = require('https');
const http    = require('http');
const { URL } = require('url');

// Force non-keepalive agents to avoid socket reuse hangs
const HTTPS_AGENT = new https.Agent({ keepAlive: false, timeout: 10000 });
const HTTP_AGENT  = new http.Agent({ keepAlive: false, timeout: 10000 });

// Low-level fetch that guarantees socket cleanup on timeout
function rawGet(urlStr, timeoutMs = 12000) {
  return new Promise((resolve, reject) => {
    let done = false;
    const u = new URL(urlStr);
    const mod = u.protocol === 'https:' ? https : http;
    const req = mod.request({
      method: 'GET',
      hostname: u.hostname,
      port: u.port || (u.protocol === 'https:' ? 443 : 80),
      path: u.pathname + u.search,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
        'Accept-Language': 'ru-RU,ru;q=0.9',
        'Accept': 'text/html,application/xhtml+xml',
        'Connection': 'close',
      },
      agent: u.protocol === 'https:' ? HTTPS_AGENT : HTTP_AGENT,
    }, res => {
      // Follow one redirect manually
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        const next = new URL(res.headers.location, urlStr).toString();
        if (!done) { done = true; clearTimeout(killer); rawGet(next, timeoutMs).then(resolve, reject); }
        return;
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        if (done) return;
        done = true; clearTimeout(killer);
        resolve({ status: res.statusCode, data: Buffer.concat(chunks).toString('utf8') });
      });
      res.on('error', e => { if (!done) { done = true; clearTimeout(killer); reject(e); } });
    });
    const killer = setTimeout(() => {
      if (done) return;
      done = true;
      try { req.destroy(new Error('timeout')); } catch {}
      reject(new Error('timeout'));
    }, timeoutMs);
    req.on('error', e => { if (!done) { done = true; clearTimeout(killer); reject(e); } });
    req.setTimeout(timeoutMs, () => { try { req.destroy(new Error('socket-timeout')); } catch {} });
    req.end();
  });
}

const DATA_DIR  = path.join(__dirname, 'data');
const OUT_FILE  = path.join(DATA_DIR, 'properties.json');
const CACHE_FILE = path.join(DATA_DIR, 'lot-details-cache.json');
const DELAY_MS  = parseInt(process.env.LOT_DELAY || '800');
const REQ_TIMEOUT = parseInt(process.env.LOT_TIMEOUT || '12000');
const BASE_URL  = 'https://api1.lot-online.ru/index.php';
const SOURCE    = 'auct';

// ── Категории недвижимости на lot-online.ru ───────────────────────────────────
const LOT_CATEGORIES = [
  { id: 34, type: 'apartment', label: 'Квартиры' },
  { id: 38, type: 'house',     label: 'Дома/коттеджи' },
  { id: 26, type: 'parking',   label: 'Паркинги/гаражи' },
  // { id: 17, type: 'commercial', label: 'Коммерческая' }, // ~1400 лотов, медленно
];

// Известные города в AIvest (для фильтрации — только по ним)
const KNOWN_CITIES = ['Москва', 'Санкт-Петербург', 'Краснодар', 'Сочи', 'Казань', 'Новосибирск', 'Екатеринбург'];

const CITY_CONFIG = {
  'Москва':          { rentPpm: 850,  growth: 9.8,  marketPpm: 240000 },
  'Санкт-Петербург': { rentPpm: 680,  growth: 8.5,  marketPpm: 185000 },
  'Краснодар':       { rentPpm: 430,  growth: 11.0, marketPpm: 105000 },
  'Сочи':            { rentPpm: 920,  growth: 13.1, marketPpm: 280000 },
  'Казань':          { rentPpm: 450,  growth: 9.1,  marketPpm: 125000 },
  'Новосибирск':     { rentPpm: 370,  growth: 8.8,  marketPpm: 130000 },
  'Екатеринбург':    { rentPpm: 400,  growth: 9.4,  marketPpm: 115000 },
};

// ── Helpers ───────────────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function extractCity(text) {
  if (!text) return null;
  // Detect known cities in address/title text
  for (const city of KNOWN_CITIES) {
    if (text.includes(city)) return city;
  }
  // Special patterns
  if (/\bМоскв/i.test(text))        return 'Москва';
  if (/Санкт-Петербург|СПб\b/i.test(text)) return 'Санкт-Петербург';
  if (/Краснодар/i.test(text))       return 'Краснодар';
  if (/Сочи/i.test(text))            return 'Сочи';
  if (/Казань/i.test(text))          return 'Казань';
  if (/Новосибирск/i.test(text))     return 'Новосибирск';
  if (/Екатеринбург/i.test(text))    return 'Екатеринбург';
  return null;
}

function parseArea(text) {
  // "273,2 кв. м" / "273.2 м²" / "273 кв.м"
  const m = text.match(/([\d\s]+[,.]?\d*)\s*(?:кв\.?\s*м|м²)/i);
  if (!m) return 0;
  return parseFloat(m[1].replace(/\s/g, '').replace(',', '.')) || 0;
}

function parsePrice(text) {
  // "152 550 000 ₽" / "152,550,000 руб"
  const m = text.match(/([\d\s,]+)\s*(?:₽|руб)/i);
  if (!m) return 0;
  return parseInt(m[1].replace(/[\s,]/g, ''), 10) || 0;
}

// ── Score calculation (same logic as scraper.js) ──────────────────────────────
function calcScore({ disc, roi, grow, liq, vac }) {
  const discScore  = disc > 0 ? 30 * (1 - Math.exp(-disc / 14)) : Math.max(0, 30 + disc * 0.4);
  const roiScore   = roi > 0 ? Math.min(28, 10 * Math.log(1 + roi * 0.8)) : 0;
  const growScore  = Math.min(22, grow * 1.47);
  const liqScore   = liq * 1.2;
  const vacPenalty = vac > 5 ? (vac - 5) * 0.9 : 0;
  const rawSum     = discScore + roiScore + growScore + liqScore - vacPenalty;
  const bonus      = (discScore > 20 && roiScore > 18 && growScore > 14) ? 5 : 0;
  return Math.min(99, Math.max(0, Math.round(rawSum + bonus)));
}

function estimateLiquidity(city, type) {
  let base = { 'Москва': 8.5, 'Санкт-Петербург': 8, 'Сочи': 7.5, 'Казань': 7,
               'Краснодар': 7, 'Новосибирск': 7, 'Екатеринбург': 7 }[city] || 6.5;
  if (type === 'land') base -= 2;
  if (type === 'commercial') base -= 0.5;
  return Math.round(Math.max(1, Math.min(10, base)));
}

function estimateVacancy(type, city) {
  if (type === 'land') return 0;
  if (city === 'Сочи') return 10;
  if (type === 'commercial') return 6;
  return 4;
}

// ── Parse one product card from HTML ─────────────────────────────────────────
function parseCard($, el, categoryType) {
  const $el = $(el);

  // Product ID from link
  const link = $el.find('a[href*="product_id"]').first().attr('href') || '';
  const idMatch = link.match(/product_id=(\d+)/);
  if (!idMatch) return null;
  const productId = idMatch[1];

  // Title / address
  const title = $el.find('.ty-grid-list__item-name, .product-title, [class*="title"], [class*="name"] a')
    .first().text().trim() ||
    $el.find('a[href*="product_id"]').first().text().trim();

  // Lot number
  const cardText = $el.text();
  const lotMatch = cardText.match(/РАД-(\d+)/);
  const lotNumber = lotMatch ? `РАД-${lotMatch[1]}` : '';

  // Price — look for "Начальная цена" pattern
  const priceMatch = cardText.match(/Начальная цена\s*([\d\s,]+)\s*(?:₽|руб)/i)
    || cardText.match(/([\d\s]{5,})\s*₽/);
  const priceRub = priceMatch ? parseInt(priceMatch[1].replace(/[\s,]/g, ''), 10) : 0;

  // Area
  const area = parseArea(cardText);

  // City from title/address
  const city = extractCity(title) || extractCity(cardText);

  // Image
  const imgSrc = $el.find('img').first().attr('src') || '';
  const photo = imgSrc && imgSrc.startsWith('http') ? imgSrc
    : imgSrc ? `https://api1.lot-online.ru${imgSrc}` : '';

  return { productId, lotNumber, priceRub, area, title, city, photo, type: categoryType };
}

// ── Fetch and parse one category page ────────────────────────────────────────
async function fetchCategoryPage(categoryId, categoryType, page) {
  const url = `${BASE_URL}?dispatch=categories.view&category_id=${categoryId}&page=${page}&items_per_page=24`;
  const res = await rawGet(url, 20000);
  if (!res || res.status !== 200) {
    const err = new Error(`HTTP ${res ? res.status : 'null'}`);
    err.response = { status: res ? res.status : 0 };
    throw err;
  }

  const $ = cheerio.load(res.data);

  // Detect total pages from pagination
  let totalPages = 1;
  const paginationText = $('.ty-pagination, [class*="pagination"]').text();
  const lastPageLink = $('a[href*="page="]').last().attr('href') || '';
  const lastPageMatch = lastPageLink.match(/page=(\d+)/);
  if (lastPageMatch) totalPages = Math.max(totalPages, parseInt(lastPageMatch[1]));

  // Find product cards — CS-Cart uses .ty-column3 or .ty-grid-list__item
  const selectors = [
    '.ty-grid-list__item',
    '.ty-column3',
    '.ty-column4',
    '[class*="product-item"]',
    '[class*="product-card"]',
    '[id*="product_"]',
  ];
  let items = $();
  for (const sel of selectors) {
    items = $(sel);
    if (items.length > 0) break;
  }

  // Fallback: find all links with product_id
  if (items.length === 0) {
    const products = new Set();
    $('a[href*="product_id"]').each((_, a) => {
      const href = $(a).attr('href') || '';
      const m = href.match(/product_id=(\d+)/);
      if (m) products.add(m[1]);
    });
    return { items: [], totalPages, rawProductIds: [...products] };
  }

  const results = [];
  items.each((_, el) => {
    const parsed = parseCard($, el, categoryType);
    if (parsed && parsed.productId && parsed.priceRub > 0) results.push(parsed);
  });

  return { items: results, totalPages };
}

// ── Fetch individual product page for extra detail ────────────────────────────
async function fetchProductDetail(productId) {
  try {
    const url = `${BASE_URL}?dispatch=products.view&product_id=${productId}`;
    const res = await rawGet(url, REQ_TIMEOUT);
    if (!res || res.status !== 200 || !res.data) return null;

    const $ = cheerio.load(res.data);
    const bodyText = $('body').text();
    const pageHtml = res.data;

    // Extract area from page
    const area = parseArea(bodyText);

    // Extract city
    const city = extractCity(bodyText);

    // Extract district (район)
    const districtMatch = bodyText.match(/(?:муниципальный округ|район)\s+([\wА-ЯЁа-яё\s-]+?)(?:\s*,|\s*\n)/i);
    const district = districtMatch ? districtMatch[1].trim() : '';

    // Extract floor info
    const floorMatch = bodyText.match(/этаж[:\s]*(\d+)\s*(?:из|\/)\s*(\d+)/i)
      || bodyText.match(/(\d+)\s*\/\s*(\d+)\s*эт/i);
    const floor = floorMatch ? `${floorMatch[1]}/${floorMatch[2]}` : '—';

    // Extract full address from structured data or text
    const addrMatch = bodyText.match(/(?:Адрес|адрес)[:\s]*((?:Российская Федерация,\s*)?[^.]{10,150})(?:\n|Кадастр)/i);
    const fullAddress = addrMatch ? addrMatch[1].trim() : '';

    // Lot number
    const lotMatch = bodyText.match(/РАД-(\d+)/);
    const lotNumber = lotMatch ? `РАД-${lotMatch[1]}` : '';

    // Photos
    const photos = [];
    $(res.data).find ? null : null;
    $('img[src*="cdn"], img[src*="/images/"]').each((_, img) => {
      const src = $(img).attr('src') || '';
      if (src && (src.includes('/cdn/') || src.match(/\.(jpg|jpeg|png|webp)/i))) {
        const full = src.startsWith('http') ? src : `https://api1.lot-online.ru${src}`;
        if (!photos.includes(full)) photos.push(full);
      }
    });

    // Description
    const descEl = $('.ty-product-description, [class*="description"]').first().text().trim();

    // Minimum price (public offer)
    const minPriceMatch = bodyText.match(/(?:Минимальная|минимальная) цена\s*([\d\s,]+)\s*(?:₽|руб)/i);
    const minPrice = minPriceMatch ? parseInt(minPriceMatch[1].replace(/[\s,]/g, ''), 10) : 0;

    return { area, city, district, floor, lotNumber, photos: photos.slice(0, 5), fullAddress, description: descEl, minPrice };
  } catch {
    return null;
  }
}

// ── Convert lot to AIvest property format ────────────────────────────────────
function toLotProperty(card, detail, lotId) {
  const city = detail?.city || card.city;
  if (!city) return null;

  const cfg = CITY_CONFIG[city];
  if (!cfg) return null;  // only known cities

  const area     = detail?.area || card.area || 0;
  const priceRub = card.priceRub;
  const price    = Math.round((priceRub / 1e6) * 10) / 10;  // млн ₽
  const ppm      = area > 0 ? Math.round(priceRub / area) : 0;
  const mktPpm   = cfg.marketPpm;

  // Skip if price is 0 or area is 0
  if (price < 0.5 || area <= 0) return null;
  // Skip if per-meter price is unrealistically low
  if (ppm > 0 && ppm < mktPpm * 0.05) return null;
  // Skip huge commercial lots (>50 мln for apartments is OK, but filter 1B+ for apartments)
  if (card.type === 'apartment' && price > 500) return null;
  if (card.type === 'apartment' && area >= 500) return null;

  const disc  = mktPpm > 0 ? Math.round(((mktPpm - ppm) / mktPpm) * 100 * 10) / 10 : 0;
  const vac   = estimateVacancy(card.type, city);
  const liq   = estimateLiquidity(city, card.type);
  const grow  = cfg.growth;
  const monthlyRent = Math.round(cfg.rentPpm * area);
  const roi   = price > 0 ? Math.round((monthlyRent * 12 * (1 - vac / 100) / (price * 1e6)) * 100 * 10) / 10 : 0;
  const score = calcScore({ disc, roi, grow, liq, vac });

  const district = detail?.district || '';
  const floor    = detail?.floor || '—';

  // Title
  const typeNames = { apartment:'Квартира', house:'Дом', commercial:'Коммерческая', parking:'Паркинг', land:'Участок' };
  const typeName = typeNames[card.type] || 'Объект';
  const loc = district || city;
  const titleBase = card.title?.slice(0, 60) || `${typeName}, ${loc}`;

  let badge = '';
  if (disc >= 20) badge = 'Ниже рынка';
  else if (disc >= 10) badge = 'Аукцион';
  else if (card.type === 'apartment') badge = 'Аукцион';

  const photos = detail?.photos?.length ? detail.photos : (card.photo ? [card.photo] : []);

  return {
    id:        lotId,
    cianId:    `lot-${card.productId}`,
    cianUrl:   `https://api1.lot-online.ru/index.php?dispatch=products.view&product_id=${card.productId}`,
    title:     titleBase,
    city,
    district,
    metro:     '',
    area:      Math.round(area),
    floor,
    type:      card.type,
    source:    SOURCE,
    price,
    ppm:       Math.round(ppm / 1000),
    mkt:       Math.round(mktPpm / 1000),
    rent:      Math.round(monthlyRent / 1000),
    vac, grow, liq, badge, score, disc, roi,
    photos,
    description: (detail?.description || '').slice(0, 200),
    lotNumber:   card.lotNumber || detail?.lotNumber || '',
    addedAt:   new Date().toISOString(),
    scrapedAt: new Date().toISOString(),
  };
}

// ── Main scrape ───────────────────────────────────────────────────────────────
async function scrapeLotOnline() {
  console.log(`\n🏛️  AIvest Lot-Online Scraper — ${new Date().toLocaleString('ru-RU')}`);
  console.log('   Источник: РАД (Российский аукционный дом)\n');

  const allCards = [];
  const seenProductIds = new Set();

  // Phase 1: collect all listing cards from category pages
  for (const cat of LOT_CATEGORIES) {
    process.stdout.write(`  📂 ${cat.label} (cat ${cat.id}): `);
    let pageNum = 1;
    let totalAdded = 0;

    while (true) {
      try {
        const { items, totalPages, rawProductIds } = await fetchCategoryPage(cat.id, cat.type, pageNum);

        if (rawProductIds) {
          // Fallback: got raw product IDs without card parsing
          for (const pid of rawProductIds) {
            if (!seenProductIds.has(pid)) {
              seenProductIds.add(pid);
              allCards.push({ productId: pid, type: cat.type, priceRub: 0, area: 0 });
              totalAdded++;
            }
          }
          process.stdout.write(`стр.${pageNum}(+${rawProductIds.length}) `);
        } else {
          if (items.length === 0) {
            process.stdout.write(`стр.${pageNum} пусто\n`);
            break;
          }
          for (const card of items) {
            if (!seenProductIds.has(card.productId)) {
              seenProductIds.add(card.productId);
              allCards.push(card);
              totalAdded++;
            }
          }
          process.stdout.write(`стр.${pageNum}(+${items.length}) `);
        }

        await sleep(DELAY_MS);
        pageNum++;
        if (pageNum > totalPages && totalPages > 1) break;
        if (pageNum > 20) break; // safety cap
      } catch (err) {
        const msg = err.response ? `HTTP ${err.response.status}` : err.message?.slice(0, 50);
        process.stdout.write(`❌(${msg}) `);
        if (err.response?.status === 403 || err.response?.status === 429) break;
        await sleep(3000);
        break;
      }
    }

    console.log(`→ итого ${totalAdded}`);
  }

  console.log(`\n  Карточек собрано: ${allCards.length}`);

  // Phase 2: fetch details for cards that need area/city
  const needsDetail = allCards.filter(c => !c.city || c.area === 0);
  console.log(`  Нужны детали: ${needsDetail.length} объектов`);

  if (process.argv.includes('--dry')) {
    const withCity  = allCards.filter(c => c.city).length;
    const withArea  = allCards.filter(c => c.area > 0).length;
    const withPrice = allCards.filter(c => c.priceRub > 0).length;
    console.log(`  С городом:   ${withCity}/${allCards.length}`);
    console.log(`  С площадью:  ${withArea}/${allCards.length}`);
    console.log(`  С ценой:     ${withPrice}/${allCards.length}`);
    console.log('\n  --dry: детальный фетч пропущен, сохранение пропущено');
    return;
  }

  console.log(`  Получение деталей...\n`);

  // Load cache (resume)
  const detailMap = new Map();
  if (fs.existsSync(CACHE_FILE)) {
    try {
      const cached = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
      for (const [pid, det] of Object.entries(cached)) detailMap.set(pid, det);
      console.log(`  Кэш: загружено ${detailMap.size} деталей`);
    } catch (e) { console.warn('  Кэш повреждён, игнорирую:', e.message); }
  }
  const saveCache = () => {
    try {
      const obj = {};
      for (const [k, v] of detailMap.entries()) obj[k] = v;
      fs.writeFileSync(CACHE_FILE, JSON.stringify(obj));
    } catch (e) { console.warn('  Ошибка записи кэша:', e.message); }
  };

  let detailFetched = 0;
  const tStart = Date.now();

  const LOG_EVERY = parseInt(process.env.LOT_LOG_EVERY || '10');
  for (const card of needsDetail) {
    const pid = String(card.productId);
    let detail = detailMap.get(pid) || null;
    if (!detail) {
      try {
        detail = await Promise.race([
          fetchProductDetail(card.productId),
          new Promise((_, rej) => setTimeout(() => rej(new Error('hard-timeout')), REQ_TIMEOUT + 5000)),
        ]);
        if (detail) detailMap.set(pid, detail);
      } catch (e) {
        if (detailFetched < 3) console.log(`  [debug] pid=${pid} err=${e.message}`);
      }
      await sleep(DELAY_MS);
    }
    if (detail) {
      if (!card.city && detail.city) card.city = detail.city;
      if (!card.area && detail.area) card.area = detail.area;
      if (!card.priceRub && detail.minPrice) card.priceRub = detail.minPrice;
    }
    detailFetched++;
    if (detailFetched <= 3 || detailFetched % LOG_EVERY === 0) {
      const rate = detailFetched / ((Date.now() - tStart) / 1000);
      const eta  = Math.round((needsDetail.length - detailFetched) / Math.max(rate, 0.1));
      console.log(`  ${detailFetched}/${needsDetail.length} (${rate.toFixed(1)}/s, ETA ${eta}s)`);
      if (detailFetched % 40 === 0) saveCache();
    }
  }
  saveCache();

  // Phase 3: convert to properties
  console.log(`\n  Конвертация объектов...`);
  const properties = [];
  let idCounter = 900000; // start high to avoid collision with Cian IDs during merge

  for (const card of allCards) {
    const detail = detailMap.get(String(card.productId)) || null;
    const prop = toLotProperty(card, detail, idCounter++);
    if (prop) properties.push(prop);
  }

  console.log(`  Отфильтровано: ${allCards.length - properties.length} (нет города/площади/цены)`);
  console.log(`  Итого аукционных объектов: ${properties.length}`);

  // Phase 4: merge into existing properties.json
  const dry = process.argv.includes('--dry');
  if (dry) {
    console.log('\n  --dry: сохранение пропущено');
    return;
  }

  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

  let existing = { properties: [], updatedAt: new Date().toISOString(), totalCount: 0, cities: [] };
  if (fs.existsSync(OUT_FILE)) {
    try { existing = JSON.parse(fs.readFileSync(OUT_FILE, 'utf8')); }
    catch { /* ignore */ }
  }

  // Remove old lot-online entries, keep Cian
  const cianProps = (existing.properties || []).filter(p => p.source !== SOURCE);

  // Merge and re-sort
  const merged = [...cianProps, ...properties];
  merged.sort((a, b) => b.score - a.score);
  merged.forEach((p, i) => { p.id = i + 1; });

  const output = {
    updatedAt:  new Date().toISOString(),
    totalCount: merged.length,
    cities:     ['Москва', 'Санкт-Петербург', 'Краснодар', 'Сочи', 'Казань', 'Новосибирск', 'Екатеринбург'],
    properties: merged,
  };

  const { safeWriteProperties } = require('./lib-safe-write');
  // For the lot scraper, allow writes that are slightly smaller (just lot churn);
  // base size is preserved because cianProps is included in `merged`.
  safeWriteProperties(OUT_FILE, output, { minRatio: 0.85 });

  console.log(`\n✅ Готово!`);
  console.log(`   Аукционных объектов: ${properties.length}`);
  console.log(`   Cian объектов: ${cianProps.length}`);
  console.log(`   Итого: ${merged.length}`);
  console.log(`   Score 60+: ${merged.filter(p => p.score >= 60).length}`);
  console.log(`   Score 80+: ${merged.filter(p => p.score >= 80).length}\n`);
  console.log(`   Города аукционов: ${[...new Set(properties.map(p => p.city))].filter(Boolean).join(', ')}\n`);
}

scrapeLotOnline().catch(err => { console.error('Fatal:', err); process.exit(1); });
