// AIvest — Yandex Realty (realty.yandex.ru) Scraper
// Парсит объявления из window.INITIAL_STATE на HTML-страницах поиска.
// Usage:
//   node scraper-yandex.js              → merge yand-items into properties.json
//   node scraper-yandex.js --dry        → print stats without saving
//   node scraper-yandex.js --city=Москва [--max-pages=10]

require('dotenv').config();
// Force unbuffered stdout so progress isn't hidden when piped
if (process.stdout._handle && process.stdout._handle.setBlocking) process.stdout._handle.setBlocking(true);

const fs    = require('fs');
const path  = require('path');
const https = require('https');
const { URL } = require('url');

const DATA_DIR  = path.join(__dirname, 'data');
const OUT_FILE  = path.join(DATA_DIR, 'properties.json');
const DELAY_MS  = parseInt(process.env.YAND_DELAY   || '1800');
const TIMEOUT   = parseInt(process.env.YAND_TIMEOUT || '20000');
const MAX_PAGES = parseInt((process.argv.find(a => a.startsWith('--max-pages=')) || '').split('=')[1] || '8');
const DRY       = process.argv.includes('--dry');
const CITY_ARG  = (process.argv.find(a => a.startsWith('--city=')) || '').split('=')[1];
const SOURCE    = 'yand';

// ── Cities (slugs as used by realty.yandex.ru URL path) ──────────────────────
const CITIES = [
  { name: 'Москва',          slug: 'moskva',          rentPpm: 850,  growth: 9.8,  marketPpm: 240000 },
  { name: 'Санкт-Петербург', slug: 'sankt-peterburg', rentPpm: 680,  growth: 8.5,  marketPpm: 185000 },
  { name: 'Краснодар',       slug: 'krasnodar',       rentPpm: 430,  growth: 11.0, marketPpm: 105000 },
  { name: 'Сочи',            slug: 'sochi',           rentPpm: 920,  growth: 13.1, marketPpm: 280000 },
  { name: 'Казань',          slug: 'kazan',           rentPpm: 450,  growth: 9.1,  marketPpm: 125000 },
  { name: 'Новосибирск',     slug: 'novosibirsk',     rentPpm: 370,  growth: 8.8,  marketPpm: 130000 },
  { name: 'Екатеринбург',    slug: 'ekaterinburg',    rentPpm: 400,  growth: 9.4,  marketPpm: 115000 },
];

// Category slug → AIvest type
const CATEGORIES = [
  { slug: 'kvartira',                       type: 'apartment',  label: 'квартиры' },
  { slug: 'komnata',                        type: 'room',       label: 'комнаты' },
  { slug: 'kommercheskaya-nedvizhimost',    type: 'commercial', label: 'коммерция' },
  { slug: 'dom',                            type: 'house',      label: 'дома' },
  { slug: 'garazh',                         type: 'parking',    label: 'гаражи/машиноместа' },
];

// Yandex offerCategory → AIvest type (for validation / sanity)
const CATEGORY_MAP = {
  APARTMENT: 'apartment',
  ROOMS:     'room',
  HOUSE:     'house',
  COMMERCIAL:'commercial',
  GARAGE:    'parking',
  LOT:       'land',
};

// ── Low-level fetch ──────────────────────────────────────────────────────────
const UAS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
];
function pickUA() { return UAS[Math.floor(Math.random() * UAS.length)]; }

function rawGet(urlStr, timeoutMs = TIMEOUT, attempt = 0) {
  return new Promise((resolve, reject) => {
    let done = false;
    const u = new URL(urlStr);
    const req = https.request({
      method: 'GET', hostname: u.hostname, port: 443,
      path: u.pathname + u.search,
      headers: {
        'User-Agent': pickUA(),
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'ru-RU,ru;q=0.9',
        'Connection': 'close',
      },
      agent: new https.Agent({ keepAlive: false }),
    }, res => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        if (done) return; done = true; clearTimeout(killer);
        const next = new URL(res.headers.location, urlStr).toString();
        return rawGet(next, timeoutMs, attempt).then(resolve, reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        if (done) return; done = true; clearTimeout(killer);
        resolve({ status: res.statusCode, data: Buffer.concat(chunks).toString('utf8') });
      });
      res.on('error', e => { if (!done) { done = true; clearTimeout(killer); reject(e); } });
    });
    const killer = setTimeout(() => {
      if (done) return; done = true;
      try { req.destroy(new Error('timeout')); } catch {}
      reject(new Error('timeout'));
    }, timeoutMs);
    req.on('error', e => { if (!done) { done = true; clearTimeout(killer); reject(e); } });
    req.end();
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Extract offers from one HTML page ────────────────────────────────────────
function parsePage(html) {
  // Yandex embeds the full React state in window.INITIAL_STATE = {...};
  const m = html.match(/window\.INITIAL_STATE\s*=\s*(\{[\s\S]*?\});?\s*<\/script>/);
  if (!m) return { offers: [], totalPages: 0, blocked: /captcha|showcaptcha|smartcaptcha/i.test(html) };
  let state;
  try { state = JSON.parse(m[1]); }
  catch { return { offers: [], totalPages: 0, blocked: false }; }
  const entities = state?.search?.offers?.entities || [];
  const pager    = state?.search?.offers?.pager || state?.search?.offers?.pagination || {};
  const totalPages = pager.totalPages || 0;
  return { offers: entities, totalPages, blocked: false };
}

// ── Convert Yandex offer → AIvest property ───────────────────────────────────
function extractDistrict(loc) {
  const comps = loc?.structuredAddress?.component || [];
  // Prefer city-district / okrug / sub-locality
  for (const c of comps) {
    if (c.regionType === 'CITY_DISTRICT' || c.regionType === 'SUB_LOCALITY') return c.value;
  }
  for (const c of comps) {
    if (c.regionType === 'CITY_DISTRICT_SECONDARY') return c.value;
  }
  return '';
}
function extractMetro(loc) {
  const stations = loc?.metro?.stations || loc?.metro ? [loc.metro] : [];
  const first = Array.isArray(stations) ? stations[0] : stations;
  return first?.name || loc?.metroList?.[0]?.name || '';
}

// ── AI scoring (same formula as Cian scraper) ───────────────────────────────
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
function estimateLiquidity(city, type, metro) {
  let base = { 'Москва': 8.5, 'Санкт-Петербург': 8, 'Сочи': 7.5, 'Казань': 7,
               'Краснодар': 7, 'Новосибирск': 7, 'Екатеринбург': 7 }[city] || 6.5;
  if (metro && (city === 'Москва' || city === 'Санкт-Петербург')) base += 0.5;
  if (type === 'commercial') base -= 0.5;
  if (type === 'newbuild')   base -= 0.2;
  if (type === 'room')       base -= 1.0;
  if (type === 'parking')    base -= 1.5;
  return Math.max(1, Math.min(10, Math.round(base * 10) / 10));
}
function estimateVacancy(type, city) {
  const base = { 'Москва': 4.5, 'Санкт-Петербург': 5, 'Сочи': 6, 'Казань': 5.5,
                 'Краснодар': 6, 'Новосибирск': 5.5, 'Екатеринбург': 5.5 }[city] || 7;
  if (type === 'commercial') return Math.round((base + 4) * 10) / 10;
  if (type === 'parking')    return Math.round((base + 3) * 10) / 10;
  if (type === 'newbuild')   return Math.round((base + 1.5) * 10) / 10;
  return base;
}

// ── Convert one Yandex offer → AIvest property shape ─────────────────────────
function toProperty(offer, cityCfg, categoryType) {
  const area  = parseFloat(offer?.area?.value) || 0;
  const price = parseFloat(offer?.price?.value) || 0;
  if (!area || !price) return null;

  // Normalize type — prefer Cian-like mapping: flat-new → newbuild, etc.
  let type = CATEGORY_MAP[offer.offerCategory] || categoryType || 'apartment';
  if (type === 'apartment' && (offer.flatType === 'NEW_FLAT' || offer.newBuilding === true ||
      offer?.building?.siteName || offer?.building?.buildingState === 'UNFINISHED')) {
    type = 'newbuild';
  }

  const priceMln = price / 1e6;
  const ppm      = Math.round(price / area);
  const mktPpm   = cityCfg.marketPpm;

  // ── Sanity / validation ───────────────────────────────────────────────────
  if (priceMln < 0.3)                             return null; // deposit/fraction placeholder
  if (type === 'apartment' && area >= 500)        return null;
  if (type === 'apartment' && priceMln > 500)     return null;
  if (type === 'room'      && area >= 200)        return null;
  if (type === 'parking'   && area >= 80)         return null;
  if (ppm < Math.round(mktPpm * 0.2))             return null; // corrupt per-m²
  if (ppm > Math.round(mktPpm * 6.0))             return null; // extreme outlier

  const loc = offer.location || {};
  const city = cityCfg.name;
  const district = extractDistrict(loc);
  const metro    = extractMetro(loc);
  const floors   = offer.floorsOffered || [];
  const floorStr = floors.length && offer.floorsTotal
    ? `${floors[0]}/${offer.floorsTotal}` : floors.length ? `${floors[0]}/?` : '—';

  const roomsLabel = { 1: '1-комн.', 2: '2-комн.', 3: '3-комн.', 4: '4-комн.' }[offer.roomsTotal] || 'Студия';
  const typeLabel  = type === 'newbuild' ? 'Новостройка' : type === 'house' ? 'Дом'
                   : type === 'commercial' ? 'Коммерция' : type === 'land' ? 'Участок'
                   : type === 'room' ? 'Комната' : type === 'parking' ? 'Машиноместо' : null;
  const titleBase  = typeLabel || (offer.roomsTotal ? roomsLabel + ' кв.' : 'Квартира');
  const titleLoc   = district || metro || city;

  const disc = mktPpm > 0 ? Math.round(((mktPpm - ppm) / mktPpm) * 100 * 10) / 10 : 0;
  const rent = Math.round(cityCfg.rentPpm * area);
  const vac  = estimateVacancy(type, city);
  const roi  = price > 0 ? Math.round((rent * 12 * (1 - vac / 100) / price) * 100 * 10) / 10 : 0;
  const liq  = estimateLiquidity(city, type, metro);
  const score= calcScore({ disc, roi, grow: cityCfg.growth, liq, vac });

  let badge = '';
  if (disc >= 20) badge = 'Ниже рынка';
  else if (type === 'newbuild') badge = 'Новостройка';
  else if (metro && (city === 'Москва' || city === 'Санкт-Петербург')) badge = 'Метро рядом';
  else if (city === 'Сочи') badge = 'Туризм';

  const url = offer.url || (offer.unsignedInternalUrl ? 'https:' + offer.unsignedInternalUrl : '');

  return {
    id:      0, // filled in merge step
    cianId:  `yand-${offer.offerId}`,
    cianUrl: url,
    title:   `${titleBase}, ${titleLoc}`,
    city, district, metro,
    area:    Math.round(area), floor: floorStr, type, source: SOURCE,
    price:   Math.round(priceMln * 10) / 10,
    ppm:     Math.round(ppm / 1000),
    mkt:     Math.round(mktPpm / 1000),
    rent:    Math.round(rent / 1000),
    vac, grow: cityCfg.growth, liq, badge, score, disc, roi,
    photos:  (offer.appMiddleImages || offer.fullImages || offer.mainImages || [])
               .map(p => typeof p === 'string' ? p : (p.url || p.origin || ''))
               .filter(Boolean).slice(0, 5),
    description: (offer.description || '').slice(0, 200),
    addedAt:   offer.creationDate || new Date().toISOString(),
    scrapedAt: new Date().toISOString(),
  };
}

// ── Validation counters ──────────────────────────────────────────────────────
const stats = { fetched: 0, parsed: 0, saneDropped: 0, typeMismatch: 0, dupInSource: 0, blocked: 0 };

// ── Main ─────────────────────────────────────────────────────────────────────
async function scrapeCity(cityCfg, categories) {
  const seen = new Set();
  const out = [];
  for (const cat of categories) {
    process.stdout.write(`  ${cat.label}: `);
    for (let page = 1; page <= MAX_PAGES; page++) {
      const url = `https://realty.yandex.ru/${cityCfg.slug}/kupit/${cat.slug}/?page=${page}`;
      let res;
      try {
        res = await rawGet(url, TIMEOUT);
      } catch (e) {
        process.stdout.write(`p${page}(ERR ${e.message?.slice(0,15)}) `);
        break;
      }
      if (res.status === 404) { process.stdout.write('404 '); break; }
      if (res.status !== 200) { process.stdout.write(`${res.status} `); break; }
      stats.fetched++;
      const { offers, totalPages, blocked } = parsePage(res.data);
      if (blocked) {
        stats.blocked++;
        process.stdout.write(`p${page}(captcha!) `);
        break;
      }
      let pageAdded = 0;
      for (const o of offers) {
        if (!o.offerId || seen.has(o.offerId)) { stats.dupInSource++; continue; }
        seen.add(o.offerId);
        const prop = toProperty(o, cityCfg, cat.type);
        if (!prop) { stats.saneDropped++; continue; }
        // Type-mismatch validation: if Yandex says APARTMENT but we're scraping commercial, log.
        const yandType = CATEGORY_MAP[o.offerCategory];
        if (yandType && yandType !== cat.type && !(cat.type === 'apartment' && yandType === 'apartment')) {
          stats.typeMismatch++;
        }
        out.push(prop);
        stats.parsed++;
        pageAdded++;
      }
      process.stdout.write(`p${page}(+${pageAdded}) `);
      if (offers.length === 0) break;
      if (totalPages && page >= totalPages) break;
      await sleep(DELAY_MS);
    }
    process.stdout.write('\n');
  }
  return out;
}

// ── Cross-source dedup: match Yandex offers against existing Cian items ─────
function keyFor(p) { return `${p.city}|${p.type}|${p.district || ''}`; }
function isNearDup(a, b) {
  // Same city + district + type required
  if (a.city !== b.city || a.type !== b.type) return false;
  const adist = (a.district || '').toLowerCase();
  const bdist = (b.district || '').toLowerCase();
  if (adist && bdist && adist !== bdist) return false;
  // Area within 2 m² AND price within 5%
  if (!a.area || !b.area) return false;
  if (Math.abs(a.area - b.area) > 2) return false;
  if (!a.price || !b.price) return false;
  const rel = Math.abs(a.price - b.price) / Math.max(a.price, b.price);
  if (rel > 0.05) return false;
  return true;
}
function mergeCrossSource(existing, incoming) {
  // Bucket existing by city|type|district for O(N+M) instead of O(N*M)
  const buckets = new Map();
  for (const p of existing) {
    const k = keyFor(p);
    if (!buckets.has(k)) buckets.set(k, []);
    buckets.get(k).push(p);
  }
  const merged = [...existing];
  let crossDup = 0, added = 0;
  for (const y of incoming) {
    const bucket = buckets.get(keyFor(y)) || [];
    const hit = bucket.find(x => isNearDup(x, y));
    if (hit) {
      // Existing takes priority; just record alt-source link
      if (!hit.altSources) hit.altSources = [];
      if (!hit.altSources.find(s => s.source === y.source && s.cianId === y.cianId)) {
        hit.altSources.push({ source: y.source, cianId: y.cianId, url: y.cianUrl });
      }
      crossDup++;
    } else {
      merged.push(y);
      const k = keyFor(y);
      if (!buckets.has(k)) buckets.set(k, []);
      buckets.get(k).push(y);
      added++;
    }
  }
  return { merged, crossDup, added };
}

async function main() {
  console.log(`\n🅈 AIvest Yandex Scraper — ${new Date().toLocaleString('ru-RU')}\n`);

  const citiesToRun = CITY_ARG ? CITIES.filter(c => c.name === CITY_ARG) : CITIES;
  if (!citiesToRun.length) { console.error(`City not found: ${CITY_ARG}`); process.exit(1); }

  const allYand = [];
  for (const cityCfg of citiesToRun) {
    console.log(`📍 ${cityCfg.name}`);
    const props = await scrapeCity(cityCfg, CATEGORIES);
    console.log(`  → ${props.length} offers\n`);
    allYand.push(...props);
    if (stats.blocked > 3) { console.log('  Too many captchas — stopping'); break; }
  }

  console.log(`\n── Статистика Yandex ──`);
  console.log(`  HTTP-страниц:    ${stats.fetched}`);
  console.log(`  Объектов:        ${stats.parsed}`);
  console.log(`  Отфильтровано:   ${stats.saneDropped} (sanity)`);
  console.log(`  Type mismatch:   ${stats.typeMismatch}`);
  console.log(`  Дублей в Yandex: ${stats.dupInSource}`);
  if (stats.blocked) console.log(`  Captcha блоков:  ${stats.blocked}`);

  if (DRY) { console.log('\n--dry: сохранение пропущено'); return; }
  if (allYand.length === 0) { console.log('\nНечего сохранять.'); return; }

  // ── Merge into properties.json ────────────────────────────────────────────
  let existing = { properties: [] };
  if (fs.existsSync(OUT_FILE)) {
    try { existing = JSON.parse(fs.readFileSync(OUT_FILE, 'utf8')); } catch {}
  }
  // Drop any previous yand-* items (we re-scraped fresh)
  const preserved = (existing.properties || []).filter(p => p.source !== SOURCE);
  const { merged, crossDup, added } = mergeCrossSource(preserved, allYand);

  // Re-assign ids and sort by score
  merged.sort((a, b) => (b.score || 0) - (a.score || 0));
  merged.forEach((p, i) => { p.id = i + 1; });

  const payload = {
    source: existing.source || 'cian+yand',
    sources: ['cian', 'yand'],
    updatedAt: new Date().toISOString(),
    total: merged.length,
    properties: merged,
  };
  fs.writeFileSync(OUT_FILE, JSON.stringify(payload));

  const byType = {};
  merged.forEach(p => { byType[p.type] = (byType[p.type] || 0) + 1; });
  const bySource = {};
  merged.forEach(p => { bySource[p.source] = (bySource[p.source] || 0) + 1; });

  console.log(`\n── Merge ──`);
  console.log(`  Cross-source дублей:  ${crossDup} (привязаны к существующим через altSources)`);
  console.log(`  Новых от Yandex:      ${added}`);
  console.log(`  Итого в базе:         ${merged.length}`);
  console.log(`  По источникам:        ${JSON.stringify(bySource)}`);
  console.log(`  По типу:              ${JSON.stringify(byType)}`);
  console.log(`\n✅ Сохранено в ${OUT_FILE}`);
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
