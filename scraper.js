// AIvest — Cian.ru Scraper
// Usage: node scraper.js          → run once
//        node scraper.js --watch  → run + auto-refresh every 6h
//
// Strategy for 20k+ Moscow listings:
//   Cian returns max 28 pages × 28 = ~784 results per query.
//   We bypass this by splitting queries by room count (0–4+) AND property type.
//   Moscow alone: 5 room counts × 3 types × 784 = up to ~11 700 per city.
//   Combined with price range splits for hottest segments → 20 000+.

require('dotenv').config();
const axios = require('axios');
const fs    = require('fs');
const path  = require('path');

// ── Config ───────────────────────────────────────────────────────────────────
const DATA_DIR   = path.join(__dirname, 'data');
const OUT_FILE   = path.join(DATA_DIR, 'properties.json');
const MAX_PAGES  = 28;
const DELAY_MS   = parseInt(process.env.SCRAPER_DELAY || '800');

const ALL_CITIES = [
  { name: 'Москва',          regionId: 1,    cityId: 1,    rentPpm: 850,  growth: 9.8  },
  { name: 'Санкт-Петербург', regionId: 2,    cityId: 2,    rentPpm: 680,  growth: 8.5  },
  { name: 'Краснодар',       regionId: 4820, cityId: 4820, rentPpm: 430,  growth: 11.0 },
  { name: 'Сочи',            regionId: 4584, cityId: 4998, rentPpm: 920,  growth: 13.1 },
  { name: 'Казань',          regionId: 4618, cityId: 4777, rentPpm: 450,  growth: 9.1  },
  { name: 'Новосибирск',     regionId: 4897, cityId: 4897, rentPpm: 370,  growth: 8.8  },
  { name: 'Екатеринбург',    regionId: 4743, cityId: 4743, rentPpm: 400,  growth: 9.4  },
];

// --city=Москва фильтрует только один город, --merge сохраняет остальные из старого файла
const cityArg = (process.argv.find(a => a.startsWith('--city=')) || '').replace('--city=', '');
const mergeMode = process.argv.includes('--merge');
const CITIES = cityArg ? ALL_CITIES.filter(c => c.name === cityArg) : ALL_CITIES;

// For Moscow: split by rooms to get many more results
// null = no room filter (for houses/commercial/land)
const MOSCOW_ROOM_SPLITS = [null, 0, 1, 2, 3, 4]; // null=all, 0=studio, 1–4=rooms

// Query types: each yields a separate set of up to 784 results
const QUERY_TYPES = [
  { _type: 'flatsale',      label: 'квартиры'    },
  { _type: 'newbuildingflatsale', label: 'новостройки' },
];
// Additional types (no room split needed)
const QUERY_TYPES_NOROOMSPLIT = [
  { _type: 'housesale',     label: 'дома'        },
  { _type: 'commercialsale', label: 'коммерция'  },
];

const CATEGORY_TYPE = {
  flatSale:            'apartment',
  newBuildingFlatSale: 'newbuild',
  roomSale:            'room',
  commercialSale:      'commercial',
  officeSale:          'commercial',
  houseSale:           'house',
  cottageSale:         'house',
  townhouseSale:       'house',
  landSale:            'land',
};

const SOURCE_TAG = 'agg';

// ── Helpers ───────────────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function extractCity(geoAddress = [], cityId = null) {
  const locs = geoAddress.filter(a => a.type === 'location');
  if (cityId) {
    const match = locs.find(l => String(l.id) === String(cityId));
    if (match) return match.name;
  }
  return locs[locs.length - 1]?.name || locs[0]?.shortName || '';
}
function extractDistrict(geoAddress = []) {
  return geoAddress.find(a => a.type === 'raion')?.shortName
      || geoAddress.find(a => a.type === 'okrug')?.shortName
      || '';
}
function extractMetro(geoAddress = []) {
  return geoAddress.find(a => a.type === 'metro')?.shortName || '';
}

// ── AI Scoring ────────────────────────────────────────────────────────────────
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

function marketPpm(cityName) {
  return { 'Москва': 240000, 'Санкт-Петербург': 185000, 'Краснодар': 105000,
           'Сочи': 280000, 'Казань': 125000, 'Новосибирск': 130000, 'Екатеринбург': 115000 }[cityName] || 130000;
}
function estimateLiquidity(cityName, type, metro) {
  let base = { 'Москва': 8.5, 'Санкт-Петербург': 8, 'Сочи': 7.5, 'Казань': 7,
               'Краснодар': 7, 'Новосибирск': 7, 'Екатеринбург': 7 }[cityName] || 6.5;
  if (metro) base = Math.min(10, base + 0.5);
  if (type === 'land') base -= 2;
  if (type === 'commercial') base -= 0.5;
  return Math.round(Math.max(1, Math.min(10, base)));
}
function estimateVacancy(type, cityName) {
  if (type === 'land') return 0;
  if (cityName === 'Сочи') return 10;
  if (type === 'commercial') return 6;
  return 4;
}

// ── Parse one offer ───────────────────────────────────────────────────────────
function parseOffer(raw, cityConfig) {
  const geo   = raw.geo?.address || [];
  const locs  = geo.filter(a => a.type === 'location');
  const inCity = locs.some(l => String(l.id) === String(cityConfig.cityId));
  if (!inCity) return null;

  const cityName = extractCity(geo, cityConfig.cityId) || cityConfig.name;
  const district = extractDistrict(geo);
  const metro    = extractMetro(geo);
  const area     = parseFloat(raw.totalArea) || 0;
  const price    = (raw.bargainTerms?.price || 0) / 1e6;
  const ppm      = area > 0 ? Math.round((raw.bargainTerms?.price || 0) / area) : 0;
  const mktPpm   = marketPpm(cityName);

  const type  = CATEGORY_TYPE[raw.category] || 'apartment';

  // Skip corrupt/placeholder prices (< 25% of market = clearly bad data)
  if (ppm > 0 && ppm < Math.round(mktPpm * 0.25)) return null;
  // Skip non-residential objects miscategorized as apartments (area > 1000m² or price > 500 млн)
  if (type === 'apartment' && (area > 1000 || price > 500)) return null;

  // Detect rooms masquerading as apartments:
  // 1) ppm < 52% of market (Cian shows full apartment area for rooms)
  // 2) raw.totalArea contains "/" (e.g. "55/19" — total apt / room area)
  // 3) raw.roomArea exists and is much smaller than totalArea (roomArea < 50% of total)
  const hasAreaSlash = typeof raw.totalArea === 'string' && raw.totalArea.includes('/');
  const hasRoomArea  = raw.roomArea > 0 && area > 0 && raw.roomArea < area * 0.5;
  const isRoom = type === 'apartment' && ppm > 0 &&
    (ppm < Math.round(mktPpm * 0.55) || hasAreaSlash || hasRoomArea);
  const finalType = isRoom ? 'room' : type;

  const disc  = mktPpm > 0 ? Math.round(((mktPpm - ppm) / mktPpm) * 100 * 10) / 10 : 0;
  const monthlyRent = Math.round(cityConfig.rentPpm * area);
  const vac   = estimateVacancy(finalType, cityName);
  const roi   = price > 0 ? Math.round((monthlyRent * 12 * (1 - vac / 100) / (price * 1e6)) * 100 * 10) / 10 : 0;
  const liq   = estimateLiquidity(cityName, type, metro);
  const score = calcScore({ disc, roi, grow: cityConfig.growth, liq, vac });

  const roomsLabel = { 1: '1-комн.', 2: '2-комн.', 3: '3-комн.', 4: '4-комн.' }[raw.roomsCount] || 'Студия';
  const typeLabel  = type === 'newbuild' ? 'Новостройка' : type === 'house' ? 'Дом'
                   : type === 'commercial' ? 'Коммерция' : type === 'land' ? 'Участок' : null;
  const titleBase  = typeLabel || (raw.roomsCount ? roomsLabel + ' кв.' : 'Квартира');
  const titleLoc   = district || metro || cityName;
  const floor      = raw.floorNumber && raw.building?.floorsCount
    ? `${raw.floorNumber}/${raw.building.floorsCount}` : raw.floorNumber ? `${raw.floorNumber}/?` : '—';

  let badge = '';
  if (disc >= 20) badge = 'Ниже рынка';
  else if (type === 'newbuild') badge = 'Новостройка';
  else if (metro && (cityName === 'Москва' || cityName === 'Санкт-Петербург')) badge = 'Метро рядом';
  else if (cityName === 'Сочи') badge = 'Туризм';

  return {
    id:      raw.id,
    cianId:  raw.id,
    cianUrl: raw.fullUrl || `https://cian.ru/sale/flat/${raw.id}/`,
    title:   `${titleBase}, ${titleLoc}`,
    city:    cityName, district, metro,
    area:    Math.round(area), floor, type, source: SOURCE_TAG,
    price:   Math.round(price * 10) / 10,
    ppm:     Math.round(ppm / 1000),
    mkt:     Math.round(mktPpm / 1000),
    rent:    Math.round(monthlyRent / 1000),
    vac, grow: cityConfig.growth, liq, badge, score, disc, roi,
    photos:  raw.photos?.map(p => p.fullUrl || p.thumbnail2Url).filter(Boolean).slice(0, 5) || [],
    description: raw.description?.slice(0, 200) || '',
    addedAt:   raw.addedTimestamp ? new Date(raw.addedTimestamp * 1000).toISOString() : new Date().toISOString(),
    scrapedAt: new Date().toISOString(),
  };
}

// ── Fetch one page ────────────────────────────────────────────────────────────
// Rotate user agents to reduce blocking
const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
];
let uaIndex = 0;
function nextUA() { return USER_AGENTS[(uaIndex++) % USER_AGENTS.length]; }

async function fetchPage(regionId, queryType, page, roomsCount = null) {
  const jsonQuery = {
    _type:          queryType,
    engine_version: { type: 'term', value: 2 },
    region:         { type: 'terms', value: [regionId] },
    page:           { type: 'term', value: page },
  };
  if (roomsCount !== null) {
    jsonQuery.room = { type: 'terms', value: [roomsCount] };
  }

  const res = await axios.post(
    'https://api.cian.ru/search-offers/v2/search-offers-desktop/',
    { jsonQuery },
    {
      headers: {
        'Content-Type':  'application/json',
        'User-Agent':    nextUA(),
        'Referer':       'https://cian.ru/',
        'Accept':        'application/json, text/plain, */*',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8',
        'Origin':        'https://cian.ru',
        'sec-ch-ua':     '"Chromium";v="124", "Google Chrome";v="124"',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
      },
      timeout: 20000,
      maxRedirects: 5,
    }
  );

  if (res.status !== 200) throw new Error(`HTTP ${res.status}`);
  const offers = res.data?.data?.offersSerialized;
  if (!offers) throw new Error(`No offersSerialized. Keys: ${Object.keys(res.data || {}).join(',')}`);
  return offers;
}

// ── Scrape one query segment ──────────────────────────────────────────────────
async function scrapeSegment(city, queryType, label, roomsCount, allProps, seen) {
  const roomLabel = roomsCount === null ? '' : roomsCount === 0 ? ' [студия]' : ` [${roomsCount}к]`;
  process.stdout.write(`  ${label}${roomLabel}: `);
  let added = 0;

  for (let page = 1; page <= MAX_PAGES; page++) {
    try {
      const offers = await fetchPage(city.regionId, queryType, page, roomsCount);
      if (!offers.length) { process.stdout.write(`стр.${page} пусто\n`); break; }

      let pageAdded = 0;
      for (const o of offers) {
        if (!o.totalArea || !o.bargainTerms?.price) continue;
        if (seen.has(o.id)) continue;
        const parsed = parseOffer(o, city);
        if (!parsed) continue;
        seen.add(o.id);
        allProps.push(parsed);
        pageAdded++;
        added++;
      }
      process.stdout.write(`${page}(+${pageAdded}) `);
      await sleep(DELAY_MS);
    } catch (err) {
      const errMsg = err.response
        ? `HTTP ${err.response.status}`
        : err.message?.slice(0, 60);
      process.stdout.write(`❌(${errMsg}) `);
      // If blocked (403/429/503) — stop this segment early
      if (err.response?.status === 403 || err.response?.status === 503) {
        process.stdout.write(`\n  ⛔ Blocked, skipping segment\n`);
        break;
      }
      await sleep(3000);
    }
  }
  process.stdout.write(`→ итого +${added}\n`);
  return added;
}

// ── Main scrape ───────────────────────────────────────────────────────────────
async function scrape() {
  console.log(`\n🔍 AIvest Scraper — ${new Date().toLocaleString('ru-RU')}`);
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

  const allProps = [];
  const seen     = new Set();

  for (const city of CITIES) {
    console.log(`\n📍 ${city.name}`);
    const isMoscow = city.name === 'Москва';

    if (isMoscow) {
      // Moscow: split by room count × query type for maximum coverage
      for (const qt of QUERY_TYPES) {
        for (const rooms of MOSCOW_ROOM_SPLITS) {
          await scrapeSegment(city, qt._type, qt.label, rooms, allProps, seen);
        }
      }
      // Houses, commercial etc. without room split
      for (const qt of QUERY_TYPES_NOROOMSPLIT) {
        await scrapeSegment(city, qt._type, qt.label, null, allProps, seen);
      }
    } else {
      // Other cities: flat sweep, no room split
      for (const qt of [...QUERY_TYPES, ...QUERY_TYPES_NOROOMSPLIT]) {
        await scrapeSegment(city, qt._type, qt.label, null, allProps, seen);
      }
    }
  }

  // Deduplicate (safety), sort, re-index
  const deduped = [...new Map(allProps.map(p => [p.cianId, p])).values()];
  deduped.sort((a, b) => b.score - a.score);
  deduped.forEach((p, i) => { p.id = i + 1; });

  if (deduped.length === 0) {
    console.log('\n⚠️  0 properties scraped — keeping existing data unchanged');
    return null;
  }

  // --merge: keep other cities from existing file, replace only scraped cities
  let finalProps = deduped;
  if (mergeMode && cityArg && fs.existsSync(OUT_FILE)) {
    const old = JSON.parse(fs.readFileSync(OUT_FILE, 'utf8'));
    const oldOthers = (old.properties || []).filter(p => p.city !== cityArg);
    finalProps = [...deduped, ...oldOthers];
    finalProps.sort((a, b) => b.score - a.score);
    finalProps.forEach((p, i) => { p.id = i + 1; });
    console.log(`   Merge: ${deduped.length} новых (${cityArg}) + ${oldOthers.length} старых других городов = ${finalProps.length} итого`);
  }

  const output = {
    updatedAt:  new Date().toISOString(),
    totalCount: finalProps.length,
    cities:     ALL_CITIES.map(c => c.name),
    properties: finalProps,
  };

  fs.writeFileSync(OUT_FILE, JSON.stringify(output));

  console.log(`\n✅ Готово! ${deduped.length} уникальных объектов → data/properties.json`);
  console.log(`   Москва:  ${deduped.filter(p => p.city === 'Москва').length}`);
  console.log(`   Score 60+: ${deduped.filter(p => p.score >= 60).length}`);
  console.log(`   Score 80+: ${deduped.filter(p => p.score >= 80).length}\n`);

  return output;
}

// ── Watch mode ────────────────────────────────────────────────────────────────
async function main() {
  await scrape();
  if (process.argv.includes('--watch')) {
    const cron = require('node-cron');
    console.log('⏰ Watch mode: перезапуск каждые 6 часов...');
    cron.schedule('0 */6 * * *', () => scrape());
  }
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
