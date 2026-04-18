// AIvest — Cian.ru Rent Scraper
// Pulls long-term rental listings (flatrent) and scores them for RENTERS
// (best deals, not investor ROI). Writes to data/properties-rent.json.
//
// Usage:
//   node scraper-rent.js           → run full sweep
//   node scraper-rent.js --dry     → fetch only, no write

require('dotenv').config();
const axios = require('axios');
const fs    = require('fs');
const path  = require('path');
const { safeWriteProperties } = require('./lib-safe-write');

const DATA_DIR  = path.join(__dirname, 'data');
const OUT_FILE  = path.join(DATA_DIR, 'properties-rent.json');
const MAX_PAGES = 20;
const DELAY_MS  = parseInt(process.env.RENT_DELAY || '900');

const ALL_CITIES = [
  { name: 'Москва',          regionId: 1,    cityId: 1,    rentPpm: 850,  growth: 9.8  },
  { name: 'Санкт-Петербург', regionId: 2,    cityId: 2,    rentPpm: 680,  growth: 8.5  },
  { name: 'Краснодар',       regionId: 4820, cityId: 4820, rentPpm: 430,  growth: 11.0 },
  { name: 'Сочи',            regionId: 4584, cityId: 4998, rentPpm: 920,  growth: 13.1 },
  { name: 'Казань',          regionId: 4618, cityId: 4777, rentPpm: 450,  growth: 9.1  },
  { name: 'Новосибирск',     regionId: 4897, cityId: 4897, rentPpm: 370,  growth: 8.8  },
  { name: 'Екатеринбург',    regionId: 4743, cityId: 4743, rentPpm: 400,  growth: 9.4  },
];

const ROOM_SPLITS = [null, 0, 1, 2, 3]; // null=all, 0=studio, 1–3 rooms
const SOURCE_TAG = 'agg';

// Cian rent categories
const RENT_CATEGORY = {
  flatRent:            'apartment',
  roomRent:            'room',
  dailyFlatRent:       'apartment-daily',
  newBuildingFlatRent: 'apartment',
  houseRent:           'house',
  cottageRent:         'house',
  townhouseRent:       'house',
};

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function extractCity(geo, cityId) {
  const locs = geo.filter(a => a.type === 'location');
  if (cityId) {
    const m = locs.find(l => String(l.id) === String(cityId));
    if (m) return m.name;
  }
  return locs[locs.length - 1]?.name || '';
}
const extractDistrict = (geo) =>
  geo.find(a => a.type === 'raion')?.shortName || geo.find(a => a.type === 'okrug')?.shortName || '';
const extractMetro = (geo) => geo.find(a => a.type === 'metro')?.shortName || '';

// ── RENT scoring: best deal for renter ────────────────────────────────────────
// Factors:
//   • Discount of monthly rent vs. market rent for city (disc, weighted high)
//   • Metro walking distance (if any metro → +)
//   • Not daily (daily mode flagged separately)
//   • Liquidity of city (big cities → more options → quality matters)
function scoreRent({ discRent, hasMetro, liq }) {
  const discScore = discRent > 0 ? 45 * (1 - Math.exp(-discRent / 15)) : Math.max(-10, discRent * 0.5);
  const metroScore = hasMetro ? 18 : 0;
  const liqScore = liq * 2.5;
  const raw = discScore + metroScore + liqScore;
  return Math.min(99, Math.max(0, Math.round(raw + 15)));
}

function parseRentOffer(raw, city) {
  const geo = raw.geo?.address || [];
  if (!geo.some(a => a.type === 'location' && String(a.id) === String(city.cityId))) return null;

  const cityName = extractCity(geo, city.cityId) || city.name;
  const district = extractDistrict(geo);
  const metro    = extractMetro(geo);
  const area     = parseFloat(raw.totalArea) || 0;

  // ── Daily-rent detection (Cian marks by category OR bargainTerms.duration) ──
  const categoryLc = String(raw.category || '').toLowerCase();
  if (categoryLc.includes('daily')) return null;
  const duration = String(raw.bargainTerms?.duration || raw.bargainTerms?.leaseType || '').toLowerCase();
  if (duration.includes('day') || duration === 'short') return null;

  // For rent: bargainTerms.price is ₽/month (monthly rent)
  const monthly = raw.bargainTerms?.price || 0;
  if (!monthly || !area) return null;
  if (monthly < 15_000) return null;                    // no real long-term rental below 15k ₽/mo
  if (monthly > 5_000_000) return null;                 // ignore mansion luxury outliers

  // ── Data-sanity: area vs. type ─────────────────────────────────────────────
  const type = RENT_CATEGORY[raw.category] || 'apartment';
  if (type === 'apartment-daily') return null;
  if (type === 'apartment' && area > 300) return null;  // 600м² «студия» = мусорные данные
  if (type === 'apartment' && area < 8)   return null;  // дроби м²/кладовые — не квартиры
  if (type === 'room'      && area > 60)  return null;
  if (type === 'room'      && area < 7)   return null;

  const ppm = Math.round(monthly / area);               // ₽/m²/month
  const mktPpm = city.rentPpm;                          // ₽/m²/month market
  // Reject listings with ppm < 25% of market — almost always daily/corrupt
  if (mktPpm > 0 && ppm < mktPpm * 0.25) return null;

  const discRent = mktPpm > 0 ? Math.round(((mktPpm - ppm) / mktPpm) * 100 * 10) / 10 : 0;

  const hasMetro = !!metro;
  let liq = { 'Москва': 9, 'Санкт-Петербург': 8.5, 'Сочи': 7, 'Казань': 7,
              'Краснодар': 7, 'Новосибирск': 7, 'Екатеринбург': 7 }[cityName] || 6.5;
  if (metro) liq = Math.min(10, liq + 0.5);
  liq = Math.round(liq);

  const score = scoreRent({ discRent, hasMetro, liq });

  const roomsLabel = { 1: '1-комн.', 2: '2-комн.', 3: '3-комн.', 4: '4-комн.' }[raw.roomsCount] || 'Студия';
  const typeLabel = type === 'room' ? 'Комната' : type === 'house' ? 'Дом' : (raw.roomsCount ? roomsLabel + ' кв.' : 'Квартира');
  const titleLoc = district || metro || cityName;
  const floor = raw.floorNumber && raw.building?.floorsCount
    ? `${raw.floorNumber}/${raw.building.floorsCount}` : raw.floorNumber ? `${raw.floorNumber}/?` : '—';

  let badge = '';
  if (discRent >= 20) badge = 'Ниже рынка';
  else if (metro && (cityName === 'Москва' || cityName === 'Санкт-Петербург')) badge = 'Метро рядом';
  else if (raw.category === 'newBuildingFlatRent') badge = 'Новостройка';

  const lat = raw.coordinates?.lat || raw.geo?.coordinates?.lat || null;
  const lng = raw.coordinates?.lng || raw.geo?.coordinates?.lng || null;

  return {
    id:      raw.id,
    cianId:  raw.id,
    cianUrl: raw.fullUrl || `https://cian.ru/rent/flat/${raw.id}/`,
    title:   `${typeLabel}, ${titleLoc}`,
    city:    cityName, district, metro,
    lat, lng,
    area:    Math.round(area), floor, type, source: SOURCE_TAG,
    mode:    'rent',                  // DISTINGUISHING FIELD
    monthly: Math.round(monthly),     // ₽/month
    ppm:     ppm,                     // ₽/m²/month
    mkt:     mktPpm,                  // market ₽/m²/month
    // For UI compatibility with buy-mode cards
    price:   Math.round(monthly / 1000 * 10) / 10, // ₽·тыс/мес (displayed as "тыс/мес")
    rent:    Math.round(monthly / 1000),
    disc:    discRent,
    liq, score,
    roi:     0, grow: 0, vac: 0,
    badge,
    photos:  raw.photos?.map(p => p.fullUrl || p.thumbnail2Url).filter(Boolean).slice(0, 5) || [],
    description: raw.description?.slice(0, 200) || '',
    addedAt:   raw.addedTimestamp ? new Date(raw.addedTimestamp * 1000).toISOString() : new Date().toISOString(),
    scrapedAt: new Date().toISOString(),
  };
}

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
];
let uaIdx = 0;
const nextUA = () => USER_AGENTS[(uaIdx++) % USER_AGENTS.length];

async function fetchPage(regionId, page, roomsCount) {
  const jsonQuery = {
    _type:          'flatrent',
    engine_version: { type: 'term', value: 2 },
    region:         { type: 'terms', value: [regionId] },
    for_day:        { type: 'term',  value: 'not_daily' }, // долгосрочная аренда
    page:           { type: 'term',  value: page },
  };
  if (roomsCount !== null) jsonQuery.room = { type: 'terms', value: [roomsCount] };

  const res = await axios.post(
    'https://api.cian.ru/search-offers/v2/search-offers-desktop/',
    { jsonQuery },
    {
      headers: {
        'Content-Type':  'application/json',
        'User-Agent':    nextUA(),
        'Referer':       'https://cian.ru/',
        'Accept':        'application/json',
        'Accept-Language': 'ru-RU,ru;q=0.9',
        'Origin':        'https://cian.ru',
      },
      timeout: 20000,
    }
  );
  const offers = res.data?.data?.offersSerialized;
  if (!offers) throw new Error('no offers');
  return offers;
}

async function sweepCity(city, allProps, seen) {
  console.log(`\n📍 ${city.name}`);
  const splits = city.name === 'Москва' ? ROOM_SPLITS : [null];
  for (const rooms of splits) {
    const lbl = rooms === null ? 'аренда' : rooms === 0 ? 'аренда [студия]' : `аренда [${rooms}к]`;
    process.stdout.write(`  ${lbl}: `);
    let added = 0;
    for (let page = 1; page <= MAX_PAGES; page++) {
      try {
        const offers = await fetchPage(city.regionId, page, rooms);
        if (!offers.length) { process.stdout.write(`стр.${page} пусто\n`); break; }
        let pa = 0;
        for (const o of offers) {
          if (seen.has(o.id)) continue;
          const p = parseRentOffer(o, city);
          if (!p) continue;
          seen.add(o.id); allProps.push(p); pa++; added++;
        }
        process.stdout.write(`${page}(+${pa}) `);
        await sleep(DELAY_MS);
      } catch (e) {
        const st = e.response?.status;
        process.stdout.write(`❌(${st||e.message.slice(0,30)}) `);
        if (st === 403 || st === 503) break;
        await sleep(3000);
      }
    }
    process.stdout.write(`→ итого +${added}\n`);
  }
}

async function main() {
  console.log(`\n🏠 AIvest Rent Scraper — ${new Date().toLocaleString('ru-RU')}`);
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);
  const dry = process.argv.includes('--dry');

  const allProps = [];
  const seen = new Set();

  for (const city of ALL_CITIES) {
    await sweepCity(city, allProps, seen);
  }

  const deduped = [...new Map(allProps.map(p => [p.cianId, p])).values()];
  deduped.sort((a, b) => b.score - a.score);
  deduped.forEach((p, i) => { p.id = i + 1; });

  console.log(`\n✅ ${deduped.length} rent listings parsed`);
  console.log(`   Score 60+: ${deduped.filter(p => p.score >= 60).length}`);
  console.log(`   Score 80+: ${deduped.filter(p => p.score >= 80).length}`);

  if (dry) { console.log('--dry: skip write'); return; }
  if (deduped.length === 0) { console.log('⚠ 0 listings — preserving existing rent file'); return; }

  const output = {
    updatedAt:  new Date().toISOString(),
    mode:       'rent',
    total:      deduped.length,
    cities:     ALL_CITIES.map(c => c.name),
    properties: deduped,
  };
  safeWriteProperties(OUT_FILE, output, { minRatio: 0.6 });
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
