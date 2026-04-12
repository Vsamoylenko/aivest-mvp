// AIvest — Cian.ru Scraper
// Fetches real listings from Cian API, calculates AI score, saves to data/properties.json
// Usage: node scraper.js          → run once
//        node scraper.js --watch  → run + auto-refresh every 6h

require('dotenv').config();
const axios = require('axios');
const fs    = require('fs');
const path  = require('path');

// ── Config ──────────────────────────────────────────────────────────────────
const DATA_DIR  = path.join(__dirname, 'data');
const OUT_FILE  = path.join(DATA_DIR, 'properties.json');
const PAGES_PER_CITY = parseInt(process.env.SCRAPER_PAGES || '3'); // 28 offers/page
const DELAY_MS  = 1200; // polite delay between requests

// City configs:
//   regionId  = Cian region used in query (oblast or city level)
//   cityId    = Cian location ID for the actual city (to filter results)
//   name      = display name
//   rentPpm   = avg rental rate ₽/m²/month (for yield estimation)
//   growth    = avg price growth %/yr
const CITIES = [
  { name: 'Москва',          regionId: 1,    cityId: 1,    rentPpm: 850,  growth: 9.8  },
  { name: 'Санкт-Петербург', regionId: 2,    cityId: 2,    rentPpm: 680,  growth: 8.5  },
  { name: 'Краснодар',       regionId: 4820, cityId: 4820, rentPpm: 430,  growth: 11.0 },
  { name: 'Сочи',            regionId: 4584, cityId: 4998, rentPpm: 920,  growth: 13.1 },
  { name: 'Казань',          regionId: 4618, cityId: 4777, rentPpm: 450,  growth: 9.1  },
  { name: 'Новосибирск',     regionId: 4897, cityId: 4897, rentPpm: 370,  growth: 8.8  },
  { name: 'Екатеринбург',    regionId: 4743, cityId: 4743, rentPpm: 400,  growth: 9.4  },
];

// Category → type mapping
const CATEGORY_TYPE = {
  flatSale:            'apartment',
  newBuildingFlatSale: 'newbuild',
  roomSale:            'apartment',
  commercialSale:      'commercial',
  officeSale:          'commercial',
  houseSale:           'house',
  cottageSale:         'house',
  townhouseSale:       'house',
  landSale:            'land',
};

const SOURCE_TAG = 'cian';

// ── Helpers ──────────────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Extract the most specific city location (last location-type entry = city, not oblast)
function extractCity(geoAddress = [], cityId = null) {
  const locs = geoAddress.filter(a => a.type === 'location');
  if (cityId) {
    const match = locs.find(l => l.id === cityId || String(l.id) === String(cityId));
    if (match) return match.name;
  }
  // Fallback: last location entry is usually the most specific (city)
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

// ── AI Scoring (same model as frontend) ──────────────────────────────────────
function calcScore({ disc, roi, grow, liq, vac }) {
  const discScore = disc > 0
    ? 30 * (1 - Math.exp(-disc / 14))
    : Math.max(0, 30 + disc * 0.4);

  const roiScore = roi > 0
    ? Math.min(28, 10 * Math.log(1 + roi * 0.8))
    : 0;

  const growScore   = Math.min(22, grow * 1.47);
  const liqScore    = liq * 1.2;
  const vacPenalty  = vac > 5 ? (vac - 5) * 0.9 : 0;
  const rawSum      = discScore + roiScore + growScore + liqScore - vacPenalty;
  const bonus       = (discScore > 20 && roiScore > 18 && growScore > 14) ? 5 : 0;

  return Math.min(99, Math.max(0, Math.round(rawSum + bonus)));
}

// Estimate market price per m² for a city (rough heuristic from current market data)
function marketPpm(cityName) {
  const map = {
    'Москва':          240000,
    'Санкт-Петербург': 185000,
    'Краснодар':       105000,
    'Сочи':            280000,
    'Казань':          125000,
    'Новосибирск':     130000,
    'Екатеринбург':    115000,
  };
  return map[cityName] || 130000;
}

// Estimate liquidity 1–10 based on city and property type
function estimateLiquidity(cityName, type, metro) {
  let base = { 'Москва': 8.5, 'Санкт-Петербург': 8, 'Сочи': 7.5, 'Казань': 7, 'Краснодар': 7, 'Новосибирск': 7, 'Екатеринбург': 7 }[cityName] || 6.5;
  if (metro) base = Math.min(10, base + 0.5);
  if (type === 'land') base -= 2;
  if (type === 'commercial') base -= 0.5;
  return Math.round(Math.max(1, Math.min(10, base)));
}

// Estimate vacancy % by type and city
function estimateVacancy(type, cityName) {
  if (type === 'land') return 0;
  if (cityName === 'Сочи') return 10; // tourism = seasonal vacancy
  if (type === 'commercial') return 6;
  return 4;
}

// ── Parse one raw offer into AIvest property format ───────────────────────────
let globalId = 1;

function parseOffer(raw, cityConfig) {
  const geo      = raw.geo?.address || [];
  // Verify this offer actually belongs to the target city (filter out nearby towns)
  const locs = geo.filter(a => a.type === 'location');
  const inCity = locs.some(l => String(l.id) === String(cityConfig.cityId));
  if (!inCity) return null; // skip listings from other cities in the same region

  const cityName = extractCity(geo, cityConfig.cityId) || cityConfig.name;
  const district = extractDistrict(geo);
  const metro    = extractMetro(geo);
  const area     = parseFloat(raw.totalArea) || 0;
  const price    = (raw.bargainTerms?.price || 0) / 1e6; // млн ₽
  const ppm      = area > 0 ? Math.round((raw.bargainTerms?.price || 0) / area) : 0;
  const mktPpm   = marketPpm(cityName);
  const disc     = mktPpm > 0 ? Math.round(((mktPpm - ppm) / mktPpm) * 100 * 10) / 10 : 0;

  const type = CATEGORY_TYPE[raw.category] || 'apartment';

  // Monthly rent estimate: city avg rent/m² × area
  const monthlyRent = Math.round(cityConfig.rentPpm * area);
  const vac         = estimateVacancy(type, cityName);
  const annualRent  = monthlyRent * 12 * (1 - vac / 100);
  const roi         = price > 0 ? Math.round((annualRent / (price * 1e6)) * 100 * 10) / 10 : 0;

  const liq   = estimateLiquidity(cityName, type, metro);
  const grow  = cityConfig.growth;
  const score = calcScore({ disc, roi, grow, liq, vac });

  // Build title
  const roomsLabel = { 1: '1-комн.', 2: '2-комн.', 3: '3-комн.', 4: '4-комн.' }[raw.roomsCount] || 'Студия';
  const typeLabel  = type === 'newbuild' ? 'Новостройка' : type === 'house' ? 'Дом' : type === 'commercial' ? 'Коммерция' : type === 'land' ? 'Участок' : null;
  const titleBase  = typeLabel || (raw.roomsCount ? roomsLabel + ' кв.' : 'Квартира');
  const titleLoc   = district ? district : metro ? metro : cityName;
  const title      = `${titleBase}, ${titleLoc}`;

  // Floor string
  const floor = raw.floorNumber && raw.building?.floorsCount
    ? `${raw.floorNumber}/${raw.building.floorsCount}`
    : raw.floorNumber ? `${raw.floorNumber}/?` : '—';

  // Badge
  let badge = '';
  if (disc >= 20) badge = 'Ниже рынка';
  else if (type === 'newbuild') badge = 'Новостройка';
  else if (metro && (cityName === 'Москва' || cityName === 'Санкт-Петербург')) badge = 'Метро рядом';
  else if (cityName === 'Сочи') badge = 'Туризм';

  return {
    id:      raw.id || (globalId++),
    cianId:  raw.id,
    cianUrl: raw.fullUrl || `https://cian.ru/sale/flat/${raw.id}/`,
    title,
    city:    cityName,
    district,
    metro,
    area:    Math.round(area),
    floor,
    type,
    source:  SOURCE_TAG,
    price:   Math.round(price * 10) / 10,
    ppm:     Math.round(ppm / 1000),    // тыс. ₽/м² (same scale as mock data)
    mkt:     Math.round(mktPpm / 1000), // тыс. ₽/м²
    rent:    Math.round(monthlyRent / 1000), // тыс. ₽/мес
    vac,
    grow,
    liq,
    badge,
    score,
    disc,
    roi,
    photos:  raw.photos?.map(p => p.fullUrl || p.thumbnail2Url).filter(Boolean).slice(0, 5) || [],
    description: raw.description?.slice(0, 200) || '',
    addedAt: raw.addedTimestamp ? new Date(raw.addedTimestamp * 1000).toISOString() : new Date().toISOString(),
    scrapedAt: new Date().toISOString(),
  };
}

// ── Fetch one page from Cian API ──────────────────────────────────────────────
async function fetchPage(regionId, offerType, page) {
  const queryType = offerType === 'sale' ? 'flatsale' : 'flatrent';
  const payload = {
    jsonQuery: {
      _type: queryType,
      engine_version: { type: 'term', value: 2 },
      region:         { type: 'terms', value: [regionId] },
      page:           { type: 'term', value: page },
    }
  };

  const res = await axios.post(
    'https://api.cian.ru/search-offers/v2/search-offers-desktop/',
    payload,
    {
      headers: {
        'Content-Type': 'application/json',
        'User-Agent':   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Referer':      'https://cian.ru/',
        'Accept':       'application/json',
      },
      timeout: 15000,
    }
  );

  return res.data?.data?.offersSerialized || [];
}

// ── Main scrape function ──────────────────────────────────────────────────────
async function scrape() {
  console.log(`\n🔍 AIvest Scraper — started at ${new Date().toLocaleString('ru-RU')}`);
  console.log(`   Cities: ${CITIES.map(c => c.name).join(', ')}`);
  console.log(`   Pages per city: ${PAGES_PER_CITY} (≈${PAGES_PER_CITY * 28} listings each)\n`);

  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

  const allProps = [];
  let totalFetched = 0;
  let errors = 0;

  for (const city of CITIES) {
    console.log(`📍 ${city.name}...`);

    for (let page = 1; page <= PAGES_PER_CITY; page++) {
      try {
        const offers = await fetchPage(city.regionId, 'sale', page);
        if (!offers.length) { console.log(`   Page ${page}: empty, stopping`); break; }

        const parsed = offers
          .filter(o => o.totalArea && o.bargainTerms?.price)
          .map(o => parseOffer(o, city))
          .filter(Boolean); // remove null (off-city offers)

        allProps.push(...parsed);
        totalFetched += parsed.length;
        process.stdout.write(`   Page ${page}: +${parsed.length} (total ${totalFetched})\n`);
        await sleep(DELAY_MS);
      } catch (err) {
        errors++;
        console.error(`   ❌ Page ${page} error: ${err.message}`);
        await sleep(2000);
      }
    }
  }

  // Deduplicate by cianId
  const seen = new Set();
  const deduped = allProps.filter(p => {
    if (seen.has(p.cianId)) return false;
    seen.add(p.cianId);
    return true;
  });

  // Sort by score desc
  deduped.sort((a, b) => b.score - a.score);

  // Re-index IDs
  deduped.forEach((p, i) => { p.id = i + 1; });

  const output = {
    updatedAt:   new Date().toISOString(),
    totalCount:  deduped.length,
    cities:      CITIES.map(c => c.name),
    properties:  deduped,
  };

  fs.writeFileSync(OUT_FILE, JSON.stringify(output, null, 2));

  const top3 = deduped.slice(0, 3).map(p => `${p.title} — ${p.score}pts`);
  console.log(`\n✅ Done! ${deduped.length} unique properties saved to data/properties.json`);
  console.log(`   Errors: ${errors}`);
  console.log(`   Top 3:\n   ${top3.join('\n   ')}`);
  console.log(`   Score distribution: 80+: ${deduped.filter(p=>p.score>=80).length} | 60-79: ${deduped.filter(p=>p.score>=60&&p.score<80).length} | <60: ${deduped.filter(p=>p.score<60).length}\n`);

  return output;
}

// ── Watch mode ────────────────────────────────────────────────────────────────
async function main() {
  await scrape();

  if (process.argv.includes('--watch')) {
    const cron = require('node-cron');
    console.log('⏰ Watch mode: re-scraping every 6 hours...');
    cron.schedule('0 */6 * * *', async () => {
      console.log('\n🔄 Scheduled re-scrape...');
      await scrape();
    });
  }
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
