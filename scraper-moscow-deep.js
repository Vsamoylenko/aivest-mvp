// Moscow-deep scraper — focused pass on specific okrugs to bypass Cian's 28-page
// per-segment cap. Splits queries by okrug + room count, dramatically multiplying
// effective coverage in the requested zones.
//
// Usage:
//   node scraper-moscow-deep.js                 # all configured okrugs
//   node scraper-moscow-deep.js --okrug=СЗАО    # single okrug
//
// Merge logic: same as scraper.js — preserves existing data, refreshes overlapping
// items (keeping addedAt), keeps stale items intact (their scrapedAt drives the
// frontend "Только активные" 90-day filter).

require('dotenv').config();
const axios = require('axios');
const fs    = require('fs');
const path  = require('path');

const DATA_DIR  = path.join(__dirname, 'data');
const OUT_FILE  = path.join(DATA_DIR, 'properties.json');
const MAX_PAGES = 28;
const DELAY_MS  = parseInt(process.env.SCRAPER_DELAY || '900');
const SOURCE_TAG = 'agg';

// Moscow okrug → Cian district IDs.
// IDs verified from Cian's public search URLs (site:cian.ru SZAO produces district[0]=NN).
// If a query returns 0 offers across all rooms, the ID is wrong — log and skip.
const MOSCOW_OKRUGS = {
  'СЗАО':  [140],   // Северо-Западный (primary focus)
  'САО':   [110],   // Северный (primary focus)
  // Other okrugs disabled — current pass focuses on north/north-west:
  // 'ЦАО':   [11], 'ЗАО':   [30],
  // 'СВАО':  [130], 'ВАО': [14], 'ЮВАО': [18], 'ЮАО': [19], 'ЮЗАО': [20]
};

// Hard cap so we keep listings affordable enough to be relevant to retail
// investors — applied as `price` range in jsonQuery + post-filter.
const PRICE_CAP_RUB = 15_000_000;

const ROOM_SPLITS = [null, 0, 1, 2, 3, 4]; // null=all rooms, 0=studio, 1-4=rooms
// Cian _type values verified against their public search-offers endpoint.
// Parking is its own _type and bundles parkingSale + garageSale + carPlaceSale.
const QUERY_TYPES = [
  { _type: 'flatsale',       label: 'квартиры/новостройки/комнаты', splitRooms: true  },
  { _type: 'commercialsale', label: 'коммерция',                    splitRooms: false },
  { _type: 'parkingsale',    label: 'паркинг/гаражи/машиноместа',   splitRooms: false },
];

const okrugArg = (process.argv.find(a => a.startsWith('--okrug=')) || '').replace('--okrug=', '');
const okrugsToRun = okrugArg
  ? { [okrugArg]: MOSCOW_OKRUGS[okrugArg] }
  : MOSCOW_OKRUGS;

// ── Reuse helpers from main scraper ────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const UA_LIST = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
];
let uaIdx = 0;
const nextUA = () => UA_LIST[(uaIdx++) % UA_LIST.length];

// Parsing: import from scraper.js by requiring it as a module would be ideal,
// but scraper.js is a script, not exporting. Quickest path is to inline parseOffer
// here, but that doubles the maintenance surface. Instead: reuse via require.cache
// trick — require scraper.js with a flag that suppresses its main(). Simpler: just
// produce a minimal record and rely on the merge step in scraper.js to enrich.
// FOR NOW: minimal parser sufficient for cards (matches scraper.js shape).
function calcScore({ disc, roi, grow, liq, vac, type }) {
  const discScore  = disc > 0 ? 30 * (1 - Math.exp(-disc / 14)) : Math.max(0, 30 + disc * 0.4);
  const roiScore   = roi > 0 ? Math.min(28, 10 * Math.log(1 + roi * 0.8)) : 0;
  const growScore  = Math.min(22, grow * 1.47);
  const liqScore   = liq * 1.2;
  const vacPenalty = vac > 5 ? (vac - 5) * 0.9 : 0;
  const rawSum     = discScore + roiScore + growScore + liqScore - vacPenalty;
  const bonus      = (discScore > 20 && roiScore > 18 && growScore > 14) ? 5 : 0;
  const categoryPenalty = (type === 'house') ? 5 : 0;
  return Math.min(99, Math.max(0, Math.round(rawSum + bonus - categoryPenalty)));
}
const MOSCOW_PPM = 240000;
const MOSCOW_RENT_PPM = 850;
const CATEGORY_TYPE = {
  flatSale: 'apartment', newBuildingFlatSale: 'newbuild', roomSale: 'room',
  commercialSale: 'commercial', officeSale: 'commercial',
  freeAppointmentObjectSale: 'commercial', shoppingAreaSale: 'commercial',
  industrySale: 'commercial', warehouseSale: 'commercial',
  buildingSale: 'commercial', businessSale: 'commercial',
  commercialLandSale: 'land',
  houseSale: 'house', cottageSale: 'house', townhouseSale: 'house',
  landSale: 'land',
  parkingSale: 'parking', garageSale: 'parking', carPlaceSale: 'parking',
};

function parseOffer(raw) {
  const geo = raw.geo?.address || [];
  const locs = geo.filter(a => a.type === 'location');
  const inMoscow = locs.some(l => String(l.id) === '1');
  if (!inMoscow) return null;

  const district = geo.find(a => a.type === 'raion')?.shortName
                || geo.find(a => a.type === 'okrug')?.shortName || '';
  const metro    = geo.find(a => a.type === 'metro')?.shortName || '';
  const area     = parseFloat(raw.totalArea) || 0;
  const rawPrice  = raw.bargainTerms?.price || 0;
  const priceType = raw.bargainTerms?.priceType || '';
  const isByMeter = priceType === 'square' || priceType === 'sqm';
  const totalRub  = isByMeter && area > 0 ? rawPrice * area : rawPrice;
  const price     = totalRub / 1e6;
  const ppm       = area > 0 ? Math.round(totalRub / area) : 0;
  const type  = CATEGORY_TYPE[raw.category] || 'apartment';

  if (type === 'apartment' && area >= 500) return null;
  if (type === 'apartment' && price > 500) return null;
  if (type === 'apartment' && raw.floorNumber < 0) return null;
  // Hard cap from CLI: drop everything pricier than PRICE_CAP_RUB.
  if (totalRub > PRICE_CAP_RUB) return null;
  // Min-price floor — only apply to housing. Parking spots routinely sell
  // at 300-900k₽, garages even less. The 1M floor was excluding ~all of them.
  if (type !== 'parking' && price > 0 && price < 1) return null;
  // Per-meter floor — also housing-only. Parking has its own market dynamic
  // (priced per spot, not per m²) where ppm of 60-100k is typical and not a
  // sign of fraud the way it would be for a flat.
  if (type !== 'parking' && ppm > 0 && ppm < Math.round(MOSCOW_PPM * 0.20)) return null;

  const desc = (raw.description || '').toLowerCase();
  const isShareSale  = type === 'apartment' && /продаётся доля|продается доля|продам долю|\bдоли\b|\bдоля\b/.test(desc);
  const isCommercial = type === 'apartment' && /(торговая площадь|свободного назначения|нежилое помещение|торговое помещение)/.test(desc);
  const finalType = isShareSale ? 'room' : isCommercial ? 'commercial' : type;

  const disc  = MOSCOW_PPM > 0 ? Math.round(((MOSCOW_PPM - ppm) / MOSCOW_PPM) * 100 * 10) / 10 : 0;
  const monthlyRent = Math.round(MOSCOW_RENT_PPM * area);
  const annualRent  = monthlyRent * 12;
  const roi   = totalRub > 0 ? Math.round(((annualRent - totalRub * 0.04) / totalRub) * 100 * 10) / 10 : 0;
  const grow  = 9.8;
  const liq   = metro ? 9 : 8;
  const vac   = finalType === 'commercial' ? 6 : 4;
  const score = calcScore({ disc, roi, grow, liq, vac, type: finalType });

  return {
    id: 0,
    cianId: `cian-${raw.id}`,
    cianUrl: `https://cian.ru/sale/flat/${raw.id}/`,
    title: raw.title || `${finalType} в Москве`,
    city: 'Москва',
    district, metro,
    area: Math.round(area),
    floor: raw.floorNumber || '—',
    type: finalType, source: SOURCE_TAG,
    price: Math.round(price * 10) / 10,
    ppm: Math.round(ppm / 1000),
    mkt: Math.round(MOSCOW_PPM / 1000),
    rent: Math.round(monthlyRent / 1000),
    vac, grow, liq,
    badge: disc > 12 ? 'Ниже рынка' : undefined,
    score, disc, roi,
    photos: (raw.photos || []).slice(0, 5).map(p => p.fullUrl).filter(Boolean),
    description: (raw.description || '').slice(0, 600),
    addedAt: raw.addedTimestamp ? new Date(raw.addedTimestamp * 1000).toISOString() : new Date().toISOString(),
    scrapedAt: new Date().toISOString(),
  };
}

// ── Fetch one page with okrug + rooms + price filters ─────────────────────
async function fetchPage(districtIds, qtype, page, rooms) {
  const jsonQuery = {
    _type: qtype,
    engine_version: { type: 'term', value: 2 },
    region:  { type: 'terms', value: [1] },                  // Moscow
    district:{ type: 'terms', value: districtIds },          // okrug filter
    page:    { type: 'term',  value: page },
    // API-side price cap — Cian honours range filters, saves us pages of
    // unaffordable listings we'd just throw away in parseOffer anyway.
    price:   { type: 'range', value: { gte: 0, lte: PRICE_CAP_RUB } },
  };
  if (rooms !== null) jsonQuery.room = { type: 'terms', value: [rooms] };

  const res = await axios.post(
    'https://api.cian.ru/search-offers/v2/search-offers-desktop/',
    { jsonQuery },
    {
      headers: {
        'Content-Type': 'application/json',
        'User-Agent':   nextUA(),
        'Referer':      'https://cian.ru/',
        'Origin':       'https://cian.ru',
        'Accept':       'application/json, text/plain, */*',
        'Accept-Language': 'ru-RU,ru;q=0.9',
      },
      timeout: 20000,
    }
  );
  return res.data?.data?.offersSerialized || [];
}

// ── Main ───────────────────────────────────────────────────────────────────
(async () => {
  console.log(`\n🔍 Moscow-deep scraper — ${new Date().toLocaleString('ru-RU')}`);
  console.log(`   Targets: ${Object.keys(okrugsToRun).join(', ')}\n`);

  const allProps = [];
  const seen = new Set();

  for (const [okrugName, ids] of Object.entries(okrugsToRun)) {
    console.log(`📍 ${okrugName} (district[]=${ids.join(',')})`);
    for (const qt of QUERY_TYPES) {
      const splits = qt.splitRooms ? ROOM_SPLITS : [null];
      for (const rooms of splits) {
        const roomLbl = rooms === null ? '' : rooms === 0 ? '[студия]' : `[${rooms}к]`;
        process.stdout.write(`  ${qt.label}${roomLbl}: `);
        let added = 0;
        for (let page = 1; page <= MAX_PAGES; page++) {
          try {
            const offers = await fetchPage(ids, qt._type, page, rooms);
            if (!offers.length) { process.stdout.write(`стр.${page} пусто\n`); break; }
            let pageAdded = 0;
            for (const o of offers) {
              if (!o.totalArea || !o.bargainTerms?.price) continue;
              if (seen.has(o.id)) continue;
              const parsed = parseOffer(o);
              if (!parsed) continue;
              seen.add(o.id);
              allProps.push(parsed);
              pageAdded++; added++;
            }
            process.stdout.write(`${page}(+${pageAdded}) `);
            await sleep(DELAY_MS);
          } catch (err) {
            const msg = err.response ? `HTTP ${err.response.status}` : err.message?.slice(0, 60);
            process.stdout.write(`❌(${msg}) `);
            if (err.response?.status === 403 || err.response?.status === 503) {
              process.stdout.write(`\n  ⛔ Blocked, skipping segment\n`);
              break;
            }
            await sleep(3000);
          }
        }
        process.stdout.write(`→ итого +${added}\n`);
      }
    }
    console.log('');
  }

  if (allProps.length === 0) {
    console.log('⚠️  0 объектов — скорее всего, заблокированы Cian. Попробуйте позже.');
    return;
  }

  // ── Merge with existing properties.json (same logic as scraper.js) ──────
  let finalProps = allProps;
  if (fs.existsSync(OUT_FILE)) {
    const old = JSON.parse(fs.readFileSync(OUT_FILE, 'utf8'));
    const oldAll = old.properties || [];
    const preserved = oldAll.filter(p => p.source !== SOURCE_TAG);
    const oldAggByCianId = new Map(oldAll.filter(p => p.source === SOURCE_TAG).map(p => [p.cianId, p]));
    const freshByCianId = new Map(allProps.map(p => [p.cianId, p]));
    let refreshed = 0, added = 0, stale = 0;
    const merged = [];
    for (const oldP of oldAggByCianId.values()) {
      const fresh = freshByCianId.get(oldP.cianId);
      if (fresh) {
        merged.push({ ...fresh, addedAt: oldP.addedAt || fresh.addedAt });
        freshByCianId.delete(oldP.cianId);
        refreshed++;
      } else {
        merged.push(oldP);
        stale++;
      }
    }
    for (const fresh of freshByCianId.values()) { merged.push(fresh); added++; }
    finalProps = [...merged, ...preserved];
    finalProps.sort((a, b) => (b.score || 0) - (a.score || 0));
    finalProps.forEach((p, i) => { p.id = i + 1; });
    console.log(`\n   Merge: ${refreshed} обновлено, ${added} новых, ${stale} не найдены этим прогоном, ${preserved.length} non-agg`);
  }

  const output = {
    updatedAt: new Date().toISOString(),
    totalCount: finalProps.length,
    cities: ['Москва','Санкт-Петербург','Краснодар','Сочи','Казань','Новосибирск','Екатеринбург'],
    properties: finalProps,
  };
  const { safeWriteProperties } = require('./lib-safe-write');
  safeWriteProperties(OUT_FILE, output);
  console.log(`✅ Готово! ${allProps.length} новых из Moscow-deep, ${finalProps.length} всего\n`);
})().catch(err => {
  console.error('FAIL', err.message);
  process.exit(1);
});
