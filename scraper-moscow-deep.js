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
// ⚠️ ВАЖНО: Cian's `district[]` filter в search-offers/v2 ОЧЕНЬ ненадёжен.
// При probe (см. transcript 2026-05-11) даже «рабочие» ID типа 110/130/140
// возвращают перемешанные округа (3 из 15 первых записей в правильном).
// IDs ниже скорее всего служат как «hint», но не фильтруют строго.
//
// Практический эффект: глубокий проход формально по СЗАО реально вытягивает
// city-wide Moscow. Это не критично — каждая запись хранит свой настоящий
// `district` (раион) из `geo.address`, поэтому корректность ДАННЫХ ок,
// просто наши «по-окружные» отчёты — это просто city-wide с лейблом.
//
// TODO (если надо реально таргетить): построить раион→округ map и
// post-filter после parseOffer вместо доверия `district[]` фильтру.
const MOSCOW_OKRUGS = {
  // Старая Москва (within MKAD + а few exclaves like Куркино/Молжаниновский).
  'ЦАО':   [11],    // Центральный
  'САО':   [110],   // Северный
  'СВАО':  [130],   // Северо-Восточный
  'ВАО':   [14],    // Восточный  (IDs 14/18/19/20 особо подозрительны — probe-нуть)
  'ЮВАО':  [18],    // Юго-Восточный
  'ЮАО':   [19],    // Южный
  'ЮЗАО':  [20],    // Юго-Западный
  'ЗАО':   [30],    // Западный
  'СЗАО':  [140],   // Северо-Западный
  // Не подключены — пускать отдельной волной:
  //   'ЗелАО' (Зеленоград), 'НАО' / 'ТАО' (Новая Москва)
};

// Hard cap so we keep listings affordable enough to be relevant to retail
// investors — applied as `price` range in jsonQuery + post-filter.
// Override at runtime via `--max-price=5000000` (value in rubles).
const priceCapArg = (process.argv.find(a => a.startsWith('--max-price=')) || '').replace('--max-price=', '');
const PRICE_CAP_RUB = priceCapArg ? parseInt(priceCapArg, 10) : 15_000_000;

const ROOM_SPLITS = [null, 0, 1, 2, 3, 4]; // null=all rooms, 0=studio, 1-4=rooms
// Cian _type values verified against their public search-offers endpoint.
// Parking is its own _type and bundles parkingSale + garageSale + carPlaceSale.
const QUERY_TYPES = [
  { _type: 'flatsale',       label: 'квартиры/новостройки/комнаты', splitRooms: true  },
  { _type: 'commercialsale', label: 'коммерция',                    splitRooms: false },
  { _type: 'parkingsale',    label: 'паркинг/гаражи/машиноместа',   splitRooms: false },
];

// CLI:
//   --okrug=СЗАО              single okrug (legacy)
//   --okrugs=СЗАО,САО,ЦАО     comma-separated list (preferred)
// Unknown names are dropped with a warning.
const okrugArg  = (process.argv.find(a => a.startsWith('--okrug=')) || '').replace('--okrug=', '');
const okrugsArg = (process.argv.find(a => a.startsWith('--okrugs=')) || '').replace('--okrugs=', '');
let okrugsToRun;
if (okrugsArg) {
  okrugsToRun = {};
  for (const name of okrugsArg.split(',').map(s => s.trim()).filter(Boolean)) {
    if (MOSCOW_OKRUGS[name]) okrugsToRun[name] = MOSCOW_OKRUGS[name];
    else console.warn(`⚠️  unknown okrug "${name}" — пропускаю`);
  }
} else if (okrugArg) {
  okrugsToRun = MOSCOW_OKRUGS[okrugArg] ? { [okrugArg]: MOSCOW_OKRUGS[okrugArg] } : {};
} else {
  okrugsToRun = MOSCOW_OKRUGS;
}

// Commercial price-bucket segmentation. Cian caps each query at 28 pages (~784
// results); СЗАО commercial at ≤5M alone returns >540 hits across all 28 pages,
// meaning we're losing inventory at the tail. Split the commercial sweep into
// N price buckets so each bucket gets its own 28-page window.
//
// Bucket boundaries chosen so the densest segments (low-priced offices in
// central okrugs) fit inside one bucket; widens as we climb into rare high-end
// inventory where saturation is unlikely.
function commercialPriceBuckets(cap) {
  const bn = (lo, hi) => ({ lo, hi: Math.min(hi, cap) });
  if (cap <= 5_000_000) return [
    bn(0,         1_000_000),
    bn(1_000_001, 2_000_000),
    bn(2_000_001, 3_000_000),
    bn(3_000_001, 4_000_000),
    bn(4_000_001, 5_000_000),
  ];
  if (cap <= 20_000_000) return [
    bn(0,          2_500_000),
    bn(2_500_001,  5_000_000),
    bn(5_000_001, 10_000_000),
    bn(10_000_001, 15_000_000),
    bn(15_000_001, 20_000_000),
  ];
  // 500M tier — 7 buckets, log-ish spacing
  return [
    bn(0,           5_000_000),
    bn(5_000_001,  15_000_000),
    bn(15_000_001, 30_000_000),
    bn(30_000_001, 60_000_000),
    bn(60_000_001, 100_000_000),
    bn(100_000_001, 200_000_000),
    bn(200_000_001, cap),
  ].filter(b => b.lo <= b.hi);
}

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
function calcScore({ disc, roi, grow, liq, vac, type, floor }) {
  const discScore  = disc > 0 ? 30 * (1 - Math.exp(-disc / 14)) : Math.max(0, 30 + disc * 0.4);
  const roiScore   = roi > 0 ? Math.min(28, 10 * Math.log(1 + roi * 0.8)) : 0;
  const growScore  = Math.min(22, grow * 1.47);
  const liqScore   = liq * 1.2;
  const vacPenalty = vac > 5 ? (vac - 5) * 0.9 : 0;
  const rawSum     = discScore + roiScore + growScore + liqScore - vacPenalty;
  const bonus      = (discScore > 20 && roiScore > 18 && growScore > 14) ? 5 : 0;
  // Category-level weighting (tuned by user feedback):
  //   • commercial: -12 — reduced importance so it doesn't dominate flats,
  //     but still visible in top alongside apartments.
  //   • house: -20 — heavy penalty because house ppm gets compared to
  //     residential FLAT bench (240k₽/м²) but suburban houses sell at
  //     30-80k → fake "−75%" disc inflates scores to 85-88. Until we
  //     introduce a HOUSE_PPM_BENCH per region, this penalty offsets
  //     the bug so houses settle below apartments in top-N.
  const categoryPenalty = (type === 'commercial') ? 12
                       : (type === 'house')       ? 20
                       : (type === 'parking')     ? 15  // same bench bug as house/commercial
                       : 0;
  // Floor penalty for commercial: подвал / цокольный этаж резко снижает
  // ликвидность (нет вывески, нет проходимости, не годится под ритейл/услуги).
  // Парковки и склады из этого исключены — для них -1 норма.
  let floorPenalty = 0;
  if (type === 'commercial' && typeof floor === 'number' && floor < 0) floorPenalty = 12;
  if (type === 'commercial' && typeof floor === 'number' && floor === 0) floorPenalty = 6; // цоколь
  return Math.min(99, Math.max(0, Math.round(rawSum + bonus - categoryPenalty - floorPenalty)));
}
const MOSCOW_PPM = 240000;       // Жилье (флэт-bench)
const MOSCOW_RENT_PPM = 850;
// Подтипы коммерции — у каждого свой средний ppm в Москве, иначе сравнение
// с жилым 240k₽/м² даёт всем -75-80% «дисконта» и забивает топ.
// Источник: усреднённые данные по Москве 2024-2025 (Knight Frank, ЦИАН).
const COMMERCIAL_PPM_BENCH = {
  officeSale:                180000,  // офис
  shoppingAreaSale:          250000,  // торговая площадь (стрит-ритейл / ТЦ)
  freeAppointmentObjectSale: 100000,  // ПСН — свободное назначение
  warehouseSale:              50000,  // склад
  industrySale:               40000,  // производство
  buildingSale:              150000,  // здание целиком
  commercialSale:            120000,  // общий бакет / неклассифицированное
  default:                   120000,
};
const COMMERCIAL_SUBTYPE_RU = {
  officeSale:                'Офис',
  shoppingAreaSale:          'Торговая площадь',
  freeAppointmentObjectSale: 'ПСН (свободного назначения)',
  warehouseSale:             'Склад',
  industrySale:              'Производство',
  buildingSale:              'Здание',
  commercialSale:            'Коммерческое помещение',
};
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

  // ── Hard rejects before any further parsing ─────────────────────────────
  // 1) "Арендный бизнес" / "Готовый бизнес" — Cian's `businessSale` category
  //    is the sale of an operating BUSINESS (revenue stream), not the
  //    underlying real estate. Investors browsing AIvest want property,
  //    not a working laundromat — skip entirely.
  if (raw.category === 'businessSale') return null;

  // 2) Description-level guard: any commercial/apartment listing that openly
  //    says "продажа бизнеса" / "готовый бизнес" / "действующий бизнес" /
  //    "арендный бизнес" — same rationale, catches mis-categorised cases.
  const descLower = (raw.description || '').toLowerCase();
  const titleLower = (raw.title || '').toLowerCase();
  const businessRe = /(продажа\s+бизнес|готовый\s+бизнес|действующий\s+бизнес|арендный\s+бизнес|работающий\s+бизнес)/i;
  if (businessRe.test(descLower) || businessRe.test(titleLower)) return null;

  // 3) Доля (fractional ownership). Belt-and-suspenders: explicit text match
  //    in description OR title; numeric checks (ppm floor, disc ceiling) run
  //    below for cases without the word.
  const shareRe = /(продаётся\s+доля|продается\s+доля|продам\s+долю|\bдоли\b|\bдоля\b|\bдолей\b|\b\d\/\d\s+(?:доля|долей|долю)\b)/i;
  if (shareRe.test(descLower) || shareRe.test(titleLower)) return null;

  const district = geo.find(a => a.type === 'raion')?.shortName
                || geo.find(a => a.type === 'okrug')?.shortName || '';

  // Excluded districts (administratively in СЗАО but not what we want to surface).
  // Match against any address part that carries a name — raion name, street, etc.
  const addrText = geo.map(a => a.shortName || a.fullName || a.name || '').join(' ');
  if (/Куркино/i.test(addrText) || /Куркино/i.test(raw.title || '')) return null;
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
  // Per-meter floor for non-housing — same as before (parking exempt).
  if (type !== 'parking' && type !== 'apartment' && ppm > 0 && ppm < Math.round(MOSCOW_PPM * 0.20)) return null;
  // SHARES detection. Cian sells fractional ownership (1/2, 1/3 …) as
  // ordinary flatSale at proportional price, sometimes without "доля" in the
  // description. Two safety nets — if either fires, drop:
  //   • ppm under 50% of market (120k₽/м²) — proportional pricing tell
  //   • discount > 45% — structurally impossible for a real Moscow flat
  const SHARE_PPM_FLOOR    = Math.round(MOSCOW_PPM * 0.50); // 120 000 ₽/м²
  const SHARE_DISC_CEILING = 45;
  if (type === 'apartment') {
    if (ppm > 0 && ppm < SHARE_PPM_FLOOR) return null;
    // pre-compute disc for the cutoff (mirrors logic below)
    const previewDisc = MOSCOW_PPM > 0 ? ((MOSCOW_PPM - ppm) / MOSCOW_PPM) * 100 : 0;
    if (previewDisc > SHARE_DISC_CEILING) return null;
  }

  const desc = (raw.description || '').toLowerCase();
  const isShareSale  = type === 'apartment' && /продаётся доля|продается доля|продам долю|\bдоли\b|\bдоля\b/.test(desc);
  const isCommercial = type === 'apartment' && /(торговая площадь|свободного назначения|нежилое помещение|торговое помещение)/.test(desc);
  const finalType = isShareSale ? 'room' : isCommercial ? 'commercial' : type;

  // For komnaty (rooms in kommunalka): Cian's `totalArea` is the WHOLE flat,
  // not the room. Using it for ppm/disc gives fake "−78% to market" and
  // inflated score that swamps the top list. Extract room area from desc
  // when stated explicitly ("комната 11.5 м²"); fall back to 14 m² (typical
  // Moscow kommunalka room).
  function extractRoomArea(d) {
    if (!d) return null;
    const re = /(?:комнат[аы]?|комнату)\s+(?:площад[а-яё]*\s+)?(\d{1,2}[.,]?\d?)\s*(?:кв\.?\s*м|м[²2])/i;
    const m = d.match(re);
    if (!m) return null;
    const n = parseFloat(m[1].replace(',', '.'));
    return (n >= 6 && n <= 40) ? n : null;
  }
  const isRoom = finalType === 'room';
  const isCommercialType = finalType === 'commercial';
  const effectiveArea = isRoom ? (extractRoomArea(raw.description) || 14) : area;
  const effectivePpm  = effectiveArea > 0 ? Math.round(totalRub / effectiveArea) : 0;

  // Choose market reference: residential 240k for housing, commercial-subtype
  // specific bench for offices/retail/warehouse/etc. Without this, commercial
  // ppm gets compared to residential 240k and everything shows fake "−78%".
  const commercialSubtype = isCommercialType ? raw.category : null;
  const subTypeLabel = commercialSubtype ? COMMERCIAL_SUBTYPE_RU[commercialSubtype] : null;
  const marketPpm = isCommercialType
    ? (COMMERCIAL_PPM_BENCH[commercialSubtype] ?? COMMERCIAL_PPM_BENCH.default)
    : MOSCOW_PPM;

  const disc  = marketPpm > 0 ? Math.round(((marketPpm - effectivePpm) / marketPpm) * 100 * 10) / 10 : 0;
  const monthlyRent = Math.round(MOSCOW_RENT_PPM * effectiveArea);
  const annualRent  = monthlyRent * 12;
  const roi   = totalRub > 0 ? Math.round(((annualRent - totalRub * 0.04) / totalRub) * 100 * 10) / 10 : 0;
  const grow  = 9.8;
  const liq   = metro ? 9 : 8;
  const vac   = isCommercialType ? 6 : 4;
  const floorNum = Number.isFinite(raw.floorNumber) ? raw.floorNumber : null;
  const score = calcScore({ disc, roi, grow, liq, vac, type: finalType, floor: floorNum });

  // Build human-readable Russian title even when Cian's raw.title is missing.
  const TYPE_RU = {
    apartment: 'Квартира', room: 'Комната', newbuild: 'Новостройка',
    commercial: 'Коммерческая недвижимость', house: 'Дом', land: 'Участок',
    parking: 'Машиноместо',
  };
  const ROOMS_RU = ['Студия', '1-комн. квартира', '2-комн. квартира', '3-комн. квартира', '4-комн. квартира', '5+ комн. квартира'];
  let niceTitle = (raw.title || '').trim();
  // Override Cian's generic title for commercial — we want subtype prefix
  // ("Офис", "Торговая площадь", etc.) so cards aren't all "Коммерческая
  // недвижимость". Also annotate floor for commercial: -1 → "(подвал)",
  // 0 → "(цоколь)".
  if (isCommercialType && subTypeLabel) {
    const floorTag = floorNum === 0 ? ' (цоколь)' : floorNum < 0 ? ' (подвал)' : '';
    niceTitle = subTypeLabel + floorTag;
  }
  if (!niceTitle) {
    if (finalType === 'apartment' || finalType === 'newbuild') {
      const rc = Number(raw.roomsCount);
      niceTitle = (Number.isFinite(rc) && rc >= 0 && rc <= 5) ? ROOMS_RU[rc] : (TYPE_RU[finalType] || 'Квартира');
    } else {
      niceTitle = TYPE_RU[finalType] || 'Объект';
    }
  }

  return {
    id: 0,
    cianId: `cian-${raw.id}`,
    cianUrl: `https://cian.ru/sale/flat/${raw.id}/`,
    title: niceTitle,
    city: 'Москва',
    district, metro,
    // For rooms: store the room's own area; preserve the full-apartment area
    // separately so the UI can show "комната ~14м² в 75м² кв." if it wants.
    area: Math.round(effectiveArea),
    ...(isRoom ? { totalApartmentArea: Math.round(area) } : {}),
    ...(isCommercialType ? { subType: commercialSubtype, subTypeLabel } : {}),
    floor: raw.floorNumber || '—',
    type: finalType, source: SOURCE_TAG,
    price: Math.round(price * 10) / 10,
    ppm: Math.round(effectivePpm / 1000),
    mkt: Math.round(marketPpm / 1000),
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
async function fetchPage(districtIds, qtype, page, rooms, priceLo = 0, priceHi = PRICE_CAP_RUB) {
  const jsonQuery = {
    _type: qtype,
    engine_version: { type: 'term', value: 2 },
    region:  { type: 'terms', value: [1] },                  // Moscow
    district:{ type: 'terms', value: districtIds },          // okrug filter
    page:    { type: 'term',  value: page },
    // API-side price cap — Cian honours range filters, saves us pages of
    // unaffordable listings we'd just throw away in parseOffer anyway.
    price:   { type: 'range', value: { gte: priceLo, lte: priceHi } },
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
      // Pagination plan:
      //   • flatsale  → split by rooms (existing logic)
      //   • commercialsale → split by price buckets (28-page cap bypass)
      //   • parkingsale → single segment (volume is low)
      const isCommercial = qt._type === 'commercialsale';
      const splits = qt.splitRooms ? ROOM_SPLITS.map(r => ({ rooms: r })) :
                     isCommercial ? commercialPriceBuckets(PRICE_CAP_RUB).map(b => ({ rooms: null, lo: b.lo, hi: b.hi })) :
                     [{ rooms: null }];
      for (const split of splits) {
        const rooms = split.rooms;
        const lo = split.lo ?? 0;
        const hi = split.hi ?? PRICE_CAP_RUB;
        const roomLbl = rooms === null ? '' : rooms === 0 ? '[студия]' : `[${rooms}к]`;
        const priceLbl = isCommercial ? `[${(lo/1e6).toFixed(1)}–${(hi/1e6).toFixed(1)}M]` : '';
        process.stdout.write(`  ${qt.label}${roomLbl}${priceLbl}: `);
        let added = 0;
        for (let page = 1; page <= MAX_PAGES; page++) {
          try {
            const offers = await fetchPage(ids, qt._type, page, rooms, lo, hi);
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
