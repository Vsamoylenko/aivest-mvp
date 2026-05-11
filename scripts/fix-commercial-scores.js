// Recompute ppm / disc / score for type=commercial listings.
//
// Bug: parseOffer compared commercial ppm to RESIDENTIAL mktPpm (240k₽/м²
// for Moscow). Real commercial bench is much lower (50k for warehouse,
// 100k for ПСН, 180k for office, 250k for retail). So a normal-priced
// warehouse at 50k showed "−79% disc" and scored 86, beating every flat.
//
// Fix here:
//   1. Re-classify each commercial record into a sub-bucket using
//      description keywords (склад / офис / магазин / etc.) — best we can
//      do without re-scraping (raw.category isn't stored on old records).
//   2. Pick the right ppm bench per sub-type.
//   3. Recompute disc / score using that bench.
//   4. Apply floor penalty: floor < 0 (подвал) costs ~12 score points;
//      floor = 0 (цоколь) costs ~6.
//   5. Annotate `subType` field so frontend can show "Офис (подвал)" etc.
//
// Idempotent via __commercialScoreFixed marker.
//
// Usage:
//   node scripts/fix-commercial-scores.js          # writes
//   node scripts/fix-commercial-scores.js --dry    # report only

const fs = require('fs');
const path = require('path');

const FILE = path.join(__dirname, '..', 'data', 'properties.json');
const dryRun = process.argv.includes('--dry');

// City-specific residential bench (matches scraper.js).
const RESIDENTIAL_PPM = {
  'Москва': 240000, 'Санкт-Петербург': 185000, 'Краснодар': 105000,
  'Сочи': 280000, 'Казань': 125000, 'Новосибирск': 130000, 'Екатеринбург': 115000,
};
// Commercial bench in Moscow by subtype. Scale by residential ratio for other cities.
const COMMERCIAL_PPM_MOSCOW = {
  office:    180000,
  retail:    250000,
  free:      100000,
  warehouse:  50000,
  industry:   40000,
  building:  150000,
  default:   120000,
};
const SUBTYPE_RU = {
  office:    'Офис',
  retail:    'Торговая площадь',
  free:      'ПСН (свободного назначения)',
  warehouse: 'Склад',
  industry:  'Производство',
  building:  'Здание',
  default:   'Коммерческое помещение',
};

function commercialBench(city, subKey) {
  const moscow = COMMERCIAL_PPM_MOSCOW[subKey] ?? COMMERCIAL_PPM_MOSCOW.default;
  if (city === 'Москва') return moscow;
  const ratio = (RESIDENTIAL_PPM[city] ?? 130000) / 240000;
  return Math.round(moscow * ratio);
}

// Description heuristic — classify a generic 'commercial' record into one of
// office/retail/free/warehouse/industry/building.
function classifyCommercial(p) {
  const text = ((p.title || '') + ' ' + (p.description || '')).toLowerCase();
  // Order matters: more specific markers first.
  if (/\bсклад\b|складск[оа][йе]\s+помещени|склад\s+\d/i.test(text)) return 'warehouse';
  if (/производств|цех\b|завод|промышл/i.test(text)) return 'industry';
  if (/(?:торгов(?:ая|ое)|стрит-?ритейл|магазин\s+площадью|витринн)/i.test(text)) return 'retail';
  if (/\bофис(?:е|ом|ный|ное|а)?\b|business\s*center|бизнес-?центр/i.test(text)) return 'office';
  if (/свободного\s+назначени|псн\b|нежилое\s+помещени/i.test(text)) return 'free';
  if (/здание\s+целиком|отдельно\s+стоящ|особняк/i.test(text)) return 'building';
  return 'default';
}

function parseFloorNum(floorStr) {
  if (!floorStr || floorStr === '—') return null;
  const m = String(floorStr).match(/^(-?\d+)/);
  return m ? parseInt(m[1], 10) : null;
}

function calcScore({ disc, roi, grow, liq, vac, type, floor }) {
  const discScore  = disc > 0 ? 30 * (1 - Math.exp(-disc / 14)) : Math.max(0, 30 + disc * 0.4);
  const roiScore   = roi > 0 ? Math.min(28, 10 * Math.log(1 + roi * 0.8)) : 0;
  const growScore  = Math.min(22, grow * 1.47);
  const liqScore   = liq * 1.2;
  const vacPenalty = vac > 5 ? (vac - 5) * 0.9 : 0;
  const rawSum     = discScore + roiScore + growScore + liqScore - vacPenalty;
  const bonus      = (discScore > 20 && roiScore > 18 && growScore > 14) ? 5 : 0;
  const categoryPenalty = (type === 'house') ? 5 : 0;
  let floorPenalty = 0;
  if (type === 'commercial' && typeof floor === 'number' && floor < 0) floorPenalty = 12;
  if (type === 'commercial' && typeof floor === 'number' && floor === 0) floorPenalty = 6;
  return Math.min(99, Math.max(0, Math.round(rawSum + bonus - categoryPenalty - floorPenalty)));
}

const json = JSON.parse(fs.readFileSync(FILE, 'utf8'));
const props = json.properties || [];

let touched = 0, skipped = 0;
const subCounts = {};
const sampleBefore = [];
const sampleAfter  = [];

for (const p of props) {
  if (p.type !== 'commercial') continue;
  if (p.__commercialScoreFixed) { skipped++; continue; }
  if (!p.price || p.price <= 0 || !p.area || p.area <= 0) { skipped++; continue; }

  const before = { id: p.id, title: p.title?.slice(0,30), ppm: p.ppm, disc: p.disc, score: p.score, floor: p.floor };

  const subKey = classifyCommercial(p);
  subCounts[subKey] = (subCounts[subKey] || 0) + 1;
  const subLabel = SUBTYPE_RU[subKey];
  const mkt = commercialBench(p.city, subKey);

  const totalRub = p.price * 1e6;
  const ppmRub = totalRub / p.area;
  const ppmK   = Math.round(ppmRub / 1000);
  const disc   = Math.round(((mkt - ppmRub) / mkt) * 100 * 10) / 10;

  // ROI uses rent stream. For commercial we keep the (admittedly loose)
  // residential rent rate × area + vacancy. Refining the rent model for
  // commercial is a separate task.
  const monthlyRent = p.rent ? p.rent * 1000 : 0;
  const annualRent = monthlyRent * 12;
  const vac = p.vac || 6;
  const roi = totalRub > 0 ? Math.round(((annualRent * (1 - vac/100)) / totalRub) * 100 * 10) / 10 : 0;
  const liq = p.liq || 8;
  const grow = p.grow || 9.8;
  const floor = parseFloorNum(p.floor);
  const score = calcScore({ disc, roi, grow, liq, vac, type: 'commercial', floor });

  // Update record
  p.ppm = ppmK;
  p.mkt = Math.round(mkt / 1000);
  p.disc = disc;
  p.roi = roi;
  p.score = score;
  p.subType = subKey;
  p.subTypeLabel = subLabel;
  p.__commercialScoreFixed = true;

  // Improve title for generic Cian-default labels
  if (/^Коммерческая недвижимость$/i.test(p.title || '') || /^Коммерция/i.test(p.title || '')) {
    const floorTag = floor === 0 ? ' (цоколь)' : floor < 0 ? ' (подвал)' : '';
    p.title = subLabel + floorTag + (p.district ? ', ' + p.district : '');
  }
  // Re-evaluate badge: only "Ниже рынка" if real disc ≥ 12% under proper bench
  if (p.badge === 'Ниже рынка' && disc < 12) p.badge = undefined;

  if (sampleBefore.length < 8) {
    sampleBefore.push(before);
    sampleAfter.push({ id: p.id, sub: subKey, ppm: ppmK, mkt: Math.round(mkt/1000), disc, score, floor });
  }
  touched++;
}

console.log(`Commercial touched: ${touched}`);
console.log(`Skipped (already done or no price/area): ${skipped}`);
console.log(`Subtype distribution:`, subCounts);

console.log('\n— Before / after samples —');
for (let i = 0; i < sampleBefore.length; i++) {
  const b = sampleBefore[i], a = sampleAfter[i];
  console.log(`#${b.id} ${a.sub.padEnd(9)} ppm: ${b.ppm}k→${a.ppm}k mkt:?→${a.mkt}k disc: ${b.disc}→${a.disc}% score: ${b.score}→${a.score} floor=${a.floor}`);
  console.log(`   was: ${b.title}`);
}

if (dryRun) {
  console.log('\n[--dry] no write.');
  process.exit(0);
}

const output = { ...json, updatedAt: new Date().toISOString() };
const { safeWriteProperties } = require('../lib-safe-write');
safeWriteProperties(FILE, output);
console.log(`\n✅ Wrote ${props.length} props (${touched} commercial re-scored).`);
