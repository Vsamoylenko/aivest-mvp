// Recompute ppm / disc / roi / score for type=room listings.
//
// Bug: parseOffer in scraper.js / scraper-moscow-deep.js uses
//   ppm = price / totalArea
// For a kommunalka room, `totalArea` is the WHOLE apartment (60–300 m²), not
// the room. That makes ppm look ~5× below market, disc shows "−78%", and the
// AI score lands at 87–88 — so rooms swamp the top-N investment list even
// though they're tiny lots, not real bargains.
//
// Fix here: assume a representative Moscow kommunalka room ≈ 14 m². Recompute
// ppm/disc/roi/score from that. We try description-based extraction first
// (works for ~30% of listings — those that explicitly say "комната 11.5 м²"),
// then fall back to the 14 m² heuristic.
//
// Idempotent: a marker flag `__roomScoreFixed: true` is set so re-running is
// a no-op.
//
// Usage:
//   node scripts/fix-room-scores.js          # writes
//   node scripts/fix-room-scores.js --dry    # report only

const fs = require('fs');
const path = require('path');

const FILE = path.join(__dirname, '..', 'data', 'properties.json');
const dryRun = process.argv.includes('--dry');

const MOSCOW_PPM      = 240000;   // matches scraper.js market reference
const MOSCOW_RENT_PPM = 850;
const ROOM_FALLBACK_M2 = 14;      // typical Moscow kommunalka room

// Description heuristic — only fires when the wording is unambiguous.
function extractRoomArea(desc) {
  if (!desc) return null;
  // "комната 11.5 м²" / "комнату 16,2 кв.м" / "комната площадью 23 м²"
  const patterns = [
    /(?:комнат[аы]?|комнату)\s+(?:площад[а-яё]*\s+)?(\d{1,2}[.,]?\d?)\s*(?:кв\.?\s*м|м[²2])/i,
    /(?:комнат[аы]?|комнату)[^.0-9]{0,40}?(\d{1,2}[.,]?\d?)\s*(?:кв\.?\s*м|м[²2])/i,
    /площад[а-яё]*\s+комнаты\s+(\d{1,2}[.,]?\d?)\s*(?:кв\.?\s*м|м[²2])/i,
  ];
  for (const re of patterns) {
    const m = desc.match(re);
    if (m) {
      const n = parseFloat(m[1].replace(',', '.'));
      if (n >= 6 && n <= 40) return n; // sanity guard: rooms are 6-40 m²
    }
  }
  return null;
}

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

const json = JSON.parse(fs.readFileSync(FILE, 'utf8'));
const props = json.properties || [];

let touched = 0, descBased = 0, fallbackBased = 0, skipped = 0;
const sampleBefore = [];
const sampleAfter  = [];

for (const p of props) {
  if (p.type !== 'room') continue;
  if (p.__roomScoreFixed) { skipped++; continue; }
  if (!p.price || p.price <= 0) { skipped++; continue; }

  // Snapshot for sample
  const before = { id: p.id, ppm: p.ppm, disc: p.disc, roi: p.roi, score: p.score, area: p.area };

  // Room area: description-first, fallback to 14 m².
  const fromDesc = extractRoomArea(p.description || '');
  const roomArea = fromDesc ?? ROOM_FALLBACK_M2;
  if (fromDesc) descBased++; else fallbackBased++;

  const totalRub = p.price * 1e6;
  const newPpmRub = totalRub / roomArea;
  const newPpmK   = Math.round(newPpmRub / 1000);
  const newDisc   = Math.round(((MOSCOW_PPM - newPpmRub) / MOSCOW_PPM) * 100 * 10) / 10;
  // ROI uses room area for rent stream — a room rents at ~850₽/m²/mo just
  // like any other Moscow housing.
  const monthlyRent = Math.round(MOSCOW_RENT_PPM * roomArea);
  const annualRent  = monthlyRent * 12;
  const vac = 4;
  const newRoi = Math.round(((annualRent * (1 - vac/100)) / totalRub) * 100 * 10) / 10;
  const liq = p.liq || 8;
  const grow = p.grow || 9.8;
  const newScore = calcScore({ disc: newDisc, roi: newRoi, grow, liq, vac, type: 'room' });

  p.ppm = newPpmK;
  p.disc = newDisc;
  p.roi = newRoi;
  p.rent = Math.round(monthlyRent / 1000);
  p.score = newScore;
  // Preserve the original apartment area for context — frontend can show
  // both ("комната ~14 м² в 75 м² кв.") if it wants. For now just stash
  // total area into a new field and overwrite area with room area so all
  // displays use the room number consistently.
  p.totalApartmentArea = p.area;
  p.area = Math.round(roomArea);
  p.__roomScoreFixed = true;

  if (sampleBefore.length < 5) {
    sampleBefore.push(before);
    sampleAfter.push({ id: p.id, ppm: newPpmK, disc: newDisc, roi: newRoi, score: newScore, area: p.area, src: fromDesc ? 'desc' : 'heur' });
  }
  touched++;
}

console.log(`Rooms touched:        ${touched}`);
console.log(`  from description:   ${descBased}`);
console.log(`  fallback (14 m²):   ${fallbackBased}`);
console.log(`Skipped (already done or no price): ${skipped}`);

console.log('\n— Before / after samples —');
for (let i = 0; i < sampleBefore.length; i++) {
  const b = sampleBefore[i], a = sampleAfter[i];
  console.log(`#${b.id}  area: ${b.area}→${a.area}  ppm: ${b.ppm}k→${a.ppm}k  disc: ${b.disc}→${a.disc}%  roi: ${b.roi}→${a.roi}%  score: ${b.score}→${a.score}  (${a.src})`);
}

if (dryRun) {
  console.log('\n[--dry] no write.');
  process.exit(0);
}

const output = { ...json, updatedAt: new Date().toISOString() };
const { safeWriteProperties } = require('../lib-safe-write');
safeWriteProperties(FILE, output);
console.log(`\n✅ Wrote ${props.length} props (${touched} rooms re-scored).`);
