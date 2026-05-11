// Re-apply calcScore to every record using the current tuned penalties.
// Doesn't touch disc/ppm/roi — only recomputes the final score from the
// already-stored signals + new categoryPenalty + floorPenalty rules.
//
// Use this whenever you tune calcScore weights and want existing records
// to reflect the change without re-scraping.
//
// Tracked under marker `__scoreRevision`. Each tuning bump → new revision.
//
// Usage:
//   node scripts/rebalance-scores.js          # writes
//   node scripts/rebalance-scores.js --dry    # report only

const fs = require('fs');
const path = require('path');

const FILE = path.join(__dirname, '..', 'data', 'properties.json');
const dryRun = process.argv.includes('--dry');

// Bump this when calcScore weights change → records get re-scored on next run.
const SCORE_REVISION = 4; // v4: parking -15 (apartments+commercial mix in top, houses+parking penalized)

function calcScore({ disc, roi, grow, liq, vac, type, floor }) {
  const discScore  = disc > 0 ? 30 * (1 - Math.exp(-disc / 14)) : Math.max(0, 30 + disc * 0.4);
  const roiScore   = roi > 0 ? Math.min(28, 10 * Math.log(1 + roi * 0.8)) : 0;
  const growScore  = Math.min(22, grow * 1.47);
  const liqScore   = liq * 1.2;
  const vacPenalty = vac > 5 ? (vac - 5) * 0.9 : 0;
  const rawSum     = discScore + roiScore + growScore + liqScore - vacPenalty;
  const bonus      = (discScore > 20 && roiScore > 18 && growScore > 14) ? 5 : 0;
  const categoryPenalty = (type === 'commercial') ? 12
                       : (type === 'house')       ? 20
                       : (type === 'parking')     ? 15
                       : 0;
  let floorPenalty = 0;
  if (type === 'commercial' && typeof floor === 'number' && floor < 0) floorPenalty = 12;
  if (type === 'commercial' && typeof floor === 'number' && floor === 0) floorPenalty = 6;
  return Math.min(99, Math.max(0, Math.round(rawSum + bonus - categoryPenalty - floorPenalty)));
}

function parseFloorNum(floorStr) {
  if (floorStr == null || floorStr === '—' || floorStr === '') return null;
  const m = String(floorStr).match(/^(-?\d+)/);
  return m ? parseInt(m[1], 10) : null;
}

const json = JSON.parse(fs.readFileSync(FILE, 'utf8'));
const props = json.properties || [];

let touched = 0, skipped = 0;
const byType = {};   // type → { before: avg, after: avg, max_before, max_after, n }

for (const p of props) {
  if (p.__scoreRevision === SCORE_REVISION) { skipped++; continue; }
  if (typeof p.disc !== 'number' || typeof p.roi !== 'number') { skipped++; continue; }
  if (!p.type) { skipped++; continue; }

  const before = p.score || 0;
  const floor = parseFloorNum(p.floor);
  const newScore = calcScore({
    disc: p.disc, roi: p.roi,
    grow: p.grow || 9.8,
    liq: p.liq || 8,
    vac: p.vac || (p.type === 'commercial' ? 6 : 4),
    type: p.type, floor,
  });

  p.score = newScore;
  p.__scoreRevision = SCORE_REVISION;

  // Re-evaluate "Ниже рынка" badge with the new score baseline
  // (only the visual badge — actual score is what frontend sorts on).
  if (p.badge === 'Ниже рынка' && p.disc < 12) p.badge = undefined;

  const t = byType[p.type] || { before: 0, after: 0, max_before: 0, max_after: 0, n: 0 };
  t.before += before; t.after += newScore;
  t.max_before = Math.max(t.max_before, before);
  t.max_after  = Math.max(t.max_after,  newScore);
  t.n++;
  byType[p.type] = t;
  touched++;
}

console.log(`Touched: ${touched}, skipped: ${skipped}`);
console.log('\n— Score shift by type —');
console.log('type        n      avg_before → avg_after    max_before → max_after');
for (const [t, s] of Object.entries(byType).sort((a,b) => b[1].n - a[1].n)) {
  const ab = (s.before/s.n).toFixed(1);
  const aa = (s.after/s.n).toFixed(1);
  console.log(`${t.padEnd(10)} ${String(s.n).padEnd(6)} ${ab.padStart(5)} → ${aa.padStart(5)}    ${String(s.max_before).padStart(3)} → ${String(s.max_after).padStart(3)}`);
}

if (dryRun) {
  console.log('\n[--dry] no write.');
  process.exit(0);
}

const output = { ...json, updatedAt: new Date().toISOString() };
const { safeWriteProperties } = require('../lib-safe-write');
safeWriteProperties(FILE, output);
console.log(`\n✅ Wrote ${props.length} props (revision ${SCORE_REVISION}).`);
