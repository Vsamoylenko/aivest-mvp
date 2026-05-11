// One-shot cleaner for data/properties.json.
//
// Removes listings that are:
//   • Real-estate "shares" (Доля) — fractional-ownership flats sold under
//     ordinary flatSale category. Type=room is exempted (kommunalka rooms
//     with separate cadastral numbers legitimately say "не доля").
//   • Operating businesses (готовый/арендный/действующий бизнес, ГАБ,
//     "продаём бизнес", arenda+income packaging) — these sell a revenue
//     stream, not real estate; out of scope for AIvest investor cards.
//
// Idempotent — running twice is a no-op.
//
// Usage:
//   node scripts/purge-shares-and-business.js          # writes
//   node scripts/purge-shares-and-business.js --dry    # report only

const fs = require('fs');
const path = require('path');

const FILE = path.join(__dirname, '..', 'data', 'properties.json');
const dryRun = process.argv.includes('--dry');

// Positive share patterns — match "продаётся доля", "1/2 доля", explicit
// "долевая собственность". Bare "доля" gets a negative-lookbehind check to
// skip "не доля!" / "не доли" disclaimers in kommunalka-room ads.
function isShareListing(text, type) {
  if (type === 'room') return false; // room legit even if "не доля" appears
  if (/прода[её]тся\s+доля|продам\s+долю|продается\s+доля/.test(text)) return true;
  if (/\d\s*\/\s*\d\s+(?:дол[яеию]|долей)/.test(text)) return true;
  if (/долевая\s+собственность|общая\s+долев/.test(text)) return true;
  // Bare 'доля' — exclude when preceded by "не" within ~4 chars
  const matches = [...text.matchAll(/\bдол[яеию]х?\b/g)];
  for (const m of matches) {
    const pre = text.slice(Math.max(0, m.index - 6), m.index);
    if (!/не\s*$/.test(pre)) return true;
  }
  return false;
}

// Business patterns. "Арендный бизнес" / "ГАБ" / "готовый бизнес" / "продаём
// бизнес" — Russian real estate jargon for selling an operating revenue
// stream. Underlying asset may technically be real estate, but it's sold as
// a financial product not a property card.
// Patterns that flag a listing as "operating business for sale" rather than
// pure real estate. Order doesn't matter — first match wins.
//
// IMPORTANT: JS regex \b doesn't fire on Cyrillic boundaries (Cyrillic is not
// in [A-Za-z0-9_]), so we use explicit non-letter lookarounds where needed.
const BIZ_RE = new RegExp([
  // "продаю/продаётся [adj] бизнес"
  'прода[ёею]м?(?:тся)?\\s+(?:готовый\\s+|действующий\\s+|арендный\\s+|работающий\\s+)?бизнес',
  'продажа\\s+бизнес',
  'готовый\\s+бизнес',
  'действующий\\s+бизнес',
  'арендный\\s+бизнес',
  'работающий\\s+бизнес',
  'бизнес\\s+под\\s+ключ',                             // "бизнес под ключ"
  // NB: JS \w doesn't match Cyrillic — use explicit [а-яё]+ instead.
  '(?:готов[а-яё]+|действующ[а-яё]+)\\s+(?:салон|кафе|магазин|клуб|студи[яю]|кофейн[а-яё]+|пекарн[а-яё]+|барбершоп|ресторан|бар|пиццери[яю])',
  '(?:салон|кафе|магазин|клуб|кофейн[а-яё]+|пекарн[а-яё]+|барбершоп|ресторан|пиццери[яю])\\s+под\\s+ключ',
  'готовая\\s+аренда',
  'арендный\\s+поток',
  // ГАБ acronym — Cyrillic, so we replace \b with non-letter lookaround
  '(?:^|[^А-Яа-яЁё])ГАБ(?:[^А-Яа-яЁё]|$)',
].join('|'), 'i');

function isBusinessListing(text, raw) {
  if (raw && raw.category === 'businessSale') return true;
  return BIZ_RE.test(text);
}

const json = JSON.parse(fs.readFileSync(FILE, 'utf8'));
const props = json.properties || [];
const keep = [];
const removed = { shares: 0, business: 0, byCity: {} };
const samples = { shares: [], business: [] };

for (const p of props) {
  const text = ((p.title || '') + ' ' + (p.description || '')).toLowerCase();
  // Note: BIZ_RE checks `\bГАБ\b` case-sensitively against original text too.
  const textOrig = (p.title || '') + ' ' + (p.description || '');

  let drop = null;
  if (isShareListing(text, p.type)) drop = 'shares';
  else if (isBusinessListing(text, p) || /\bГАБ\b/.test(textOrig)) drop = 'business';

  if (drop) {
    removed[drop]++;
    removed.byCity[p.city || '?'] = (removed.byCity[p.city || '?'] || 0) + 1;
    if (samples[drop].length < 5) {
      samples[drop].push({ id: p.id, type: p.type, price: p.price, city: p.city, district: p.district, desc: (p.description || '').slice(0, 140), url: p.cianUrl });
    }
  } else {
    keep.push(p);
  }
}

// Re-index ids so the frontend renumbers cleanly.
keep.forEach((p, i) => { p.id = i + 1; });

console.log(`Total before: ${props.length}`);
console.log(`Shares removed:   ${removed.shares}`);
console.log(`Business removed: ${removed.business}`);
console.log(`By city:`, removed.byCity);
console.log(`Total after:  ${keep.length}`);

console.log('\nSample shares:');
for (const s of samples.shares) console.log(' ·', s);
console.log('\nSample business:');
for (const s of samples.business) console.log(' ·', s);

if (dryRun) {
  console.log('\n[--dry] no write performed.');
  process.exit(0);
}

const output = { ...json, properties: keep, totalCount: keep.length, updatedAt: new Date().toISOString() };
const { safeWriteProperties } = require('../lib-safe-write');
safeWriteProperties(FILE, output);
console.log(`\n✅ Wrote ${keep.length} listings (removed ${props.length - keep.length}).`);
