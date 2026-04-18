// AIvest — Застройщики (developers) scraper
// Pulls listings directly from developer sites: MR Group, Samolet.
// Tags every property with source='dev' so the UI shows them under
// the "Застройщики" pill. Merges into data/properties.json without
// removing other-source entries.
//
// Usage:
//   node scraper-dev.js          → scrape & merge
//   node scraper-dev.js --dry    → scrape & print stats only

const fs       = require('fs');
const path     = require('path');
const cheerio  = require('cheerio');
const { safeWriteProperties } = require('./lib-safe-write');

const DATA_DIR = path.join(__dirname, 'data');
const OUT_FILE = path.join(DATA_DIR, 'properties.json');
const SOURCE   = 'dev';
const DELAY    = parseInt(process.env.DEV_DELAY || '1500', 10);
const UA       = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
                 '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: {
      'User-Agent': UA,
      'Accept': 'text/html,application/xhtml+xml',
      'Accept-Language': 'ru,en;q=0.8',
    }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return await res.text();
}

// ─────────────────────────────────────────────────────────────
// MR Group — mr-group.ru
// Public catalog: https://www.mr-group.ru/objects/
// Structure varies, so we extract conservatively.
// ─────────────────────────────────────────────────────────────
async function scrapeMrGroup() {
  console.log('\n📍 MR Group (mr-group.ru)');
  const items = [];
  try {
    const html = await fetchHtml('https://www.mr-group.ru/objects/');
    const $ = cheerio.load(html);

    // Each project card → one developer "anchor" listing (project-level, not per-flat)
    $('a[href*="/objects/"], a[href*="/project"], .project-card, .objects-list__item').each((_, el) => {
      const $el = $(el);
      const href  = $el.attr('href') || $el.find('a').first().attr('href') || '';
      const name  = ($el.find('h2,h3,.title,.name').first().text() || $el.text() || '').trim().slice(0, 120);
      if (!href || !name || name.length < 3) return;
      const url = href.startsWith('http') ? href : 'https://www.mr-group.ru' + href;
      items.push({
        developer: 'MR Group',
        title:     name,
        url,
      });
    });
  } catch (e) {
    console.log('  ⚠ MR Group fetch failed:', e.message);
  }
  console.log(`  Найдено проектов: ${items.length}`);
  return items;
}

// ─────────────────────────────────────────────────────────────
// Самолёт — samolet.ru
// Public catalog: https://samolet.ru/flats/
// ─────────────────────────────────────────────────────────────
async function scrapeSamolet() {
  console.log('\n📍 Самолёт (samolet.ru)');
  const items = [];
  try {
    const html = await fetchHtml('https://samolet.ru/flats/');
    const $ = cheerio.load(html);

    $('a[href*="/flats/"], a[href*="/projects/"], .flat-card, .project-card').each((_, el) => {
      const $el = $(el);
      const href = $el.attr('href') || '';
      const name = ($el.find('h2,h3,.title,.name').first().text() || $el.text() || '').trim().slice(0, 120);
      if (!href || !name || name.length < 3) return;
      const url = href.startsWith('http') ? href : 'https://samolet.ru' + href;
      items.push({
        developer: 'Самолёт',
        title:     name,
        url,
      });
    });
  } catch (e) {
    console.log('  ⚠ Самолёт fetch failed:', e.message);
  }
  console.log(`  Найдено проектов: ${items.length}`);
  return items;
}

// ─────────────────────────────────────────────────────────────
// Convert raw items to AIvest property objects.
// Developer-level cards have low fidelity (no м², no exact price)
// so we mark them as "anchor" entries with score=0 to hide from
// score-sorted lists by default but still surface under "Застройщики".
// ─────────────────────────────────────────────────────────────
function toDevProperty(it, idx) {
  return {
    id:         900000 + idx, // dev id range
    title:      it.title,
    city:       'Москва',     // both MR Group and Samolet are MSK-region focused
    type:       'apartment',
    typeLabel:  'Новостройка',
    source:     SOURCE,
    developer:  it.developer,
    url:        it.url,
    price:      null,
    area:       null,
    ppm:        null,
    rooms:      null,
    floor:      '',
    score:      0,
    roi:        0,
    grow:       0,
    disc:       0,
    rent:       0,
    badge:      it.developer,
    scrapedAt:  new Date().toISOString(),
  };
}

async function main() {
  const dry = process.argv.includes('--dry');

  const [mr, sm] = await Promise.all([scrapeMrGroup(), scrapeSamolet()]);
  await sleep(DELAY); // courtesy

  const all = [...mr, ...sm];
  // Dedup by URL
  const seen = new Set();
  const unique = all.filter(x => {
    if (seen.has(x.url)) return false;
    seen.add(x.url);
    return true;
  });

  const props = unique.map((it, i) => toDevProperty(it, i));
  console.log(`\n  Итого dev-объектов: ${props.length}`);

  if (dry) {
    console.log('  --dry: запись пропущена');
    return;
  }
  if (props.length === 0) {
    console.log('  ⚠ Нет данных — файл не трогаем (preserved).');
    return;
  }

  // Merge: keep all non-'dev' entries intact
  let existing = { properties: [], updatedAt: new Date().toISOString(), cities: [] };
  if (fs.existsSync(OUT_FILE)) {
    try { existing = JSON.parse(fs.readFileSync(OUT_FILE, 'utf8')); } catch {}
  }
  const others = (existing.properties || []).filter(p => p.source !== SOURCE);
  const merged = [...others, ...props];
  merged.sort((a, b) => (b.score || 0) - (a.score || 0));
  merged.forEach((p, i) => { p.id = i + 1; });

  const output = {
    updatedAt:  new Date().toISOString(),
    total:      merged.length,
    cities:     existing.cities || ['Москва','Санкт-Петербург','Краснодар','Сочи','Казань','Новосибирск','Екатеринбург'],
    properties: merged,
  };

  safeWriteProperties(OUT_FILE, output, { minRatio: 0.9 });
  console.log(`\n✅ Готово! dev=${props.length}, others=${others.length}, total=${merged.length}`);
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
