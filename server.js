// AIvest.ru — Backend Server
// Node.js + Express MVP
// ─────────────────────────────────────────────────────

require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const nodemailer = require('nodemailer');
const path       = require('path');
const fs         = require('fs');
const crypto     = require('crypto');
const axios      = require('axios');
const { pingIndexNow, coreUrls } = require('./indexnow');

// Canonical site origin. .trim() defends against env-var-with-trailing-newline
// (legacy echo-piped values would inject "\n" into URLs), and the trailing-slash
// strip keeps URL concatenation clean: `${SITE}/path` instead of `${SITE}//path`.
const SITE_URL = (process.env.SITE_URL || 'https://aivest.ru').trim().replace(/\/$/, '');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
// Capture raw body for webhook signature verification
app.use(express.json({
  verify: (req, _res, buf) => { req.rawBody = buf; }
}));
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res, filePath) => {
    const f = path.basename(filePath).toLowerCase();
    // Immutable, 1-year cache for hashed/stable static assets (icons, OG image, CSS, manifest).
    if (
      /^favicon\.(?:ico|svg)$/.test(f) ||
      /^favicon-\d+\.png$/.test(f) ||
      /^apple-touch-icon\.png$/.test(f) ||
      /^icon-\d+\.png$/.test(f) ||
      /^og-image\.png$/.test(f) ||
      /^site\.webmanifest$/.test(f) ||
      /^theme-claude\.css$/.test(f)
    ) {
      res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
      return;
    }
    // Short cache for robots/sitemap (regenerated periodically).
    if (/^robots\.txt$/.test(f) || /^sitemap\.xml$/.test(f)) {
      res.setHeader('Cache-Control', 'public, max-age=3600');
      return;
    }
    // Default: short cache for HTML (index/moscow/spb/sochi).
    if (/\.html?$/.test(f)) {
      res.setHeader('Cache-Control', 'public, max-age=300, must-revalidate');
    }
  }
}));

// ── Password hashing (built-in crypto scrypt) ──
function hashPassword(plain) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(String(plain), salt, 64).toString('hex');
  return `scrypt$${salt}$${hash}`;
}
function verifyPassword(plain, stored) {
  if (!stored) return false;
  if (!stored.startsWith('scrypt$')) return stored === plain; // legacy plaintext
  const [, salt, hash] = stored.split('$');
  const test = crypto.scryptSync(String(plain), salt, 64).toString('hex');
  try { return crypto.timingSafeEqual(Buffer.from(hash,'hex'), Buffer.from(test,'hex')); }
  catch { return false; }
}
function genToken(email, role) {
  return Buffer.from(`${email}:${role}:${Date.now()}:${process.env.ADMIN_KEY || 'aivest-key'}`).toString('base64');
}
function normalizeEmail(e) { return String(e || '').trim().toLowerCase(); }
function validUsername(u) { return /^[a-z0-9_]{3,24}$/i.test(String(u || '')); }

// ── Properties data ──
const PROPERTIES_FILE      = path.join(__dirname, 'data', 'properties.json');
const PROPERTIES_RENT_FILE = path.join(__dirname, 'data', 'properties-rent.json');
function loadProperties(mode) {
  const file = mode === 'rent' ? PROPERTIES_RENT_FILE : PROPERTIES_FILE;
  if (!fs.existsSync(file)) return null;
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); }
  catch (e) { return null; }
}

// ═══════════════════════════════════════════════════════
//  SUBSCRIBER STORE — Vercel KV (primary) + file fallback
//
//  Vercel KV env vars are injected automatically when you
//  link a KV store to the project in the Vercel dashboard:
//    KV_URL, KV_REST_API_URL, KV_REST_API_TOKEN, KV_REST_API_READ_ONLY_TOKEN
//
//  Data layout:
//    Key  "subscribers"  → JSON array of subscriber objects
// ═══════════════════════════════════════════════════════

// Upstash Redis client (lazy init so server starts even without env vars)
let _redis = null;
function getRedis() {
  if (_redis) return _redis;
  // Support both Upstash native vars and Vercel-injected KV_ vars
  const url   = process.env.UPSTASH_REDIS_REST_URL  || process.env.KV_REST_API_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
  if (!url || !token) return null;
  try {
    const { Redis } = require('@upstash/redis');
    _redis = new Redis({ url, token });
    return _redis;
  } catch (e) {
    console.error('Redis init error:', e.message);
    return null;
  }
}

function isKvConfigured() {
  return !!(
    (process.env.UPSTASH_REDIS_REST_URL  || process.env.KV_REST_API_URL) &&
    (process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN)
  );
}

async function loadSubscribers() {
  const redis = getRedis();
  if (redis) {
    try {
      const raw = await redis.get('subscribers');
      if (!raw) return [];
      let parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
      // Defensive: Upstash may return object/double-encoded string; always end with an array
      if (typeof parsed === 'string') { try { parsed = JSON.parse(parsed); } catch {} }
      if (Array.isArray(parsed)) return parsed;
      if (parsed && typeof parsed === 'object') return Object.values(parsed);
      return [];
    } catch (e) {
      console.error('Redis loadSubscribers error:', e.message);
    }
  }
  return loadSubscribersFile();
}

async function saveSubscribers(list) {
  const redis = getRedis();
  if (redis) {
    try {
      await redis.set('subscribers', JSON.stringify(list));
      return;
    } catch (e) {
      console.error('Redis saveSubscribers error:', e.message);
    }
  }
  saveSubscribersFile(list);
}

// ── Local file fallback ──
const SUBSCRIBERS_FILE = process.env.VERCEL
  ? '/tmp/subscribers.json'
  : path.join(__dirname, 'subscribers.json');

function loadSubscribersFile() {
  try {
    if (!fs.existsSync(SUBSCRIBERS_FILE)) return [];
    return JSON.parse(fs.readFileSync(SUBSCRIBERS_FILE, 'utf8'));
  } catch { return []; }
}
function saveSubscribersFile(list) {
  try { fs.writeFileSync(SUBSCRIBERS_FILE, JSON.stringify(list, null, 2)); }
  catch (e) { console.error('saveSubscribersFile error:', e.message); }
}

// ── REQUESTS STORE (support & suggestions) ──
const REQUESTS_FILE = process.env.VERCEL
  ? '/tmp/requests.json'
  : path.join(__dirname, 'requests.json');

async function loadRequests() {
  const redis = getRedis();
  if (redis) {
    try {
      const raw = await redis.get('requests');
      if (!raw) return [];
      let parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
      if (typeof parsed === 'string') { try { parsed = JSON.parse(parsed); } catch {} }
      if (Array.isArray(parsed)) return parsed;
      if (parsed && typeof parsed === 'object') return Object.values(parsed);
      return [];
    } catch (e) { console.error('Redis loadRequests error:', e.message); }
  }
  try {
    if (!fs.existsSync(REQUESTS_FILE)) return [];
    return JSON.parse(fs.readFileSync(REQUESTS_FILE, 'utf8'));
  } catch { return []; }
}
async function saveRequests(list) {
  const redis = getRedis();
  if (redis) {
    try { await redis.set('requests', JSON.stringify(list)); }
    catch (e) { console.error('Redis saveRequests error:', e.message); }
  }
  try { fs.writeFileSync(REQUESTS_FILE, JSON.stringify(list, null, 2)); }
  catch (e) { console.error('saveRequestsFile error:', e.message); }
}

// ── Email transporter ──
const transporter = nodemailer.createTransport({
  host:   process.env.SMTP_HOST || 'smtp.yandex.ru',
  port:   parseInt(process.env.SMTP_PORT || '465'),
  secure: true,
  auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
});

// ═══════════════════════════════════════════════════════
//  ROUTES
// ═══════════════════════════════════════════════════════

// Health check
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    time: new Date().toISOString(),
    kv: isKvConfigured(),
  });
});

// Debug: inspect filesystem for data/properties.json
app.get('/api/debug/fs', (req, res) => {
  const out = {
    __dirname,
    propertiesFile: PROPERTIES_FILE,
    exists: fs.existsSync(PROPERTIES_FILE),
    cwd: process.cwd(),
  };
  try { out.size = fs.statSync(PROPERTIES_FILE).size; } catch (e) { out.statError = e.message; }
  try { out.cwdList = fs.readdirSync(process.cwd()).slice(0, 40); } catch (e) { out.cwdListError = e.message; }
  try { out.dirList = fs.readdirSync(__dirname).slice(0, 40); } catch (e) { out.dirListError = e.message; }
  try { out.dataList = fs.readdirSync(path.join(__dirname, 'data')); } catch (e) { out.dataListError = e.message; }
  res.json(out);
});

// POST /api/login
app.post('/api/login', async (req, res) => {
  const { email, password } = req.body;
  if (!email) return res.status(400).json({ error: 'Email обязателен' });

  const adminEmail = process.env.ADMIN_EMAIL || 'vl.an.samoylenko@gmail.com';
  const adminPass  = process.env.ADMIN_PASS  || 'adminpass';
  if (email.toLowerCase() === adminEmail.toLowerCase() && password === adminPass) {
    const token = Buffer.from(`${email}:admin:${Date.now()}:${process.env.ADMIN_KEY || 'aivest-key'}`).toString('base64');
    return res.json({ success: true, token, role: 'admin', email });
  }

  const rawSubs = await loadSubscribers();
  const subscribers = Array.isArray(rawSubs) ? rawSubs.filter(s => s && typeof s.email === 'string') : [];
  const sub = subscribers.find(s => s.email.toLowerCase() === email.toLowerCase() && s.status === 'active');
  if (sub) {
    // If subscriber has a password set, verify it; otherwise allow login by email only
    if (sub.password && sub.password !== password) {
      return res.status(401).json({ error: 'Неверный email или пароль' });
    }
    const token = Buffer.from(`${email}:pro:${Date.now()}:${process.env.ADMIN_KEY || 'aivest-key'}`).toString('base64');
    return res.json({ success: true, token, role: 'pro', email, activatedAt: sub.activatedAt, plan: sub.plan });
  }

  res.status(401).json({ error: 'Неверный email или пароль' });
});

// GET /api/properties  (?mode=rent for rental listings)
app.get('/api/properties', (req, res) => {
  const mode = req.query.mode === 'rent' ? 'rent' : 'buy';
  const data = loadProperties(mode);
  if (!data) return res.json({ source: 'mock', mode, properties: [], updatedAt: null });

  // Apply −5 penalty for private houses (lower liquidity, longer time-on-market,
  // harder short-term rental). This is applied on-the-fly so existing scraped
  // scores are corrected without a re-scrape.
  let props = (data.properties || []).map(p => {
    if (p.type === 'house' && !p._housePenaltyApplied) {
      return Object.assign({}, p, {
        score: Math.max(0, Math.round((p.score || 0) - 5)),
        _housePenaltyApplied: true,
      });
    }
    return p;
  });
  const { city, type, minScore, maxPrice, source, sort, page = 1, limit = 10000 } = req.query;
  if (city && city !== 'all')     props = props.filter(p => p.city === city);
  if (type && type !== 'all')     props = props.filter(p => p.type === type);
  if (source && source !== 'all') props = props.filter(p => p.source === source);
  if (minScore)                   props = props.filter(p => p.score >= parseInt(minScore));
  if (maxPrice)                   props = props.filter(p => p.price <= parseFloat(maxPrice));

  const sortFns = {
    score:      (a, b) => b.score - a.score,
    roi:        (a, b) => b.roi - a.roi,
    discount:   (a, b) => b.disc - a.disc,
    growth:     (a, b) => b.grow - a.grow,
    price_asc:  (a, b) => a.price - b.price,
    price_desc: (a, b) => b.price - a.price,
  };
  if (sortFns[sort]) props.sort(sortFns[sort]);

  const pageNum   = parseInt(page);
  const limitNum  = Math.min(parseInt(limit), 25000);
  const total     = props.length;
  const paginated = props.slice((pageNum - 1) * limitNum, pageNum * limitNum);

  res.json({ source: 'live', mode, updatedAt: data.updatedAt, total, page: pageNum, limit: limitNum, properties: paginated });
});

// GET /api/properties/stats
app.get('/api/properties/stats', (req, res) => {
  const data = loadProperties();
  if (!data) return res.json({});
  // Apply house −5 penalty on-the-fly (same as /api/properties)
  const props = (data.properties || []).map(p =>
    p.type === 'house' ? Object.assign({}, p, { score: Math.max(0, Math.round((p.score || 0) - 5)) }) : p
  );
  res.json({
    total:     props.length,
    pro:       props.filter(p => p.score >= 60).length,
    avgScore:  Math.round(props.reduce((s,p) => s + p.score, 0) / (props.length || 1)),
    avgRoi:    +(props.reduce((s,p) => s + p.roi, 0) / (props.length || 1)).toFixed(1),
    topScore:  Math.max(...props.map(p => p.score || 0), 0),
    updatedAt: data.updatedAt,
    cities:    data.cities || [],
  });
});

// POST /api/subscribe
app.post('/api/subscribe', async (req, res) => {
  try {
    const { email, plan, name, telegram } = req.body;
    if (!email || !email.includes('@')) return res.status(400).json({ error: 'Некорректный email' });

    const planLabel = plan === 'year'
      ? 'Годовая подписка — 4 680 ₽/год (390 ₽/мес)'
      : 'Месячная подписка — 660 ₽/мес';

    // Save to KV (or file fallback)
    const subscribers = await loadSubscribers();
    const existing = subscribers.find(s => s.email.toLowerCase() === email.toLowerCase());
    if (!existing) {
      subscribers.push({ email, name: name || '', telegram: telegram || '', plan, createdAt: new Date().toISOString(), status: 'pending' });
      await saveSubscribers(subscribers);
    }

    // Email to admin
    try {
      await transporter.sendMail({
        from:    process.env.SMTP_USER,
        to:      process.env.ADMIN_EMAIL || process.env.SMTP_USER,
        subject: `[AIvest] Новая заявка: ${email}`,
        html: `
          <h2>Новая заявка на подписку PRO</h2>
          <p><strong>Email:</strong> ${email}</p>
          ${name ? `<p><strong>Имя:</strong> ${name}</p>` : ''}
          ${telegram ? `<p><strong>Telegram:</strong> ${telegram}</p>` : ''}
          <p><strong>Тариф:</strong> ${planLabel}</p>
          <p><strong>Дата:</strong> ${new Date().toLocaleString('ru-RU')}</p>
        `
      });
    } catch (err) { console.error('Email error (admin):', err.message); }

    // Confirmation to user
    try {
      await transporter.sendMail({
        from:    `AIvest.ru <${process.env.SMTP_USER}>`,
        to:      email,
        subject: 'Заявка на подписку AIvest PRO принята',
        html: `
          <div style="font-family:sans-serif;max-width:520px;margin:0 auto;color:#1a1a1a">
            <h2 style="color:#3a7a00">Спасибо за заявку!</h2>
            <p>Мы получили вашу заявку на <strong>AIvest PRO</strong>.</p>
            <p><strong>Тариф:</strong> ${planLabel}</p>
            <p>Мы свяжемся с вами по email с инструкцией по оплате и активации доступа.</p>
            <hr style="border:none;border-top:1px solid #eee;margin:20px 0">
            <p style="font-size:12px;color:#888">AIvest.ru</p>
          </div>
        `
      });
    } catch (err) { console.error('Email error (user):', err.message); }

    res.json({ success: true, message: 'Заявка принята' });
  } catch (err) {
    console.error('Subscribe handler error:', err.message);
    if (!res.headersSent) res.json({ success: true, message: 'Заявка принята' });
  }
});

// GET /api/admin/subscribers
app.get('/api/admin/subscribers', async (req, res) => {
  const key   = req.query.key || req.headers['x-admin-key'];
  const token = req.query.token || req.headers['x-admin-token'];

  let authorized = (key && key === process.env.ADMIN_KEY);
  if (!authorized && token) {
    try {
      const decoded = Buffer.from(token, 'base64').toString('utf8');
      const [, role, , k] = decoded.split(':');
      authorized = role === 'admin' && k === (process.env.ADMIN_KEY || 'aivest-key');
    } catch {}
  }
  // Back-compat: some old links put token in ?key=
  if (!authorized && req.query.key) {
    try {
      const decoded = Buffer.from(req.query.key, 'base64').toString('utf8');
      const [, role, , k] = decoded.split(':');
      authorized = role === 'admin' && k === (process.env.ADMIN_KEY || 'aivest-key');
    } catch {}
  }

  if (!authorized) return res.status(403).send(`
    <html><body style="font-family:sans-serif;padding:2rem;background:#0b0c0a;color:#ede9df">
      <h2 style="color:#e35d5d">403 — Нет доступа</h2>
      <p>Войдите в аккаунт администратора.</p>
    </body></html>
  `);

  const subs = await loadSubscribers();

  if (req.headers.accept?.includes('text/html') || req.query.format !== 'json') {
    const rows = subs.map(s => `
      <tr>
        <td>${s.name || '—'}</td>
        <td>${s.email}</td>
        <td>${s.telegram || '—'}</td>
        <td>${s.plan === 'year' ? 'Годовая' : 'Месячная'}</td>
        <td><span style="color:${s.status==='active'?'#5ecb7e':'#e4ab3c'}">${s.status}</span></td>
        <td>${new Date(s.createdAt).toLocaleString('ru-RU')}</td>
        <td>${s.activatedAt ? new Date(s.activatedAt).toLocaleString('ru-RU') : '—'}</td>
      </tr>`).join('');
    return res.send(`
      <html><head><title>AIvest — Подписчики</title>
      <style>
        body{font-family:'Segoe UI',sans-serif;background:#0b0c0a;color:#ede9df;padding:2rem}
        h2{color:#c9f151;margin-bottom:.5rem}
        .badge{display:inline-block;font-size:11px;padding:3px 10px;border-radius:99px;margin-bottom:1.5rem;
               background:${isKvConfigured()?'rgba(94,203,126,.15)':'rgba(228,171,60,.15)'};
               color:${isKvConfigured()?'#5ecb7e':'#e4ab3c'};border:1px solid currentColor}
        table{width:100%;border-collapse:collapse}
        th{text-align:left;padding:10px 14px;border-bottom:1px solid #333;color:#7d7b6e;font-size:12px;text-transform:uppercase;letter-spacing:.08em}
        td{padding:10px 14px;border-bottom:1px solid #1a1c14;font-size:14px}
        tr:hover td{background:#141510}
        .count{color:#7d7b6e;font-size:13px;margin-bottom:1rem}
      </style></head>
      <body>
        <h2>AIvest · Подписчики</h2>
        <div class="badge">${isKvConfigured() ? '✅ Upstash Redis' : '⚠ Временное хранилище — настройте Upstash'}</div>
        <p class="count">Всего: ${subs.length} · Активных: ${subs.filter(s=>s.status==='active').length}</p>
        <table>
          <thead><tr><th>Имя</th><th>Email</th><th>Telegram</th><th>Тариф</th><th>Статус</th><th>Дата заявки</th><th>Активирован</th></tr></thead>
          <tbody>${rows || '<tr><td colspan="7" style="color:#4a4840;text-align:center;padding:2rem">Нет подписчиков</td></tr>'}</tbody>
        </table>
      </body></html>
    `);
  }
  res.json(subs);
});

// POST /api/admin/activate
app.post('/api/admin/activate', async (req, res) => {
  const { email, key } = req.body;
  if (key !== process.env.ADMIN_KEY) return res.status(403).json({ error: 'Forbidden' });

  const subscribers = await loadSubscribers();
  const sub = subscribers.find(s => s.email.toLowerCase() === email.toLowerCase());
  if (!sub) return res.status(404).json({ error: 'Не найден' });

  sub.status      = 'active';
  sub.activatedAt = new Date().toISOString();
  await saveSubscribers(subscribers);

  const token = Buffer.from(`${email}:${Date.now()}:${process.env.ADMIN_KEY}`).toString('base64');

  try {
    await transporter.sendMail({
      from:    `AIvest.ru <${process.env.SMTP_USER}>`,
      to:      email,
      subject: '✅ Доступ AIvest PRO активирован',
      html: `
        <div style="font-family:sans-serif;max-width:520px;margin:0 auto;color:#1a1a1a">
          <h2 style="color:#3a7a00">Доступ активирован!</h2>
          <p>Ваша подписка <strong>AIvest PRO</strong> активирована.</p>
          <p style="text-align:center;margin:24px 0">
            <a href="${SITE_URL}?token=${token}"
               style="background:#7ea800;color:#fff;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:bold">
              Открыть AIvest PRO →
            </a>
          </p>
          <hr style="border:none;border-top:1px solid #eee;margin:20px 0">
          <p style="font-size:12px;color:#888">AIvest.ru</p>
        </div>
      `
    });
  } catch (err) { console.error('Activation email error:', err.message); }

  res.json({ success: true, token });
});

// POST /api/verify-token
app.post('/api/verify-token', async (req, res) => {
  const { token } = req.body;
  if (!token) return res.json({ valid: false });
  try {
    const decoded = Buffer.from(token, 'base64').toString('utf8');
    const [email, ts, key] = decoded.split(':');
    if (key !== process.env.ADMIN_KEY) return res.json({ valid: false });
    const subscribers = await loadSubscribers();
    const sub = subscribers.find(s => s.email === email && s.status === 'active');
    return res.json({ valid: !!sub, email: sub ? sub.email : null });
  } catch {
    return res.json({ valid: false });
  }
});

// ═══════════════════════════════════════════════════════
//  PAYMENT CHECKOUT
//  Provider-agnostic. Resolves provider from env / body:
//    - yookassa  (default for RUB)  — YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY
//    - stripe    (USD / intl)       — STRIPE_SECRET_KEY
//    - manual    (no provider set)  — creates pending subscriber + emails admin
//
//  Plan prices (₽/month, yearly billed up-front):
// ═══════════════════════════════════════════════════════
// 1 Star ≈ $0.013. Rounded to friendly numbers.
// Stars ≈ rub / 1.7  (user-facing parity; Telegram sells ~100⭐ ≈ $1.99)
const PLAN_PRICES = {
  month:    { rub: 660,   usd: 7,   stars: 400,  label: 'Месяц · AIvest PRO'      },
  year:     { rub: 5880,  usd: 65,  stars: 3500, label: 'Год · AIvest PRO (−26%)' },
  lifetime: { rub: 14900, usd: 165, stars: 9000, label: 'Бессрочная · AIvest PRO' },
};

const PROVIDERS = ['yookassa','stripe','telegram','manual'];
function pickProvider(requested) {
  if (requested && PROVIDERS.includes(requested)) return requested;
  if (process.env.YOOKASSA_SHOP_ID && process.env.YOOKASSA_SECRET_KEY) return 'yookassa';
  if (process.env.STRIPE_SECRET_KEY) return 'stripe';
  if (process.env.TELEGRAM_BOT_TOKEN) return 'telegram';
  return 'manual';
}

// POST /api/checkout
// body: { email, password, username, name?, telegram?, plan:'month'|'year', currency?:'rub'|'usd', provider? }
// returns: { success, redirectUrl?, provider, orderId }
app.post('/api/checkout', async (req, res) => {
  try {
    const { email: rawEmail, password, username, name, telegram, plan, currency, provider: reqProvider } = req.body || {};
    const email = normalizeEmail(rawEmail);

    if (!email || !email.includes('@'))  return res.status(400).json({ error: 'Укажите корректный email' });
    if (!password || password.length < 6) return res.status(400).json({ error: 'Пароль — минимум 6 символов' });
    if (!validUsername(username))          return res.status(400).json({ error: 'Имя пользователя: 3–24 символа (буквы, цифры, _)' });
    if (!PLAN_PRICES[plan])                return res.status(400).json({ error: 'Неверный тариф' });

    const rawList = await loadSubscribers();
    // Normalize: drop malformed legacy rows without email
    const subscribers = Array.isArray(rawList) ? rawList.filter(s => s && typeof s.email === 'string' && s.email.includes('@')) : [];
    const existing    = subscribers.find(s => s.email.toLowerCase() === email);
    if (existing && existing.status === 'active') {
      return res.status(409).json({ error: 'Аккаунт с таким email уже активен. Войдите.' });
    }
    // Username uniqueness
    const userTaken = subscribers.find(s =>
      (s.username || '').toLowerCase() === String(username).toLowerCase() &&
      s.email.toLowerCase() !== email
    );
    if (userTaken) return res.status(409).json({ error: 'Это имя пользователя занято' });

    const cur   = (currency === 'usd') ? 'usd' : 'rub';
    const price = PLAN_PRICES[plan][cur];
    const orderId = crypto.randomBytes(8).toString('hex');

    const record = {
      email,
      username,
      name:     name || '',
      telegram: telegram || '',
      plan,
      password: hashPassword(password),
      createdAt: existing ? existing.createdAt : new Date().toISOString(),
      status:   'pending',
      orderId,
      provider: pickProvider(reqProvider),
      currency: cur,
      price,
    };
    if (existing) Object.assign(existing, record);
    else          subscribers.push(record);
    await saveSubscribers(subscribers);

    const provider = record.provider;
    const returnUrl = `${SITE_URL}/?paid=1&order=${orderId}`;

    // ── YooKassa ──
    if (provider === 'yookassa') {
      try {
        const idempotencyKey = crypto.randomBytes(12).toString('hex');
        // Trim whitespace/newlines from env vars — vercel CLI or UI paste can introduce them.
        const ykShopId = (process.env.YOOKASSA_SHOP_ID || '').trim();
        const ykSecret = (process.env.YOOKASSA_SECRET_KEY || '').trim();
        const auth = Buffer.from(`${ykShopId}:${ykSecret}`).toString('base64');
        const resp = await axios.post('https://api.yookassa.ru/v3/payments', {
          amount:      { value: price.toFixed(2), currency: cur === 'usd' ? 'USD' : 'RUB' },
          capture:     true,
          confirmation:{ type: 'redirect', return_url: returnUrl },
          description: `${PLAN_PRICES[plan].label} · ${email}`,
          metadata:    { email, orderId, plan },
          receipt:     {
            customer: { email },
            // 54-ФЗ: система налогообложения.
            // 3 = УСН «доходы минус расходы» (указано владельцем магазина).
            tax_system_code: 3,
            items: [{
              description: PLAN_PRICES[plan].label,
              quantity:   '1',
              amount:     { value: price.toFixed(2), currency: cur === 'usd' ? 'USD' : 'RUB' },
              // 1 = без НДС (УСН-плательщики не являются плательщиками НДС)
              vat_code:   1,
              payment_subject: 'service',
              payment_mode:    'full_payment',
            }],
          },
        }, {
          headers: {
            'Authorization':  `Basic ${auth}`,
            'Idempotence-Key': idempotencyKey,
            'Content-Type':    'application/json',
          },
          timeout: 15000,
        });
        const redirectUrl = resp.data?.confirmation?.confirmation_url;
        return res.json({ success: true, provider, orderId, redirectUrl });
      } catch (e) {
        const details = e.response?.data || e.message || 'unknown';
        console.error('YooKassa error:', details);
        // Surface real error to admin-ish clients so we can see it in DevTools.
        // Triggered by either ?debug=1 or header x-debug: 1. Safe: returns status + YooKassa error body.
        const wantDebug = req.query.debug === '1' || req.headers['x-debug'] === '1';
        return res.status(502).json({
          error: 'Платёжный шлюз недоступен. Попробуйте позже.',
          ...(wantDebug ? {
            debug: {
              status:   e.response?.status,
              data:     e.response?.data,
              message:  e.message,
              shopIdSet: !!process.env.YOOKASSA_SHOP_ID,
              secretKeySet: !!process.env.YOOKASSA_SECRET_KEY,
              shopIdLen: (process.env.YOOKASSA_SHOP_ID || '').length,
              secretKeyPrefix: (process.env.YOOKASSA_SECRET_KEY || '').slice(0, 8),
            }
          } : {})
        });
      }
    }

    // ── Stripe ──
    if (provider === 'stripe') {
      try {
        const params = new URLSearchParams();
        params.append('mode', 'payment');
        params.append('success_url', returnUrl);
        params.append('cancel_url', `${SITE_URL}/?paid=0`);
        params.append('customer_email', email);
        params.append('client_reference_id', orderId);
        params.append('line_items[0][price_data][currency]', cur === 'usd' ? 'usd' : 'rub');
        params.append('line_items[0][price_data][product_data][name]', PLAN_PRICES[plan].label);
        params.append('line_items[0][price_data][unit_amount]', String(price * 100));
        params.append('line_items[0][quantity]', '1');
        params.append('metadata[email]', email);
        params.append('metadata[orderId]', orderId);
        params.append('metadata[plan]', plan);

        const resp = await axios.post('https://api.stripe.com/v1/checkout/sessions', params, {
          headers: {
            'Authorization': `Bearer ${process.env.STRIPE_SECRET_KEY}`,
            'Content-Type':  'application/x-www-form-urlencoded',
          },
          timeout: 15000,
        });
        return res.json({ success: true, provider, orderId, redirectUrl: resp.data.url });
      } catch (e) {
        console.error('Stripe error:', e.response?.data || e.message);
        return res.status(502).json({ error: 'Платёжный шлюз недоступен. Попробуйте позже.' });
      }
    }

    // ── Telegram Stars ──
    if (provider === 'telegram') {
      try {
        if (!process.env.TELEGRAM_BOT_TOKEN) throw new Error('TELEGRAM_BOT_TOKEN not set');
        const stars = PLAN_PRICES[plan].stars;
        const payload = `aivest:${orderId}`;
        // createInvoiceLink — currency XTR (Telegram Stars), provider_token MUST be empty
        const resp = await axios.post(
          `https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/createInvoiceLink`,
          {
            title:        PLAN_PRICES[plan].label,
            description:  `AIvest PRO — ${plan === 'year' ? 'годовая' : 'месячная'} подписка`,
            payload:      payload,
            provider_token: '',
            currency:     'XTR',
            prices:       [{ label: PLAN_PRICES[plan].label, amount: stars }],
          },
          { timeout: 15000 },
        );
        const invoiceUrl = resp.data?.result;
        if (!invoiceUrl) throw new Error('Telegram invoice not created');
        record.currency = 'xtr';
        record.price    = stars;
        // Save updated price
        const subs2 = await loadSubscribers();
        const s2 = subs2.find(s => s.orderId === orderId);
        if (s2) { s2.currency = 'xtr'; s2.price = stars; await saveSubscribers(subs2); }
        return res.json({ success: true, provider, orderId, redirectUrl: invoiceUrl });
      } catch (e) {
        console.error('Telegram Stars error:', e.response?.data || e.message);
        return res.status(502).json({ error: 'Не удалось создать счёт Telegram Stars' });
      }
    }

    // ── Manual (no provider configured) ──
    try {
      await transporter.sendMail({
        from:    process.env.SMTP_USER,
        to:      process.env.ADMIN_EMAIL || process.env.SMTP_USER,
        subject: `[AIvest] Новая заявка (manual): ${email}`,
        html:    `<p>Заказ ${orderId} · ${PLAN_PRICES[plan].label} · ${price} ${cur.toUpperCase()}</p>
                  <p>Пользователь: ${username} · ${name || '—'} · ${telegram || '—'}</p>
                  <p>Активируйте вручную: POST /api/admin/activate</p>`,
      });
    } catch (e) { /* noop */ }
    return res.json({ success: true, provider: 'manual', orderId, redirectUrl: null,
      message: 'Заявка принята. Оплата пока обрабатывается вручную — мы пришлём счёт на email в течение дня.' });
  } catch (err) {
    console.error('Checkout error:', err.message);
    res.status(500).json({ error: 'Внутренняя ошибка' });
  }
});

// Activate a subscriber after successful payment (internal helper)
async function activateSubscriberByOrder(orderId) {
  const subscribers = await loadSubscribers();
  const sub = subscribers.find(s => s.orderId === orderId);
  if (!sub) return null;
  if (sub.status !== 'active') {
    sub.status      = 'active';
    sub.activatedAt = new Date().toISOString();
    await saveSubscribers(subscribers);
    // Welcome email
    try {
      const loginUrl = `${SITE_URL}/?login=${encodeURIComponent(sub.email)}`;
      await transporter.sendMail({
        from:    `AIvest.ru <${process.env.SMTP_USER}>`,
        to:      sub.email,
        subject: '✅ Добро пожаловать в AIvest PRO',
        html:    `
          <div style="font-family:sans-serif;max-width:520px;margin:0 auto;color:#1a1a1a">
            <h2 style="color:#3a7a00">Аккаунт создан, доступ активирован</h2>
            <p><strong>Имя пользователя:</strong> ${sub.username}</p>
            <p><strong>Тариф:</strong> ${PLAN_PRICES[sub.plan]?.label || sub.plan}</p>
            <p style="text-align:center;margin:24px 0">
              <a href="${loginUrl}" style="background:#c15f3c;color:#fff;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:bold">
                Войти в AIvest PRO →
              </a>
            </p>
            <p style="color:#666;font-size:13px">Используйте ваш email и пароль, указанные при оформлении.</p>
          </div>`,
      });
    } catch (e) { console.error('Welcome email error:', e.message); }
  }
  return sub;
}

// Allowed YooKassa webhook source IPs (https://yookassa.ru/developers/using-api/webhooks)
const YOOKASSA_IP_PREFIXES = [
  '185.71.76.', '185.71.77.',
  '77.75.153.', '77.75.154.', '77.75.156.',
  '2a02:5180:',
  '127.0.0.1', '::1', // local testing
];
function isYookassaIp(req) {
  // Vercel sets x-forwarded-for; pick the first (client IP).
  const fwd = (req.headers['x-forwarded-for'] || req.headers['x-real-ip'] || req.ip || '').toString();
  const ip  = fwd.split(',')[0].trim();
  if (!ip) return false;
  return YOOKASSA_IP_PREFIXES.some(p => ip.startsWith(p));
}

// POST /api/webhook/yookassa
app.post('/api/webhook/yookassa', async (req, res) => {
  try {
    // IP allowlist: reject requests not from YooKassa's known ranges.
    if (!isYookassaIp(req)) {
      console.warn('YooKassa webhook rejected — untrusted IP:', req.headers['x-forwarded-for'] || req.ip);
      return res.status(403).json({ ok: false, error: 'forbidden' });
    }
    const event = req.body || {};
    const kind  = event.event || '';
    const obj   = event.object || {};
    const orderId = obj.metadata?.orderId;
    if (kind === 'payment.succeeded' && orderId) {
      await activateSubscriberByOrder(orderId);
    } else if (kind === 'payment.canceled' && orderId) {
      // Optionally: flag order as canceled. For now we just log.
      console.log('YooKassa payment canceled for order', orderId);
    }
    // YooKassa expects 200 on success; other codes trigger retries for 24h.
    res.json({ ok: true });
  } catch (e) {
    console.error('YooKassa webhook error:', e.message);
    // Return 200 anyway so YooKassa doesn't retry on our parsing bug.
    res.status(200).json({ ok: false });
  }
});

// POST /api/webhook/telegram — Telegram Bot API updates
// Set this URL via: https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://aivest.ru/api/webhook/telegram
app.post('/api/webhook/telegram', async (req, res) => {
  try {
    const update = req.body || {};

    // Step 1: pre_checkout_query — must answer within 10s to confirm
    if (update.pre_checkout_query) {
      const q = update.pre_checkout_query;
      try {
        await axios.post(
          `https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/answerPreCheckoutQuery`,
          { pre_checkout_query_id: q.id, ok: true },
          { timeout: 10000 },
        );
      } catch (e) { console.error('answerPreCheckoutQuery error:', e.response?.data || e.message); }
      return res.json({ ok: true });
    }

    // Step 2: successful_payment — actually paid
    const sp = update.message?.successful_payment;
    if (sp) {
      const payload = String(sp.invoice_payload || '');
      const m = payload.match(/^aivest:([a-f0-9]+)$/);
      if (m) await activateSubscriberByOrder(m[1]);
    }
    res.json({ ok: true });
  } catch (e) {
    console.error('Telegram webhook error:', e.message);
    res.status(200).json({ ok: false });
  }
});

// POST /api/webhook/stripe
// NB: signature verification left as a TODO (requires `stripe` npm pkg).
app.post('/api/webhook/stripe', async (req, res) => {
  try {
    const event = req.body || {};
    if (event.type === 'checkout.session.completed') {
      const orderId = event.data?.object?.client_reference_id || event.data?.object?.metadata?.orderId;
      if (orderId) await activateSubscriberByOrder(orderId);
    }
    res.json({ received: true });
  } catch (e) {
    console.error('Stripe webhook error:', e.message);
    res.status(200).json({ received: false });
  }
});

// GET /api/checkout/status?orderId=...
app.get('/api/checkout/status', async (req, res) => {
  const { orderId } = req.query;
  if (!orderId) return res.status(400).json({ error: 'orderId required' });
  const subs = await loadSubscribers();
  const sub  = subs.find(s => s.orderId === orderId);
  if (!sub) return res.json({ status: 'unknown' });
  res.json({
    status: sub.status,
    email:  sub.email,
    plan:   sub.plan,
  });
});

// ═══════════════════════════════════════════════════════
//  SUPPORT / SUGGESTIONS
// ═══════════════════════════════════════════════════════

// POST /api/support
// body: { email, name?, type:'support'|'suggestion', message }
app.post('/api/support', async (req, res) => {
  try {
    const email   = normalizeEmail(req.body?.email);
    const name    = String(req.body?.name || '').slice(0, 100);
    const type    = ['support','suggestion'].includes(req.body?.type) ? req.body.type : 'support';
    const message = String(req.body?.message || '').trim().slice(0, 4000);

    if (!email || !email.includes('@')) return res.status(400).json({ error: 'Укажите корректный email' });
    if (!message || message.length < 5) return res.status(400).json({ error: 'Слишком короткое сообщение' });

    const list = await loadRequests();
    const entry = {
      id:        crypto.randomBytes(6).toString('hex'),
      email, name, type, message,
      createdAt: new Date().toISOString(),
      status:    'new',
    };
    list.unshift(entry);
    // Keep last 2000 only
    if (list.length > 2000) list.length = 2000;
    await saveRequests(list);

    // Notify admin by email (best-effort)
    try {
      await transporter.sendMail({
        from:    process.env.SMTP_USER,
        to:      process.env.ADMIN_EMAIL || process.env.SMTP_USER,
        subject: `[AIvest] ${type === 'suggestion' ? 'Предложение' : 'Обращение в поддержку'} от ${email}`,
        html: `
          <h3>${type === 'suggestion' ? '💡 Предложение' : '🛟 Обращение в поддержку'}</h3>
          <p><strong>Email:</strong> ${email}</p>
          ${name ? `<p><strong>Имя:</strong> ${name}</p>` : ''}
          <pre style="white-space:pre-wrap;font-family:inherit;background:#f7f5ee;padding:12px;border-radius:8px">${message.replace(/</g,'&lt;')}</pre>`,
      });
    } catch (e) { /* noop */ }

    res.json({ success: true });
  } catch (err) {
    console.error('Support handler error:', err.message);
    res.status(500).json({ error: 'Внутренняя ошибка' });
  }
});

// Helper: verify admin auth (header or query)
function isAdminAuth(req) {
  const key   = req.query.key || req.headers['x-admin-key'];
  const token = req.query.token || req.headers['x-admin-token'];
  if (key && key === process.env.ADMIN_KEY) return true;
  if (token) {
    try {
      const decoded = Buffer.from(token, 'base64').toString('utf8');
      const [, role, , k] = decoded.split(':');
      return role === 'admin' && k === (process.env.ADMIN_KEY || 'aivest-key');
    } catch {}
  }
  return false;
}

// GET /api/admin/requests  — list support + suggestions
app.get('/api/admin/requests', async (req, res) => {
  if (!isAdminAuth(req)) return res.status(403).json({ error: 'Forbidden' });
  const list = await loadRequests();

  // JSON API
  if (req.query.format === 'json' || req.headers.accept?.includes('application/json')) {
    return res.json({ total: list.length, requests: list });
  }

  // HTML admin page
  const rows = list.map(r => `
    <tr>
      <td>${new Date(r.createdAt).toLocaleString('ru-RU')}</td>
      <td><span class="type-${r.type}">${r.type === 'suggestion' ? '💡 предложение' : '🛟 поддержка'}</span></td>
      <td>${r.name ? r.name.replace(/</g,'&lt;') : '—'}</td>
      <td><a href="mailto:${r.email}">${r.email}</a></td>
      <td><div class="msg">${r.message.replace(/</g,'&lt;')}</div></td>
      <td>${r.status}</td>
    </tr>`).join('');
  res.send(`
    <html><head><title>AIvest — Обращения</title>
    <style>
      body{font-family:'Segoe UI',sans-serif;background:#0b0c0a;color:#ede9df;padding:2rem}
      h2{color:#c9f151;margin-bottom:.5rem}
      .count{color:#7d7b6e;font-size:13px;margin-bottom:1rem}
      table{width:100%;border-collapse:collapse}
      th{text-align:left;padding:10px 14px;border-bottom:1px solid #333;color:#7d7b6e;font-size:12px;text-transform:uppercase;letter-spacing:.08em}
      td{padding:10px 14px;border-bottom:1px solid #1a1c14;font-size:14px;vertical-align:top}
      tr:hover td{background:#141510}
      .msg{max-width:520px;white-space:pre-wrap;color:#c9c6bb}
      .type-suggestion{color:#c9f151}
      .type-support{color:#e4ab3c}
      a{color:#7ea800}
    </style></head>
    <body>
      <h2>AIvest · Обращения и предложения</h2>
      <p class="count">Всего: ${list.length} · Новых: ${list.filter(r=>r.status==='new').length}</p>
      <table>
        <thead><tr><th>Дата</th><th>Тип</th><th>Имя</th><th>Email</th><th>Сообщение</th><th>Статус</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="6" style="color:#4a4840;text-align:center;padding:2rem">Пока обращений нет</td></tr>'}</tbody>
      </table>
    </body></html>`);
});

// ═══════════════════════════════════════════════════════
//  SEO — sitemap.xml (dynamic, lists top listings + city anchors)
// ═══════════════════════════════════════════════════════
app.get('/sitemap.xml', (req, res) => {
  // .trim() guards against env-var-with-trailing-newline (legacy echo-piped values),
  // which would inject "\n" into every <loc> URL and make Yandex reject the sitemap.
  const SITE = SITE_URL;
  const today = new Date().toISOString().slice(0, 10);
  const data = loadProperties();
  const props = (data && data.properties) ? data.properties : [];

  // Build alternates helper
  const xhtmlLinks = (loc) => [
    `<xhtml:link rel="alternate" hreflang="ru"        href="${loc}"/>`,
    `<xhtml:link rel="alternate" hreflang="en"        href="${loc}${loc.includes('?') ? '&' : '?'}lang=en"/>`,
    `<xhtml:link rel="alternate" hreflang="uz"        href="${loc}${loc.includes('?') ? '&' : '?'}lang=uz"/>`,
    `<xhtml:link rel="alternate" hreflang="kk"        href="${loc}${loc.includes('?') ? '&' : '?'}lang=kz"/>`,
    `<xhtml:link rel="alternate" hreflang="x-default" href="${loc}"/>`,
  ].join('');

  // Static URLs + city anchors + geo landing pages
  const urls = [
    { loc: `${SITE}/`,                changefreq: 'hourly',  priority: '1.0', alts: true },
    { loc: `${SITE}/#how-it-works`,   changefreq: 'monthly', priority: '0.7' },
    { loc: `${SITE}/#methodology`,    changefreq: 'monthly', priority: '0.7' },
    { loc: `${SITE}/#pricing`,        changefreq: 'monthly', priority: '0.6' },
    // Geo landing pages (static HTML)
    { loc: `${SITE}/moscow`,          changefreq: 'daily',   priority: '0.9', alts: true },
    { loc: `${SITE}/spb`,             changefreq: 'daily',   priority: '0.9', alts: true },
    { loc: `${SITE}/sochi`,           changefreq: 'daily',   priority: '0.9', alts: true },
  ];
  const cities = ['Москва','Санкт-Петербург','Краснодар','Сочи','Казань','Новосибирск','Екатеринбург'];
  for (const c of cities) {
    urls.push({ loc: `${SITE}/?city=${encodeURIComponent(c)}`, changefreq: 'daily', priority: '0.8' });
  }
  // Top 500 highest-scored listings (anchor links — SPA hash deep-link)
  props.slice(0, 500).forEach(p => {
    urls.push({ loc: `${SITE}/?id=${p.id}#listing-${p.id}`, changefreq: 'weekly', priority: '0.5' });
  });

  res.set('Content-Type', 'application/xml; charset=utf-8');
  res.set('Cache-Control', 'public, max-age=3600');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml">
${urls.map(u => `  <url><loc>${u.loc}</loc><lastmod>${today}</lastmod><changefreq>${u.changefreq}</changefreq><priority>${u.priority}</priority>${u.alts ? xhtmlLinks(u.loc) : ''}</url>`).join('\n')}
</urlset>`);
});

// ── IndexNow: push URLs to Yandex, Bing, Seznam, Naver for near-instant indexing ──
// POST /api/indexnow/ping        (admin) — pings a custom list of URLs (body: {urls: [...]})
// POST /api/indexnow/ping-core   (admin) — pings homepage + geo pages (no body needed)
app.post('/api/indexnow/ping', async (req, res) => {
  if (!isAdminAuth(req)) return res.status(401).json({ error: 'unauthorized' });
  const urls = Array.isArray(req.body && req.body.urls) ? req.body.urls : null;
  if (!urls || !urls.length) return res.status(400).json({ error: 'urls[] required' });
  try {
    const r = await pingIndexNow(urls);
    res.json({ success: true, ...r });
  } catch (e) {
    res.status(500).json({ success: false, error: String(e.message || e) });
  }
});

app.post('/api/indexnow/ping-core', async (req, res) => {
  if (!isAdminAuth(req)) return res.status(401).json({ error: 'unauthorized' });
  try {
    const r = await pingIndexNow(coreUrls());
    res.json({ success: true, ...r });
  } catch (e) {
    res.status(500).json({ success: false, error: String(e.message || e) });
  }
});

// GET /api/cron/indexnow — daily auto-ping of core URLs + top properties.
// Called by Vercel Cron (configured in vercel.json). Vercel sets
// `Authorization: Bearer $CRON_SECRET` header automatically; we check it.
app.get('/api/cron/indexnow', async (req, res) => {
  // Vercel Cron sends the CRON_SECRET env var in the Authorization header.
  const auth = req.headers.authorization || '';
  const expected = `Bearer ${process.env.CRON_SECRET || ''}`;
  if (!process.env.CRON_SECRET || auth !== expected) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  try {
    // Core URLs — homepage, geo pages, key sections
    const urls = coreUrls();

    // Plus top 100 property listings (daily freshness signal for property URLs).
    try {
      const data = await loadProperties();
      const top = (data.properties || [])
        .filter(p => p && p.id && (p.score || 0) >= 60)
        .sort((a, b) => (b.score || 0) - (a.score || 0))
        .slice(0, 100)
        .map(p => `https://aivest.ru/?id=${p.id}#listing-${p.id}`);
      urls.push(...top);
    } catch (e) {
      console.warn('cron indexnow: could not load properties, pinging core only:', e.message);
    }

    const r = await pingIndexNow(urls);
    console.log(`[cron] IndexNow pinged ${urls.length} URLs — status ${r.status}`);
    return res.json({ success: true, pinged: urls.length, ...r });
  } catch (e) {
    console.error('[cron] IndexNow ping failed:', e.message);
    return res.status(500).json({ success: false, error: String(e.message || e) });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
//  Yandex.Market — digital-goods (activation key) delivery
// ═══════════════════════════════════════════════════════════════════════════
const ymLib = require('./lib-yandex-market');
const ym    = new ymLib.YandexMarket();

// Yandex.Market notifications webhook.
//
// Their admin UI rejects URLs with `?query=string` ("Некорректный URL"), so we
// embed the shared secret as a path segment instead:
//     https://aivest.ru/api/ym/notification/<YM_WEBHOOK_SECRET>
//
// Behavior matrix:
//   GET   → 200 OK plain-text "ok"   (URL reachability check before PING)
//   POST  with type=PING → 200 OK    (handshake, no order processing)
//   POST  with orderId   → 200 OK fast, then async order delivery
//
// Both legacy `?s=` query-param form and the new path form are accepted to
// avoid breaking a config that's already saved.
function ymVerifySecret(req) {
  const expected = (process.env.YM_WEBHOOK_SECRET || '').trim();
  if (!expected) return { ok: false, reason: 'no env secret' };
  const fromPath  = (req.params?.secret || '').toString();
  const fromQuery = (req.query?.s || '').toString();
  const fromHdr   = (req.headers['x-ym-secret'] || '').toString();
  if (fromPath  === expected) return { ok: true, source: 'path' };
  if (fromQuery === expected) return { ok: true, source: 'query' };
  if (fromHdr   === expected) return { ok: true, source: 'header' };
  // Helpful diagnostic that NEVER prints the actual secret — just lengths +
  // whether each candidate was non-empty. Lets us see "they sent a 22-char
  // secret in query but our env var is 32 chars" without leaking values.
  return {
    ok: false,
    reason: 'secret mismatch',
    expectedLen: expected.length,
    pathLen:  fromPath.length,
    queryLen: fromQuery.length,
    hdrLen:   fromHdr.length,
  };
}

function handleYmNotification(req, res) {
  const v = ymVerifySecret(req);
  if (!v.ok) {
    console.warn('[ym] notification AUTH FAIL', JSON.stringify(v), 'url=', req.originalUrl);
    return res.status(401).json({ status: 'ERROR', errors: [{ code: 'UNAUTHORIZED', message: 'bad secret' }] });
  }

  // Acknowledge FAST — Yandex retries slow/failed webhooks.
  // YM Partner push-API requires EXACTLY this shape, otherwise the cabinet
  // rejects the URL with INVALID_RESPONSE / INVALID_DATA / CANT_GET_RESPONSE.
  // Source: yandex.ru/dev/market/partner-api/doc/ru/reference/notifications.
  res.status(200).json({ status: 'OK' });

  const body = req.body || {};
  // Yandex.Market ships `notificationType`; older docs / examples use `type`.
  const type = (body.notificationType || body.type || body.eventType || '').toString().toUpperCase();

  // PING / handshake — nothing to process.
  if (type === 'PING') {
    console.log(`[ym] notification PING ok (secret via ${v.source})`);
    return;
  }

  const orderId = body.orderId || body.order?.id || body.order?.orderId;
  if (!orderId) {
    console.log(`[ym] notification type=${type} no orderId — body:`, JSON.stringify(body).slice(0, 400));
    return;
  }

  (async () => {
    try {
      const redis = getRedis();
      if (!redis) return console.warn('YM notification: KV not configured');
      const r = await ymLib.processOrder(orderId, ym, redis);
      console.log(`[ym] notification orderId=${orderId} type=${type} → ${JSON.stringify(r)}`);
    } catch (e) {
      console.error(`[ym] notification orderId=${orderId} failed:`, e.response?.data || e.message);
    }
  })();
}

// Yandex.Market hard-codes a `/notification` suffix that it appends to every
// configured URL. So if user enters         https://aivest.ru/api/ym/<SECRET>
// the actual incoming path will be          /api/ym/<SECRET>/notification
// and if user (mis)enters …/notification/<SECRET>, the path becomes
//                                           /api/ym/notification/<SECRET>/notification
// We register routes for ALL plausible shapes so any reasonable input works:
const ymGetOK = (_req, res) => res.status(200).type('text/plain').send('ok');

// (Removed: /api/ym/_diag was a public diagnostic endpoint used to bootstrap
// the YM webhook integration. Now that secrets are sorted, leaving it public
// only helps attackers fingerprint our env layout. If you need it back during
// debugging, gate it behind isAdminAuth.)

// GET — reachability probes (any of these returns 200).
app.get('/api/ym/notification',                         ymGetOK);
app.get('/api/ym/notification/:secret',                 ymGetOK);
app.get('/api/ym/notification/:secret/notification',    ymGetOK);
app.get('/api/ym/:secret/notification',                 ymGetOK);

// POST — actual notification deliveries (PING, ORDER_*).
app.post('/api/ym/notification',                        handleYmNotification);
app.post('/api/ym/notification/:secret',                handleYmNotification);
app.post('/api/ym/notification/:secret/notification',   handleYmNotification);
app.post('/api/ym/:secret/notification',                handleYmNotification);

// Cron fallback — runs every N minutes (see vercel.json) to catch any
// PROCESSING+DIGITAL orders we missed (webhook lost, network blip, etc.).
//
// Auth ladder (any one passes):
//   1. CRON_SECRET in Authorization Bearer header   — when env var is set
//   2. Vercel's internal cron caller (vercel-cron/* User-Agent)  — Vercel
//      Hobby plan does NOT inject CRON_SECRET, but does pin a UA we can trust
//   3. ADMIN_KEY in x-admin-key header              — manual ops convenience
app.get('/api/cron/ym-deliver', async (req, res) => {
  const auth = req.headers.authorization || '';
  const ua   = String(req.headers['user-agent'] || '');
  const adminKey = req.headers['x-admin-key'] || req.query.key;
  const cronSecretSet = !!process.env.CRON_SECRET;
  const okCronSecret  = cronSecretSet && auth === `Bearer ${process.env.CRON_SECRET}`;
  const okVercelUA    = /vercel-cron/i.test(ua);
  const okAdminKey    = !!process.env.ADMIN_KEY && adminKey === process.env.ADMIN_KEY;
  if (!okCronSecret && !okVercelUA && !okAdminKey) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  try {
    const redis = getRedis();
    if (!redis) return res.status(503).json({ error: 'KV not configured' });
    const r = await ymLib.sweepProcessingDigital(ym, redis);
    console.log(`[cron] YM sweep — ${r.swept} orders processed`);
    return res.json({ success: true, ...r });
  } catch (e) {
    console.error('[cron] YM sweep failed:', e.message);
    return res.status(500).json({ success: false, error: String(e.message || e) });
  }
});

// ── Admin: inventory management ────────────────────────────────────────────
// POST /api/admin/ym/keys  body: { sku: "<offerId>", keys: ["KEY1", "KEY2", ...] }
app.post('/api/admin/ym/keys', async (req, res) => {
  if (!isAdminAuth(req)) return res.status(401).json({ error: 'unauthorized' });
  const redis = getRedis();
  if (!redis) return res.status(503).json({ error: 'KV not configured' });
  const sku  = (req.body?.sku || '').toString().trim();
  const keys = Array.isArray(req.body?.keys) ? req.body.keys : null;
  if (!sku || !keys?.length) return res.status(400).json({ error: 'sku + keys[] required' });
  try {
    const total = await ymLib.addKeys(redis, sku, keys);
    await ymLib.logEvent(redis, { type: 'stock_added', sku, count: keys.length });
    return res.json({ success: true, sku, total_in_stock: total });
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }
});

// GET /api/admin/ym/inventory — stock counts per SKU
app.get('/api/admin/ym/inventory', async (req, res) => {
  if (!isAdminAuth(req)) return res.status(401).json({ error: 'unauthorized' });
  const redis = getRedis();
  if (!redis) return res.status(503).json({ error: 'KV not configured' });
  const inv = await ymLib.inventoryStatus(redis);
  return res.json({ inventory: inv, configured: ym.isConfigured() });
});

// GET /api/admin/ym/log?limit=50 — recent events
app.get('/api/admin/ym/log', async (req, res) => {
  if (!isAdminAuth(req)) return res.status(401).json({ error: 'unauthorized' });
  const redis = getRedis();
  if (!redis) return res.status(503).json({ error: 'KV not configured' });
  const limit = Math.max(1, Math.min(200, parseInt(req.query.limit) || 50));
  const log = await ymLib.recentLog(redis, limit);
  return res.json({ events: log });
});

// POST /api/admin/ym/deliver/:orderId — manual trigger (bypasses webhook), for debugging
app.post('/api/admin/ym/deliver/:orderId', async (req, res) => {
  if (!isAdminAuth(req)) return res.status(401).json({ error: 'unauthorized' });
  const redis = getRedis();
  if (!redis) return res.status(503).json({ error: 'KV not configured' });
  try {
    const r = await ymLib.processOrder(req.params.orderId, ym, redis);
    return res.json({ success: true, ...r });
  } catch (e) {
    return res.status(500).json({ success: false, error: e.response?.data || e.message });
  }
});

// POST /api/admin/ym/sweep — list every PROCESSING+DIGITAL order and try to
// deliver each. Idempotent thanks to ym:delivered:<orderId> guard, so safe to
// hammer. Use when you suspect a webhook was missed (e.g. test orders that
// don't trigger push notifications, or flaky deliveries).
app.post('/api/admin/ym/sweep', async (req, res) => {
  if (!isAdminAuth(req)) return res.status(401).json({ error: 'unauthorized' });
  const redis = getRedis();
  if (!redis) return res.status(503).json({ error: 'KV not configured' });
  try {
    const r = await ymLib.sweepProcessingDigital(ym, redis);
    return res.json({ success: true, ...r });
  } catch (e) {
    return res.status(500).json({ success: false, error: e.response?.data || e.message });
  }
});

// Clean-URL routes for geo landing pages
app.get(['/moscow', '/moscow/'], (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'moscow.html'));
});
app.get(['/spb', '/spb/'], (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'spb.html'));
});
app.get(['/sochi', '/sochi/'], (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'sochi.html'));
});

// IndexNow key file — required for Yandex/Bing IndexNow ping.
// Hosts the key at https://aivest.ru/<INDEXNOW_KEY>.txt so IndexNow can verify ownership.
app.get('/:key.txt', (req, res, next) => {
  const key = process.env.INDEXNOW_KEY;
  if (key && req.params.key === key) {
    res.set('Content-Type', 'text/plain');
    return res.send(key);
  }
  return next();
});

// SPA fallback — only serve index.html for the root path.
// Everything else (garbage/copy-paste URLs, old legacy paths) gets 301 → /
// so Google consolidates on the canonical root and doesn't index junk.
// Legitimate sub-pages (/moscow, /spb, /sochi, /api/*, static files) are
// handled by their specific routes above this one.
app.get('*', (req, res) => {
  if (req.path === '/' || req.path === '') {
    return res.sendFile(path.join(__dirname, 'public', 'index.html'));
  }
  // 301 permanent — tells crawlers to update the URL in their index.
  return res.redirect(301, '/');
});

app.listen(PORT, () => {
  console.log(`\n🏠 AIvest.ru запущен на http://localhost:${PORT}`);
  console.log(`📧 SMTP: ${process.env.SMTP_HOST || 'не настроен'}`);
  console.log(`🔑 Admin key: ${process.env.ADMIN_KEY ? '✓ задан' : '⚠ не задан!'}`);
  console.log(`🗄  Upstash Redis: ${isKvConfigured() ? '✓ подключён' : '⚠ не настроен (файловый fallback)'}\n`);
});
