// AIvest.ru — Backend Server
// Node.js + Express MVP
// ─────────────────────────────────────────────────────

require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const nodemailer = require('nodemailer');
const path       = require('path');
const fs         = require('fs');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── Properties data ──
const PROPERTIES_FILE = path.join(__dirname, 'data', 'properties.json');
function loadProperties() {
  if (!fs.existsSync(PROPERTIES_FILE)) return null;
  try { return JSON.parse(fs.readFileSync(PROPERTIES_FILE, 'utf8')); }
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
      return typeof raw === 'string' ? JSON.parse(raw) : raw;
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

  const subscribers = await loadSubscribers();
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

// GET /api/properties
app.get('/api/properties', (req, res) => {
  const data = loadProperties();
  if (!data) return res.json({ source: 'mock', properties: [], updatedAt: null });

  let props = data.properties || [];
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

  res.json({ source: 'cian', updatedAt: data.updatedAt, total, page: pageNum, limit: limitNum, properties: paginated });
});

// GET /api/properties/stats
app.get('/api/properties/stats', (req, res) => {
  const data = loadProperties();
  if (!data) return res.json({});
  const props = data.properties || [];
  res.json({
    total:     props.length,
    pro:       props.filter(p => p.score >= 60).length,
    avgScore:  Math.round(props.reduce((s,p) => s + p.score, 0) / (props.length || 1)),
    avgRoi:    +(props.reduce((s,p) => s + p.roi, 0) / (props.length || 1)).toFixed(1),
    topScore:  props[0]?.score || 0,
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
  const key   = req.query.key;
  const token = req.query.key || req.headers['x-admin-token'];

  let authorized = (key === process.env.ADMIN_KEY);
  if (!authorized && token) {
    try {
      const decoded = Buffer.from(token, 'base64').toString('utf8');
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
            <a href="${process.env.SITE_URL || 'https://aivest.ru'}?token=${token}"
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

// SPA fallback
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`\n🏠 AIvest.ru запущен на http://localhost:${PORT}`);
  console.log(`📧 SMTP: ${process.env.SMTP_HOST || 'не настроен'}`);
  console.log(`🔑 Admin key: ${process.env.ADMIN_KEY ? '✓ задан' : '⚠ не задан!'}`);
  console.log(`🗄  Upstash Redis: ${isKvConfigured() ? '✓ подключён' : '⚠ не настроен (файловый fallback)'}\n`);
});
