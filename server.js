// AIvest.ru — Backend Server
// Node.js + Express MVP
// ─────────────────────────────────────────────────────
// npm install express nodemailer dotenv cors
// node server.js

require('dotenv').config();
const express  = require('express');
const cors     = require('cors');
const nodemailer = require('nodemailer');
const path     = require('path');
const fs       = require('fs');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'))); // frontend goes in /public

// ── Properties data (scraped from Cian) ──
const PROPERTIES_FILE = path.join(__dirname, 'data', 'properties.json');
function loadProperties() {
  if (!fs.existsSync(PROPERTIES_FILE)) return null;
  try { return JSON.parse(fs.readFileSync(PROPERTIES_FILE, 'utf8')); }
  catch (e) { return null; }
}

// POST /api/login — authenticate admin or pro subscriber
app.post('/api/login', (req, res) => {
  const { email, password } = req.body;
  if (!email) return res.status(400).json({ error: 'Email обязателен' });

  // Admin login
  const adminEmail = process.env.ADMIN_EMAIL || 'vl.an.samoylenko@gmail.com';
  const adminPass  = process.env.ADMIN_PASS  || 'adminpass';
  if (email.toLowerCase() === adminEmail.toLowerCase() && password === adminPass) {
    const token = Buffer.from(`${email}:admin:${Date.now()}:${process.env.ADMIN_KEY || 'aivest-key'}`).toString('base64');
    return res.json({ success: true, token, role: 'admin', email });
  }

  // PRO subscriber login (email only, no password needed if activated)
  const subscribers = loadSubscribers();
  const sub = subscribers.find(s => s.email.toLowerCase() === email.toLowerCase() && s.status === 'active');
  if (sub) {
    const token = Buffer.from(`${email}:pro:${Date.now()}:${process.env.ADMIN_KEY || 'aivest-key'}`).toString('base64');
    return res.json({ success: true, token, role: 'pro', email });
  }

  res.status(401).json({ error: 'Неверный email или пароль' });
});

// GET /api/properties — return scraped properties with optional filters
app.get('/api/properties', (req, res) => {
  const data = loadProperties();
  if (!data) return res.json({ source: 'mock', properties: [], updatedAt: null });

  let props = data.properties || [];

  // Filters
  const { city, type, minScore, maxPrice, source, sort, page = 1, limit = 1000 } = req.query;
  if (city && city !== 'all')       props = props.filter(p => p.city === city);
  if (type && type !== 'all')       props = props.filter(p => p.type === type);
  if (source && source !== 'all')   props = props.filter(p => p.source === source);
  if (minScore)                     props = props.filter(p => p.score >= parseInt(minScore));
  if (maxPrice)                     props = props.filter(p => p.price <= parseFloat(maxPrice));

  // Sort
  const sortFns = {
    score:      (a, b) => b.score - a.score,
    roi:        (a, b) => b.roi - a.roi,
    discount:   (a, b) => b.disc - a.disc,
    growth:     (a, b) => b.grow - a.grow,
    price_asc:  (a, b) => a.price - b.price,
    price_desc: (a, b) => b.price - a.price,
  };
  if (sortFns[sort]) props.sort(sortFns[sort]);

  // Pagination
  const pageNum   = parseInt(page);
  const limitNum  = Math.min(parseInt(limit), 2000);
  const total     = props.length;
  const paginated = props.slice((pageNum - 1) * limitNum, pageNum * limitNum);

  res.json({
    source:    'cian',
    updatedAt: data.updatedAt,
    total,
    page:      pageNum,
    limit:     limitNum,
    properties: paginated,
  });
});

// GET /api/properties/stats — aggregated stats
app.get('/api/properties/stats', (req, res) => {
  const data = loadProperties();
  if (!data) return res.json({});
  const props = data.properties || [];
  res.json({
    total:     props.length,
    pro:       props.filter(p => p.score >= 80).length,
    avgScore:  Math.round(props.reduce((s,p) => s + p.score, 0) / (props.length || 1)),
    avgRoi:    +(props.reduce((s,p) => s + p.roi, 0) / (props.length || 1)).toFixed(1),
    topScore:  props[0]?.score || 0,
    updatedAt: data.updatedAt,
    cities:    data.cities || [],
  });
});

// ── Simple file-based subscriber store (swap for DB later) ──
const SUBSCRIBERS_FILE = path.join(__dirname, 'subscribers.json');
function loadSubscribers() {
  if (!fs.existsSync(SUBSCRIBERS_FILE)) return [];
  return JSON.parse(fs.readFileSync(SUBSCRIBERS_FILE, 'utf8'));
}
function saveSubscribers(list) {
  fs.writeFileSync(SUBSCRIBERS_FILE, JSON.stringify(list, null, 2));
}

// ── Email transporter (configure in .env) ──
const transporter = nodemailer.createTransport({
  host:   process.env.SMTP_HOST   || 'smtp.yandex.ru',
  port:   parseInt(process.env.SMTP_PORT || '465'),
  secure: true,
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS,
  }
});

// ── ROUTES ──

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

// Subscribe endpoint
app.post('/api/subscribe', async (req, res) => {
  const { email, plan } = req.body;

  if (!email || !email.includes('@')) {
    return res.status(400).json({ error: 'Некорректный email' });
  }

  const planLabel = plan === 'year'
    ? 'Годовая подписка — 7 080 ₽/год (590 ₽/мес)'
    : 'Месячная подписка — 990 ₽/мес';

  // Save subscriber
  const subscribers = loadSubscribers();
  const existing = subscribers.find(s => s.email === email);
  if (!existing) {
    subscribers.push({ email, plan, createdAt: new Date().toISOString(), status: 'pending' });
    saveSubscribers(subscribers);
  }

  // Email to admin
  try {
    await transporter.sendMail({
      from:    process.env.SMTP_USER,
      to:      process.env.ADMIN_EMAIL || process.env.SMTP_USER,
      subject: `[EstateInvest] Новая заявка на подписку: ${email}`,
      html: `
        <h2>Новая заявка на подписку PRO</h2>
        <p><strong>Email:</strong> ${email}</p>
        <p><strong>Тариф:</strong> ${planLabel}</p>
        <p><strong>Дата:</strong> ${new Date().toLocaleString('ru-RU')}</p>
        <hr>
        <p>Войдите в <a href="${process.env.SITE_URL}/admin">панель управления</a> для подтверждения.</p>
      `
    });
  } catch (err) {
    console.error('Email error (admin):', err.message);
  }

  // Confirmation email to user
  try {
    await transporter.sendMail({
      from:    `AIvest.ru <${process.env.SMTP_USER}>`,
      to:      email,
      subject: 'Заявка на подписку EstateInvest PRO принята',
      html: `
        <div style="font-family:sans-serif;max-width:520px;margin:0 auto;color:#1a1a1a">
          <h2 style="color:#3a7a00">Спасибо за заявку!</h2>
          <p>Мы получили вашу заявку на <strong>EstateInvest PRO</strong>.</p>
          <p><strong>Тариф:</strong> ${planLabel}</p>
          <p>В течение нескольких часов вы получите ссылку на оплату и инструкцию по активации доступа.</p>
          <p>По всем вопросам: <a href="mailto:${process.env.SUPPORT_EMAIL || process.env.SMTP_USER}">${process.env.SUPPORT_EMAIL || process.env.SMTP_USER}</a></p>
          <hr style="border:none;border-top:1px solid #eee;margin:20px 0">
          <p style="font-size:12px;color:#888">AIvest.ru — Недвижимость для сдачи и долгосрочных инвестиций</p>
        </div>
      `
    });
  } catch (err) {
    console.error('Email error (user):', err.message);
  }

  res.json({ success: true, message: 'Заявка принята' });
});

// Admin: list all subscribers — accessible via ADMIN_KEY or admin JWT token
app.get('/api/admin/subscribers', (req, res) => {
  const key   = req.query.key;
  const token = req.query.key || req.headers['x-admin-token'];

  // Allow ADMIN_KEY or a valid admin token
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
      <p>Войдите в аккаунт администратора для просмотра подписчиков.</p>
    </body></html>
  `);

  const subs = loadSubscribers();
  // Return HTML table for easy viewing in browser
  if (req.headers.accept?.includes('text/html') || req.query.format !== 'json') {
    const rows = subs.map(s => `
      <tr>
        <td>${s.email}</td>
        <td>${s.plan === 'year' ? 'Годовая' : 'Месячная'}</td>
        <td><span style="color:${s.status==='active'?'#5ecb7e':'#e4ab3c'}">${s.status}</span></td>
        <td>${new Date(s.createdAt).toLocaleString('ru-RU')}</td>
        <td>${s.activatedAt ? new Date(s.activatedAt).toLocaleString('ru-RU') : '—'}</td>
      </tr>`).join('');
    return res.send(`
      <html><head><title>AIvest — Подписчики</title>
      <style>body{font-family:'Segoe UI',sans-serif;background:#0b0c0a;color:#ede9df;padding:2rem}
      h2{color:#c9f151;margin-bottom:1.5rem}
      table{width:100%;border-collapse:collapse}
      th{text-align:left;padding:10px 14px;border-bottom:1px solid #333;color:#7d7b6e;font-size:12px;text-transform:uppercase;letter-spacing:.08em}
      td{padding:10px 14px;border-bottom:1px solid #1a1c14;font-size:14px}
      tr:hover td{background:#141510}
      .count{color:#7d7b6e;font-size:13px;margin-bottom:1rem}</style></head>
      <body>
        <h2>AIvest · Подписчики</h2>
        <p class="count">Всего: ${subs.length} · Активных: ${subs.filter(s=>s.status==='active').length}</p>
        <table>
          <thead><tr><th>Email</th><th>Тариф</th><th>Статус</th><th>Дата заявки</th><th>Активирован</th></tr></thead>
          <tbody>${rows || '<tr><td colspan="5" style="color:#4a4840;text-align:center;padding:2rem">Нет подписчиков</td></tr>'}</tbody>
        </table>
      </body></html>
    `);
  }
  res.json(subs);
});

// Admin: activate subscriber (mark as paid)
app.post('/api/admin/activate', async (req, res) => {
  const { email, key } = req.body;
  if (key !== process.env.ADMIN_KEY) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  const subscribers = loadSubscribers();
  const sub = subscribers.find(s => s.email === email);
  if (!sub) return res.status(404).json({ error: 'Не найден' });

  sub.status    = 'active';
  sub.activatedAt = new Date().toISOString();
  saveSubscribers(subscribers);

  // Generate simple access token (use JWT in production)
  const token = Buffer.from(`${email}:${Date.now()}:${process.env.ADMIN_KEY}`).toString('base64');

  // Send access email to user
  try {
    await transporter.sendMail({
      from:    `AIvest.ru <${process.env.SMTP_USER}>`,
      to:      email,
      subject: '✅ Доступ EstateInvest PRO активирован',
      html: `
        <div style="font-family:sans-serif;max-width:520px;margin:0 auto;color:#1a1a1a">
          <h2 style="color:#3a7a00">Доступ активирован!</h2>
          <p>Ваша подписка <strong>EstateInvest PRO</strong> активирована.</p>
          <p>Войдите на сайт и введите ваш email чтобы разблокировать все объекты с оценкой 80+:</p>
          <p style="text-align:center;margin:24px 0">
            <a href="${process.env.SITE_URL}?token=${token}" style="background:#7ea800;color:#fff;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:bold">
              Открыть EstateInvest PRO →
            </a>
          </p>
          <hr style="border:none;border-top:1px solid #eee;margin:20px 0">
          <p style="font-size:12px;color:#888">AIvest.ru</p>
        </div>
      `
    });
  } catch (err) {
    console.error('Activation email error:', err.message);
  }

  res.json({ success: true, token });
});

// Verify token (called by frontend to unlock cards)
app.post('/api/verify-token', (req, res) => {
  const { token } = req.body;
  if (!token) return res.json({ valid: false });

  try {
    const decoded = Buffer.from(token, 'base64').toString('utf8');
    const [email, ts, key] = decoded.split(':');
    if (key !== process.env.ADMIN_KEY) return res.json({ valid: false });

    const subscribers = loadSubscribers();
    const sub = subscribers.find(s => s.email === email && s.status === 'active');
    return res.json({ valid: !!sub, email: sub ? sub.email : null });
  } catch {
    return res.json({ valid: false });
  }
});

// Serve frontend for all other routes (SPA fallback)
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`\n🏠 AIvest.ru запущен на http://localhost:${PORT}`);
  console.log(`📧 SMTP: ${process.env.SMTP_HOST || 'не настроен'}`);
  console.log(`🔑 Admin key: ${process.env.ADMIN_KEY ? '✓ задан' : '⚠ не задан!'}\n`);
});
