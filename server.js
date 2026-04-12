// AIvest.ru — Backend Server
// Node.js + Express MVP
// ─────────────────────────────────────────────────────
// npm install express nodemailer dotenv cors googleapis
// node server.js

require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const nodemailer = require('nodemailer');
const path       = require('path');
const fs         = require('fs');
const { google } = require('googleapis');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── Properties data (scraped from Cian) ──
const PROPERTIES_FILE = path.join(__dirname, 'data', 'properties.json');
function loadProperties() {
  if (!fs.existsSync(PROPERTIES_FILE)) return null;
  try { return JSON.parse(fs.readFileSync(PROPERTIES_FILE, 'utf8')); }
  catch (e) { return null; }
}

// ═══════════════════════════════════════════════════════
//  SUBSCRIBER STORE — Google Sheets (primary) + file fallback
//
//  Required env vars:
//    GOOGLE_SHEETS_ID            — spreadsheet ID from the URL
//    GOOGLE_SERVICE_ACCOUNT_EMAIL — service account email
//    GOOGLE_PRIVATE_KEY          — private key (with \n as newlines)
//
//  Sheet columns: A=Email  B=Тариф  C=Дата заявки  D=Статус  E=Активирован
//  Row 1 = headers (created automatically on first write)
// ═══════════════════════════════════════════════════════

const SHEET_ID   = process.env.GOOGLE_SHEETS_ID;
const SHEET_NAME = 'Подписчики';
const HEADER_ROW = ['Email', 'Тариф', 'Дата заявки', 'Статус', 'Активирован'];

function getSheetsClient() {
  if (!SHEET_ID || !process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || !process.env.GOOGLE_PRIVATE_KEY) return null;
  try {
    const auth = new google.auth.JWT(
      process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
      null,
      process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
      ['https://www.googleapis.com/auth/spreadsheets']
    );
    return google.sheets({ version: 'v4', auth });
  } catch (e) {
    console.error('Sheets auth error:', e.message);
    return null;
  }
}

// Ensure header row exists
async function ensureHeader(sheets) {
  try {
    const r = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A1:E1`,
    });
    if (!r.data.values || !r.data.values[0] || r.data.values[0][0] !== 'Email') {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_NAME}!A1:E1`,
        valueInputOption: 'RAW',
        requestBody: { values: [HEADER_ROW] },
      });
    }
  } catch (e) {
    console.error('ensureHeader error:', e.message);
  }
}

async function loadSubscribers() {
  const sheets = getSheetsClient();
  if (sheets) {
    try {
      await ensureHeader(sheets);
      const r = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_NAME}!A2:E`,
      });
      const rows = r.data.values || [];
      return rows
        .filter(row => row[0])
        .map(row => ({
          email:       row[0] || '',
          plan:        row[1] || 'month',
          createdAt:   row[2] || new Date().toISOString(),
          status:      row[3] || 'pending',
          activatedAt: row[4] || null,
        }));
    } catch (e) {
      console.error('Sheets loadSubscribers error:', e.message);
    }
  }
  // Fallback: local file
  return loadSubscribersFile();
}

async function appendSubscriber(sub) {
  const sheets = getSheetsClient();
  if (sheets) {
    try {
      await ensureHeader(sheets);
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_NAME}!A:E`,
        valueInputOption: 'RAW',
        requestBody: {
          values: [[sub.email, sub.plan, sub.createdAt, sub.status, sub.activatedAt || '']],
        },
      });
      console.log('Sheets: appended subscriber', sub.email);
      return;
    } catch (e) {
      console.error('Sheets appendSubscriber error:', e.message);
    }
  }
  // Fallback: local file
  const list = loadSubscribersFile();
  if (!list.find(s => s.email === sub.email)) {
    list.push(sub);
    saveSubscribersFile(list);
  }
}

async function activateSubscriber(email) {
  const activatedAt = new Date().toISOString();
  const sheets = getSheetsClient();
  if (sheets) {
    try {
      const r = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_NAME}!A:A`,
      });
      const rows = r.data.values || [];
      const rowIdx = rows.findIndex(r => r[0]?.toLowerCase() === email.toLowerCase());
      if (rowIdx > 0) { // skip header (index 0)
        const rowNum = rowIdx + 1;
        await sheets.spreadsheets.values.update({
          spreadsheetId: SHEET_ID,
          range: `${SHEET_NAME}!D${rowNum}:E${rowNum}`,
          valueInputOption: 'RAW',
          requestBody: { values: [['active', activatedAt]] },
        });
        console.log('Sheets: activated subscriber', email);
        return activatedAt;
      }
    } catch (e) {
      console.error('Sheets activateSubscriber error:', e.message);
    }
  }
  // Fallback: local file
  const list = loadSubscribersFile();
  const sub = list.find(s => s.email === email);
  if (sub) { sub.status = 'active'; sub.activatedAt = activatedAt; saveSubscribersFile(list); }
  return activatedAt;
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
    sheets: !!(SHEET_ID && process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL),
  });
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
    const token = Buffer.from(`${email}:pro:${Date.now()}:${process.env.ADMIN_KEY || 'aivest-key'}`).toString('base64');
    return res.json({ success: true, token, role: 'pro', email });
  }

  res.status(401).json({ error: 'Неверный email или пароль' });
});

// GET /api/properties
app.get('/api/properties', (req, res) => {
  const data = loadProperties();
  if (!data) return res.json({ source: 'mock', properties: [], updatedAt: null });

  let props = data.properties || [];
  const { city, type, minScore, maxPrice, source, sort, page = 1, limit = 1000 } = req.query;
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
  const limitNum  = Math.min(parseInt(limit), 2000);
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
    const { email, plan } = req.body;
    if (!email || !email.includes('@')) return res.status(400).json({ error: 'Некорректный email' });

    const planLabel = plan === 'year'
      ? 'Годовая подписка — 7 080 ₽/год (590 ₽/мес)'
      : 'Месячная подписка — 990 ₽/мес';

    // Save to Google Sheets (or file fallback)
    const existing = (await loadSubscribers()).find(s => s.email.toLowerCase() === email.toLowerCase());
    if (!existing) {
      await appendSubscriber({ email, plan, createdAt: new Date().toISOString(), status: 'pending' });
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
          <p><strong>Тариф:</strong> ${planLabel}</p>
          <p><strong>Дата:</strong> ${new Date().toLocaleString('ru-RU')}</p>
          <hr>
          <p>Посмотреть все заявки: <a href="${process.env.SITE_URL || 'https://aivest.ru'}/api/admin/subscribers">панель управления</a></p>
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
            <p style="font-size:12px;color:#888">AIvest.ru — Недвижимость для сдачи и долгосрочных инвестиций</p>
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
      <p>Войдите в аккаунт администратора для просмотра подписчиков.</p>
    </body></html>
  `);

  const subs = await loadSubscribers();
  const sheetsConfigured = !!(SHEET_ID && process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL);

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
      <style>
        body{font-family:'Segoe UI',sans-serif;background:#0b0c0a;color:#ede9df;padding:2rem}
        h2{color:#c9f151;margin-bottom:.5rem}
        .storage{font-size:12px;color:${sheetsConfigured?'#5ecb7e':'#e4ab3c'};margin-bottom:1.5rem}
        table{width:100%;border-collapse:collapse}
        th{text-align:left;padding:10px 14px;border-bottom:1px solid #333;color:#7d7b6e;font-size:12px;text-transform:uppercase;letter-spacing:.08em}
        td{padding:10px 14px;border-bottom:1px solid #1a1c14;font-size:14px}
        tr:hover td{background:#141510}
        .count{color:#7d7b6e;font-size:13px;margin-bottom:1rem}
      </style></head>
      <body>
        <h2>AIvest · Подписчики</h2>
        <p class="storage">${sheetsConfigured ? '✅ Google Sheets подключён' : '⚠ Google Sheets не настроен (временное хранилище)'}</p>
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

// POST /api/admin/activate
app.post('/api/admin/activate', async (req, res) => {
  const { email, key } = req.body;
  if (key !== process.env.ADMIN_KEY) return res.status(403).json({ error: 'Forbidden' });

  const subscribers = await loadSubscribers();
  const sub = subscribers.find(s => s.email.toLowerCase() === email.toLowerCase());
  if (!sub) return res.status(404).json({ error: 'Не найден' });

  const activatedAt = await activateSubscriber(email);
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
  console.log(`📊 Google Sheets: ${SHEET_ID ? '✓ ' + SHEET_ID : '⚠ не настроен'}\n`);
});
