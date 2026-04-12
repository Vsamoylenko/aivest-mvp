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

// Admin: list all subscribers (protect with a secret key in production)
app.get('/api/admin/subscribers', (req, res) => {
  const key = req.query.key;
  if (key !== process.env.ADMIN_KEY) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  res.json(loadSubscribers());
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
