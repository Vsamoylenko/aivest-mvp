# AIvest.ru — Инструкция по запуску MVP

## Структура проекта

```
aivest/
├── server.js          ← Бэкенд (Node.js + Express)
├── package.json       ← Зависимости
├── .env.example       ← Шаблон переменных окружения
├── .env               ← Ваши настройки (создать вручную!)
├── subscribers.json   ← База подписчиков (создаётся автоматически)
└── public/
    └── index.html     ← Фронтенд (скопируйте сюда index.html)
```

---

## ШАГ 1 — Подготовка на локальной машине

### 1.1 Установите Node.js ≥ 18
Скачайте с https://nodejs.org (LTS версия)

### 1.2 Создайте структуру проекта

```bash
mkdir aivest
cd aivest
mkdir public
# Скопируйте index.html в папку public/
cp /путь/к/index.html public/index.html
```

### 1.3 Установите зависимости

```bash
npm install
```

### 1.4 Создайте файл .env

```bash
cp .env.example .env
```

Откройте `.env` в редакторе и заполните:

```env
PORT=3000
SITE_URL=https://ваш-домен.ru
SMTP_HOST=smtp.yandex.ru
SMTP_PORT=465
SMTP_USER=info@ваш-домен.ru
SMTP_PASS=пароль_приложения_яндекс
ADMIN_EMAIL=admin@ваш-домен.ru
ADMIN_KEY=длинный_случайный_ключ
```

Для Яндекс.Почты пароль приложения создаётся в разделе:
Яндекс ID → Безопасность → Пароли приложений

### 1.5 Запустите локально

```bash
npm start
# или для разработки с автоперезагрузкой:
npm run dev
```

Откройте http://localhost:3000 — сайт должен работать.

---

## ШАГ 2 — Настройка домена

### 2.1 Купите домен
Рекомендуемые регистраторы: REG.RU, NIC.RU, 2domains.ru

### 2.2 Настройте DNS
У регистратора добавьте A-запись:
```
Тип: A
Имя: @
Значение: IP-адрес вашего сервера
TTL: 3600
```
И www:
```
Тип: CNAME
Имя: www
Значение: @
```

---

## ШАГ 3 — Деплой на сервер (VPS)

### 3.1 Арендуйте VPS
Рекомендуемые: Selectel, Timeweb, Beget, DigitalOcean
Минимум: 1 CPU, 1 GB RAM, Ubuntu 22.04 LTS

### 3.2 Подключитесь по SSH

```bash
ssh root@ВАШ_IP
```

### 3.3 Установите Node.js на сервер

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs
node --version  # должно быть v20.x
```

### 3.4 Установите PM2 (менеджер процессов)

```bash
npm install -g pm2
```

### 3.5 Загрузите проект на сервер

**Вариант А — через Git (рекомендуется):**
```bash
# На сервере:
git clone https://github.com/ваш-аккаунт/aivest.git
cd aivest
npm install
cp .env.example .env
nano .env   # заполните настройки
```

**Вариант Б — через FTP/SCP:**
```bash
# С локальной машины:
scp -r ./aivest root@ВАШ_IP:/var/www/aivest
```

### 3.6 Запустите через PM2

```bash
cd /var/www/aivest
pm2 start server.js --name aivest
pm2 save
pm2 startup   # автозапуск при перезагрузке сервера
```

### 3.7 Проверка статуса

```bash
pm2 status
pm2 logs aivest
```

---

## ШАГ 4 — HTTPS (SSL-сертификат) через Nginx + Certbot

### 4.1 Установите Nginx

```bash
sudo apt install nginx -y
```

### 4.2 Создайте конфиг сайта

```bash
sudo nano /etc/nginx/sites-available/aivest
```

Вставьте:
```nginx
server {
    listen 80;
    server_name aivest.ru www.aivest.ru;

    location / {
        proxy_pass http://localhost:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_cache_bypass $http_upgrade;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/aivest /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

### 4.3 Установите SSL-сертификат (бесплатно)

```bash
sudo apt install certbot python3-certbot-nginx -y
sudo certbot --nginx -d aivest.ru -d www.aivest.ru
```

Следуйте инструкциям. Сертификат обновляется автоматически.

---

## ШАГ 5 — Управление подписками

### Просмотр всех заявок:
```
GET https://aivest.ru/api/admin/subscribers?key=ВАШ_ADMIN_KEY
```

### Активация подписчика (после получения оплаты):
```bash
curl -X POST https://aivest.ru/api/admin/activate \
  -H "Content-Type: application/json" \
  -d '{"email":"client@mail.ru","key":"ВАШ_ADMIN_KEY"}'
```

После активации клиент получит email со ссылкой для входа.

---

## ШАГ 6 — Приём оплаты

### Вариант А — Вручную (для старта):
1. Клиент оставляет заявку → вы получаете уведомление на email
2. Отправляете клиенту реквизиты (карта, СБП)
3. После оплаты вызываете `/api/admin/activate`
4. Клиент получает доступ

### Вариант Б — Автоматически (ЮКасса):
Зарегистрируйтесь на https://yookassa.ru и добавьте в server.js:
```bash
npm install @a2seven/yoo-checkout
```
Подробная инструкция: https://yookassa.ru/developers/api

---

## Быстрые команды для работы с сервером

```bash
pm2 logs aivest      # просмотр логов
pm2 restart aivest   # перезапуск после обновления
pm2 stop aivest      # остановка

# Обновление кода:
git pull && pm2 restart aivest
```

---

## Контрольный список перед запуском

- [ ] Node.js установлен (≥ 18)
- [ ] `npm install` выполнен
- [ ] `.env` заполнен (SMTP, ADMIN_KEY)
- [ ] `public/index.html` скопирован
- [ ] `npm start` — сайт открывается локально
- [ ] Домен куплен и DNS настроен
- [ ] VPS арендован, код загружен
- [ ] PM2 запущен и сохранён
- [ ] Nginx настроен и работает
- [ ] SSL-сертификат установлен (HTTPS)
- [ ] Тест: форма подписки — email приходит
- [ ] Тест: активация — клиент получает доступ

---

Вопросы и поддержка: см. README.md или откройте issue в репозитории.
