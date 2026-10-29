/* ProdU — сервер: аккаунты, синхронизация и push-уведомления (Cloudflare Worker)
 *
 * Что делает:
 *   - регистрация и вход по логину и паролю, хранение данных аккаунта (база D1);
 *   - хранит напоминания каждого устройства;
 *   - каждую минуту (Cron Trigger) отправляет push тем, у кого подошло время;
 *   - ключи VAPID создаёт сам при первом запуске и хранит в базе.
 *
 * Пароль на сервер не передаётся: браузер превращает его в ключ (PBKDF2, 300 000 итераций),
 * а сервер хранит только хэш этого ключа с солью.
 *
 * Необязательные переменные (Settings → Variables):
 *   INVITE_CODE  — если задана, зарегистрироваться можно только с этим кодом;
 *   REGISTRATION — значение closed полностью закрывает регистрацию новых аккаунтов.
 *
 * Настройка (в панели Cloudflare):
 *   1. D1 → создать базу (например, produ-push).
 *   2. Workers → создать Worker → вставить этот файл → Deploy.
 *   3. Worker → Settings → Bindings → добавить D1 database, имя переменной: DB.
 *   4. Worker → Settings → Variables → добавить SITE_URL = адрес сайта (https://...).
 *   5. Worker → Settings → Triggers → Cron Triggers → добавить "* * * * *" (каждую минуту).
 *   6. Открыть адрес Worker в браузере — должно появиться «ProdU push: OK».
 *   7. В index.html вписать адрес Worker в PUSH_SERVER.
 *
 * Адреса:
 *   GET  /        — проверка, что всё настроено
 *   GET  /config  — открыта ли регистрация, нужен ли код приглашения
 *   GET  /vapid   — публичный ключ для подписки
 *   POST /sync    — { subscription, reminders: [{ id, due, title, body }] }
 *   POST /auth/register | /auth/login | /auth/logout | /auth/password | /auth/delete
 *   GET  /data?since=N  — данные аккаунта (или { unchanged: true }); поле av — версия аватарки
 *   PUT  /data?base=N   — сохранить данные, если на сервере всё ещё версия N (иначе 409)
 *   GET | PUT | DELETE /avatar — аватарка профиля (картинка data:image/…;base64); GET /avatar?user=ID — чужая
 *   GET  /users/search?q=ник   — поиск пользователей по началу ника
 *   GET  /social               — друзья, заявки, последние сообщения и непрочитанные
 *   POST /friends/request | accept | decline | remove   { userId }
 *   GET  /messages?with=ID[&after=N | &before=N]       — переписка с другом
 *   POST /messages/send { to, text, image?, voice? } | /messages/read { with, upTo }
 *   GET  /messages/image?id=N  — картинка из сообщения (data:image/jpeg;base64,…)
 *   GET  /messages/voice?id=N  — голосовое сообщение (data:audio/…;base64,…)
 */

const MAX_REMINDERS = 500;        // напоминаний с одного устройства
const SEND_PER_RUN = 25;          // push за один запуск (лимиты бесплатного тарифа)
const MAX_LATE_MS = 12 * 3600e3;  // старше 12 часов — уже не отправляем
const PUSH_TTL = 12 * 3600;       // сколько push-сервис хранит сообщение, если телефон офлайн (сек.)

// Разрешённые push-сервисы (Chrome/Android, Firefox, Edge/Windows, Safari/iPhone)
const PUSH_HOSTS = ['fcm.googleapis.com', 'android.googleapis.com', 'push.services.mozilla.com', 'notify.windows.com', 'push.apple.com'];

// Аккаунты
const LOGIN_RE = /^[\p{L}\p{N}_.-]{3,32}$/u;
const KEY_RE = /^[A-Za-z0-9_-]{43}$/;          // 32 байта в base64url — ключ, полученный из пароля
const SESSION_IDLE_MS = 180 * 86400e3;          // сессия забывается после 180 дней без входа
const MAX_DATA_BYTES = 1_800_000;               // предел строки D1 — 2 МБ
const LOCK_AFTER = 5;                           // после 5 неверных паролей подряд — пауза
const MAX_AVATAR_CHARS = 300_000;               // аватарка 256×256 обычно весит 20–40 КБ
const AVATAR_RE = /^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+/]+={0,2}$/;

// Друзья и сообщения
const MAX_MSG_CHARS = 2000;
const MSG_PER_MINUTE = 30;         // защита от случайного или намеренного флуда
const MAX_OUTGOING_REQUESTS = 50;
const MAX_IMAGE_CHARS = 1_500_000;  // ≈ 1,1 МБ JPEG; приложение сжимает фото до 150–300 КБ
const IMAGES_PER_DAY = 100;         // на одного пользователя — чтобы не переполнить бесплатные 500 МБ базы
const IMAGE_RE = /^data:image\/jpeg;base64,[A-Za-z0-9+/]+={0,2}$/;
const MAX_VOICE_CHARS = 1_900_000;  // предел строки D1 — 2 МБ; 2 минуты голоса обычно 0,5–1 МБ
const MAX_VOICE_MS = 125_000;       // запись ограничена 2 минутами (+ запас)
const VOICES_PER_DAY = 100;
// Профиль: обложка и посты
const MAX_COVER_CHARS = 900_000;   // обложка 1500×600 JPEG обычно 150–300 КБ
const MAX_CAPTION_CHARS = 2000;
const POSTS_PER_DAY = 30;
const POSTS_PAGE = 12;
const VOICE_RE = /^data:audio\/(webm|ogg|mp4|mpeg|aac|x-m4a)(;codecs=[A-Za-z0-9.,"-]+)?;base64,[A-Za-z0-9+/]+={0,2}$/;

export default {
    async fetch(request, env, ctx) {
        const cors = corsHeaders(env);
        if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors });

        try {
            if (!env.DB) return text('Не подключена база D1 с именем переменной DB (Settings → Bindings).', 500, cors);
            await ensureSchema(env);
            const url = new URL(request.url);

            if (request.method === 'GET' && url.pathname === '/') {
                if (!env.SITE_URL) return text('Не задана переменная SITE_URL (Settings → Variables).', 500, cors);
                const { publicKey } = await getVapid(env);
                const stats = await env.DB.prepare(
                    `SELECT (SELECT COUNT(*) FROM subs) AS devices, (SELECT COUNT(*) FROM reminders WHERE sent = 0) AS pending,
                        (SELECT COUNT(*) FROM users) AS users, (SELECT COUNT(*) FROM msg_images) AS images,
                        (SELECT COALESCE(SUM(length(data)), 0) FROM msg_images) AS image_bytes,
                        (SELECT COUNT(*) FROM msg_audio) AS voices,
                        (SELECT COALESCE(SUM(length(data)), 0) FROM msg_audio) AS voice_bytes,
                        (SELECT COUNT(*) FROM posts) AS posts,
                        (SELECT COALESCE(SUM(length(data)), 0) FROM posts) AS post_bytes,
                        (SELECT COALESCE(SUM(length(cover)), 0) FROM profiles) AS cover_bytes`
                ).first();
                const mb = n => (n / 1048576).toFixed(1);
                return text(`ProdU push: OK\nАккаунтов: ${stats.users}\nУстройств с push: ${stats.devices}\nОжидают отправки: ${stats.pending}\n` +
                    `Картинок в чатах: ${stats.images} (${mb(stats.image_bytes)} МБ)\nГолосовых: ${stats.voices} (${mb(stats.voice_bytes)} МБ)\n` +
                    `Постов: ${stats.posts} (${mb(stats.post_bytes)} МБ)\nОбложки профилей: ${mb(stats.cover_bytes)} МБ\n` +
                    `Всего вложений: ${mb(stats.image_bytes + stats.voice_bytes + stats.post_bytes + stats.cover_bytes)} МБ из 500 МБ бесплатной базы\nКлюч: ${publicKey.slice(0, 16)}…`, 200, cors);
            }

            if (request.method === 'GET' && url.pathname === '/config') {
                return json({ registration: env.REGISTRATION !== 'closed', inviteRequired: !!env.INVITE_CODE }, 200, cors);
            }

            if (request.method === 'POST' && url.pathname.startsWith('/auth/')) {
                return await handleAuth(url.pathname.slice(6), request, env, cors);
            }

            if (url.pathname === '/data' && (request.method === 'GET' || request.method === 'PUT')) {
                const user = await authUser(request, env);
                if (!user) return json({ error: 'Сессия истекла — войдите снова' }, 401, cors);
                return request.method === 'GET'
                    ? await getData(env, user, url, cors)
                    : await putData(env, user, url, request, cors);
            }

            if (url.pathname === '/avatar' && ['GET', 'PUT', 'DELETE'].includes(request.method)) {
                const user = await authUser(request, env);
                if (!user) return json({ error: 'Сессия истекла — войдите снова' }, 401, cors);
                return await handleAvatar(env, user, request, url, cors);
            }

            if ((request.method === 'GET' && ['/users/search', '/social', '/messages', '/messages/image', '/messages/voice'].includes(url.pathname)) ||
                (request.method === 'POST' && (url.pathname.startsWith('/friends/') || url.pathname.startsWith('/messages/')))) {
                const user = await authUser(request, env);
                if (!user) return json({ error: 'Сессия истекла — войдите снова' }, 401, cors);
                return await handleSocial(url.pathname, request, url, env, user, cors, ctx);
            }

            if ((request.method === 'GET' && ['/profile', '/profile/cover', '/posts', '/posts/image'].includes(url.pathname)) ||
                (['PUT', 'DELETE'].includes(request.method) && url.pathname === '/profile/cover') ||
                (request.method === 'POST' && ['/profile/settings', '/posts/create', '/posts/delete'].includes(url.pathname))) {
                const user = await authUser(request, env);
                if (!user) return json({ error: 'Сессия истекла — войдите снова' }, 401, cors);
                return await handleProfile(url.pathname, request, url, env, user, cors);
            }

            if (request.method === 'GET' && url.pathname === '/vapid') {
                const { publicKey } = await getVapid(env);
                return json({ publicKey }, 200, cors);
            }

            if (request.method === 'POST' && url.pathname === '/sync') {
                const raw = await request.text();
                if (raw.length > 300_000) return json({ error: 'too large' }, 413, cors);
                const body = JSON.parse(raw);
                const user = await authUser(request, env);   // если вошли — привязываем устройство к аккаунту (для push о сообщениях)
                const count = await syncDevice(env, body, user);
                return json({ ok: true, count }, 200, cors);
            }

            return json({ error: 'not found' }, 404, cors);
        } catch (e) {
            const status = e instanceof BadRequest ? 400 : 500;
            return json({ error: String(e.message || e) }, status, cors);
        }
    },

    async scheduled(event, env, ctx) {
        ctx.waitUntil(sendDue(env));
    }
};

// ===================== База данных =====================
let schemaReady = false;
async function ensureSchema(env) {
    if (schemaReady) return;
    await env.DB.batch([
        env.DB.prepare('CREATE TABLE IF NOT EXISTS config (k TEXT PRIMARY KEY, v TEXT NOT NULL)'),
        env.DB.prepare('CREATE TABLE IF NOT EXISTS subs (endpoint TEXT PRIMARY KEY, p256dh TEXT NOT NULL, auth TEXT NOT NULL, updated INTEGER NOT NULL)'),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS reminders (
            endpoint TEXT NOT NULL, id INTEGER NOT NULL, due INTEGER NOT NULL,
            title TEXT NOT NULL, body TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (endpoint, id))`),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_reminders_due ON reminders (sent, due)'),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT, login TEXT NOT NULL UNIQUE, display TEXT NOT NULL,
            salt TEXT NOT NULL, hash TEXT NOT NULL, created INTEGER NOT NULL,
            failed INTEGER NOT NULL DEFAULT 0, locked_until INTEGER NOT NULL DEFAULT 0)`),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS sessions (
            token_hash TEXT PRIMARY KEY, user_id INTEGER NOT NULL, created INTEGER NOT NULL, last_used INTEGER NOT NULL)`),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions (user_id)'),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS userdata (
            user_id INTEGER PRIMARY KEY, version INTEGER NOT NULL, data TEXT, updated INTEGER NOT NULL)`),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS avatars (
            user_id INTEGER PRIMARY KEY, data TEXT NOT NULL, version INTEGER NOT NULL)`),
        // Дружба хранится одной строкой на пару: a < b
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS friends (
            a INTEGER NOT NULL, b INTEGER NOT NULL, status TEXT NOT NULL, requester INTEGER NOT NULL,
            created INTEGER NOT NULL, PRIMARY KEY (a, b))`),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_friends_b ON friends (b)'),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, a INTEGER NOT NULL, b INTEGER NOT NULL,
            sender INTEGER NOT NULL, body TEXT NOT NULL, created INTEGER NOT NULL)`),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_messages_pair ON messages (a, b, id)'),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_messages_b ON messages (b, id)'),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages (sender, created)'),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS reads (
            user_id INTEGER NOT NULL, peer_id INTEGER NOT NULL, last_id INTEGER NOT NULL, PRIMARY KEY (user_id, peer_id))`),
        env.DB.prepare('CREATE TABLE IF NOT EXISTS sub_users (endpoint TEXT PRIMARY KEY, user_id INTEGER NOT NULL)'),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS msg_images (
            msg_id INTEGER PRIMARY KEY, data TEXT NOT NULL, w INTEGER NOT NULL, h INTEGER NOT NULL)`),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS msg_audio (
            msg_id INTEGER PRIMARY KEY, data TEXT NOT NULL, dur INTEGER NOT NULL, peaks TEXT NOT NULL)`),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_sub_users_user ON sub_users (user_id)'),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS profiles (
            user_id INTEGER PRIMARY KEY, cover TEXT, cover_v INTEGER NOT NULL DEFAULT 0, posts_vis TEXT NOT NULL DEFAULT 'all')`),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, caption TEXT NOT NULL,
            data TEXT NOT NULL, w INTEGER NOT NULL, h INTEGER NOT NULL, created INTEGER NOT NULL)`),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_posts_user ON posts (user_id, id)')
    ]);
    schemaReady = true;
}

class BadRequest extends Error {}

// ===================== Аккаунты =====================
async function readJson(request, limit = 10_000) {
    const raw = await request.text();
    if (raw.length > limit) throw new BadRequest('too large');
    try { return JSON.parse(raw); } catch { throw new BadRequest('bad json'); }
}

async function sha256b64u(...parts) {
    return bytesToB64u(await crypto.subtle.digest('SHA-256', concat(...parts)));
}

function sameString(a, b) {   // сравнение без утечки по времени
    if (typeof a !== 'string' || typeof b !== 'string' || a.length !== b.length) return false;
    let diff = 0;
    for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
    return diff === 0;
}

const randomB64u = n => bytesToB64u(crypto.getRandomValues(new Uint8Array(n)));
const passwordHash = (salt, key) => sha256b64u(b64uToBytes(salt), b64uToBytes(key));
const normLogin = s => String(s || '').trim().normalize('NFC');

async function newSessionStatements(env, userId) {
    const token = randomB64u(32);
    const now = Date.now();
    return {
        token,
        stmts: [
            env.DB.prepare('INSERT INTO sessions (token_hash, user_id, created, last_used) VALUES (?1, ?2, ?3, ?3)')
                .bind(await sha256b64u(enc(token)), userId, now),
            env.DB.prepare('DELETE FROM sessions WHERE user_id = ?1 AND last_used < ?2').bind(userId, now - SESSION_IDLE_MS)
        ]
    };
}

async function authUser(request, env) {
    const m = /^Bearer\s+([A-Za-z0-9_-]{43})$/.exec(request.headers.get('Authorization') || '');
    if (!m) return null;
    const th = await sha256b64u(enc(m[1]));
    const row = await env.DB.prepare(
        'SELECT s.user_id AS id, s.last_used, u.display FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token_hash = ?1'
    ).bind(th).first();
    const now = Date.now();
    if (!row || row.last_used < now - SESSION_IDLE_MS) return null;
    if (row.last_used < now - 6 * 3600e3) {
        await env.DB.prepare('UPDATE sessions SET last_used = ?1 WHERE token_hash = ?2').bind(now, th).run();
    }
    return { id: row.id, display: row.display, tokenHash: th };
}

// Проверка пароля с паузой после нескольких ошибок. Возвращает пользователя или Response с ошибкой.
async function verifyPassword(env, login, key, cors) {
    if (!KEY_RE.test(String(key || ''))) throw new BadRequest('bad key');
    const user = await env.DB.prepare('SELECT * FROM users WHERE login = ?1').bind(normLogin(login).toLowerCase()).first();
    const now = Date.now();
    if (user && user.locked_until > now) {
        const min = Math.ceil((user.locked_until - now) / 60000);
        return json({ error: `Слишком много неверных попыток. Попробуйте через ${min} мин.` }, 429, cors);
    }
    const hash = await passwordHash(user ? user.salt : 'AAAAAAAAAAAAAAAAAAAAAA', key);
    if (!user || !sameString(hash, user.hash)) {
        if (user) {
            const failed = user.failed + 1;
            const lockMin = failed >= LOCK_AFTER ? Math.min(60, 2 ** (failed - LOCK_AFTER)) : 0;
            await env.DB.prepare('UPDATE users SET failed = ?1, locked_until = ?2 WHERE id = ?3')
                .bind(failed, lockMin ? now + lockMin * 60000 : 0, user.id).run();
        }
        return json({ error: 'Неверный логин или пароль' }, 403, cors);
    }
    if (user.failed) await env.DB.prepare('UPDATE users SET failed = 0, locked_until = 0 WHERE id = ?1').bind(user.id).run();
    return user;
}

async function handleAuth(action, request, env, cors) {
    if (action === 'register') {
        const { login, key, invite } = await readJson(request);
        const display = normLogin(login);
        if (!LOGIN_RE.test(display)) throw new BadRequest('Логин: от 3 до 32 символов — буквы, цифры, «_», «.», «-»');
        if (!KEY_RE.test(String(key || ''))) throw new BadRequest('bad key');
        if (env.REGISTRATION === 'closed') return json({ error: 'Регистрация новых аккаунтов закрыта' }, 403, cors);
        if (env.INVITE_CODE && !sameString(String(invite || '').trim(), String(env.INVITE_CODE))) {
            return json({ error: 'Неверный код приглашения' }, 403, cors);
        }
        const salt = randomB64u(16);
        const now = Date.now();
        const row = await env.DB.prepare(`INSERT INTO users (login, display, salt, hash, created) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(login) DO NOTHING RETURNING id`)
            .bind(display.toLowerCase(), display, salt, await passwordHash(salt, key), now).first();
        if (!row) return json({ error: 'Такой логин уже занят' }, 409, cors);
        const s = await newSessionStatements(env, row.id);
        await env.DB.batch([
            env.DB.prepare('INSERT OR IGNORE INTO userdata (user_id, version, data, updated) VALUES (?1, 0, NULL, ?2)').bind(row.id, now),
            ...s.stmts
        ]);
        return json({ login: display, token: s.token }, 200, cors);
    }

    if (action === 'login') {
        const { login, key } = await readJson(request);
        const user = await verifyPassword(env, login, key, cors);
        if (user instanceof Response) return user;
        const s = await newSessionStatements(env, user.id);
        await env.DB.batch(s.stmts);
        return json({ login: user.display, token: s.token }, 200, cors);
    }

    const me = await authUser(request, env);
    if (!me) return json({ error: 'Сессия истекла — войдите снова' }, 401, cors);

    if (action === 'logout') {
        await env.DB.prepare('DELETE FROM sessions WHERE token_hash = ?1').bind(me.tokenHash).run();
        return json({ ok: true }, 200, cors);
    }

    if (action === 'password') {
        const { key, newKey } = await readJson(request);
        if (!KEY_RE.test(String(newKey || ''))) throw new BadRequest('bad key');
        const user = await verifyPassword(env, me.display, key, cors);
        if (user instanceof Response) return user;
        const salt = randomB64u(16);
        const s = await newSessionStatements(env, me.id);
        // Новый пароль — выход на всех остальных устройствах
        await env.DB.batch([
            env.DB.prepare('UPDATE users SET salt = ?1, hash = ?2 WHERE id = ?3').bind(salt, await passwordHash(salt, newKey), me.id),
            env.DB.prepare('DELETE FROM sessions WHERE user_id = ?1').bind(me.id),
            ...s.stmts
        ]);
        return json({ login: me.display, token: s.token }, 200, cors);
    }

    if (action === 'delete') {
        const { key } = await readJson(request);
        const user = await verifyPassword(env, me.display, key, cors);
        if (user instanceof Response) return user;
        await env.DB.batch([
            env.DB.prepare('DELETE FROM sessions WHERE user_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM userdata WHERE user_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM avatars WHERE user_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM friends WHERE a = ?1 OR b = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM msg_images WHERE msg_id IN (SELECT id FROM messages WHERE a = ?1 OR b = ?1)').bind(me.id),
            env.DB.prepare('DELETE FROM msg_audio WHERE msg_id IN (SELECT id FROM messages WHERE a = ?1 OR b = ?1)').bind(me.id),
            env.DB.prepare('DELETE FROM messages WHERE a = ?1 OR b = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM reads WHERE user_id = ?1 OR peer_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM sub_users WHERE user_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM posts WHERE user_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM profiles WHERE user_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM users WHERE id = ?1').bind(me.id)
        ]);
        return json({ ok: true }, 200, cors);
    }

    return json({ error: 'not found' }, 404, cors);
}

// ===================== Данные аккаунта =====================
// Сервер не разбирает данные — хранит их как есть. Объединяет изменения с разных устройств само приложение.
async function getData(env, user, url, cors) {
    const row = await env.DB.prepare(`SELECT
            (SELECT version FROM userdata WHERE user_id = ?1) AS version,
            (SELECT version FROM avatars WHERE user_id = ?1) AS av`).bind(user.id).first();
    const version = row.version || 0;
    const av = row.av || 0;
    if (url.searchParams.get('since') === String(version)) return json({ version, av, unchanged: true }, 200, cors);
    const d = await env.DB.prepare('SELECT data FROM userdata WHERE user_id = ?1').bind(user.id).first();
    return json({ version, av, data: d ? d.data : null }, 200, cors);
}

// ===================== Аватарка =====================
async function handleAvatar(env, user, request, url, cors) {
    if (request.method === 'GET') {
        const uid = url.searchParams.has('user') ? Number(url.searchParams.get('user')) : user.id;
        if (!Number.isSafeInteger(uid)) throw new BadRequest('bad user');
        const row = await env.DB.prepare('SELECT data, version FROM avatars WHERE user_id = ?1').bind(uid).first();
        return json({ av: row ? row.version : 0, data: row ? row.data : null }, 200, cors);
    }
    if (request.method === 'DELETE') {
        await env.DB.prepare('DELETE FROM avatars WHERE user_id = ?1').bind(user.id).run();
        return json({ av: 0 }, 200, cors);
    }
    const raw = (await request.text()).trim();
    if (raw.length > MAX_AVATAR_CHARS) return json({ error: 'Картинка слишком большая' }, 413, cors);
    if (!AVATAR_RE.test(raw)) throw new BadRequest('Это не похоже на картинку');
    const av = Date.now();
    await env.DB.prepare(`INSERT INTO avatars (user_id, data, version) VALUES (?1, ?2, ?3)
        ON CONFLICT(user_id) DO UPDATE SET data = excluded.data, version = excluded.version`).bind(user.id, raw, av).run();
    return json({ av }, 200, cors);
}

async function putData(env, user, url, request, cors) {
    const base = Number(url.searchParams.get('base'));
    if (!Number.isSafeInteger(base) || base < 0) throw new BadRequest('bad base');
    const raw = await request.text();
    if (enc(raw).length > MAX_DATA_BYTES) {
        return json({ error: 'Слишком много данных для одного аккаунта (больше 1,8 МБ)' }, 413, cors);
    }
    if (!raw.startsWith('{') || !raw.endsWith('}')) throw new BadRequest('bad data');

    // Записываем, только если с момента чтения никто не успел сохранить другую версию
    const res = await env.DB.prepare(`INSERT INTO userdata (user_id, version, data, updated) VALUES (?1, 1, ?2, ?3)
        ON CONFLICT(user_id) DO UPDATE SET data = excluded.data, version = userdata.version + 1, updated = excluded.updated
        WHERE userdata.version = ?4`).bind(user.id, raw, Date.now(), base).run();
    if (res.meta && res.meta.changes === 1) return json({ version: base + 1 }, 200, cors);

    const row = await env.DB.prepare('SELECT version FROM userdata WHERE user_id = ?1').bind(user.id).first();
    return json({ error: 'conflict', version: row ? row.version : 0 }, 409, cors);
}

function validSubscription(sub) {
    if (!sub || typeof sub.endpoint !== 'string' || !sub.keys) return false;
    if (typeof sub.keys.p256dh !== 'string' || typeof sub.keys.auth !== 'string') return false;
    let u;
    try { u = new URL(sub.endpoint); } catch { return false; }
    if (u.protocol !== 'https:') return false;
    return PUSH_HOSTS.some(h => u.hostname === h || u.hostname.endsWith('.' + h));
}

async function syncDevice(env, body, user) {
    const sub = body && body.subscription;
    if (!validSubscription(sub)) throw new BadRequest('bad subscription');
    const now = Date.now();

    const list = (Array.isArray(body.reminders) ? body.reminders : [])
        .filter(r => r && Number.isSafeInteger(r.id) && Number.isFinite(r.due) && r.due > now - MAX_LATE_MS)
        .slice(0, MAX_REMINDERS)
        .map(r => ({
            id: r.id,
            due: Math.round(r.due),
            title: String(r.title || 'Напоминание').slice(0, 200),
            body: String(r.body || '').slice(0, 300)
        }));

    // Одна пачка = один запрос для лимитов D1.
    // Неотправленные напоминания устройства заменяются присланным списком;
    // уже отправленные остаются (INSERT OR IGNORE), чтобы не прислать их второй раз.
    await env.DB.batch([
        env.DB.prepare(`INSERT INTO subs (endpoint, p256dh, auth, updated) VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(endpoint) DO UPDATE SET p256dh = excluded.p256dh, auth = excluded.auth, updated = excluded.updated`)
            .bind(sub.endpoint, sub.keys.p256dh, sub.keys.auth, now),
        env.DB.prepare('DELETE FROM reminders WHERE endpoint = ?1 AND sent = 0').bind(sub.endpoint),
        env.DB.prepare(`INSERT OR IGNORE INTO reminders (endpoint, id, due, title, body)
            SELECT ?1, json_extract(value, '$.id'), json_extract(value, '$.due'),
                   json_extract(value, '$.title'), json_extract(value, '$.body')
            FROM json_each(?2)`).bind(sub.endpoint, JSON.stringify(list)),
        env.DB.prepare('DELETE FROM reminders WHERE endpoint = ?1 AND sent = 1 AND due < ?2')
            .bind(sub.endpoint, now - 3 * 86400e3),
        user
            ? env.DB.prepare(`INSERT INTO sub_users (endpoint, user_id) VALUES (?1, ?2)
                ON CONFLICT(endpoint) DO UPDATE SET user_id = excluded.user_id`).bind(sub.endpoint, user.id)
            : env.DB.prepare('DELETE FROM sub_users WHERE endpoint = ?1').bind(sub.endpoint)
    ]);
    return list.length;
}

// ===================== Отправка по расписанию =====================
async function sendDue(env) {
    if (!env.DB || !env.SITE_URL) return;
    await ensureSchema(env);
    const now = Date.now();

    // Слишком старые пропускаем, давно отправленные удаляем
    await env.DB.batch([
        env.DB.prepare('UPDATE reminders SET sent = 1 WHERE sent = 0 AND due < ?1').bind(now - MAX_LATE_MS),
        env.DB.prepare('DELETE FROM reminders WHERE sent = 1 AND due < ?1').bind(now - 3 * 86400e3)
    ]);

    const { results } = await env.DB.prepare(`
        SELECT r.endpoint, r.id, r.title, r.body, s.p256dh, s.auth
        FROM reminders r JOIN subs s ON s.endpoint = r.endpoint
        WHERE r.sent = 0 AND r.due <= ?1
        ORDER BY r.due LIMIT ?2`).bind(now, SEND_PER_RUN).all();
    if (!results.length) return;

    const vapid = await getVapid(env);
    const done = [];   // отправлено или ошибка, которую повторять бесполезно
    const gone = [];   // подписка больше не существует

    await Promise.all(results.map(async r => {
        const payload = JSON.stringify({ id: r.id, title: r.title, body: r.body, tag: 'produ-' + r.id });
        try {
            const res = await sendPush({ endpoint: r.endpoint, p256dh: r.p256dh, auth: r.auth }, payload, vapid, env.SITE_URL);
            if (res.ok) done.push({ e: r.endpoint, i: r.id });
            else if (res.status === 404 || res.status === 410) gone.push(r.endpoint);
            else if (res.status >= 400 && res.status < 500 && res.status !== 429) {
                console.warn('push rejected', res.status, await res.text());
                done.push({ e: r.endpoint, i: r.id });
            }
            // 429 и 5xx — попробуем в следующую минуту
        } catch (e) {
            console.warn('push failed', e);
        }
    }));

    const stmts = [];
    if (done.length) {
        stmts.push(env.DB.prepare(`UPDATE reminders SET sent = 1 WHERE (endpoint, id) IN
            (SELECT json_extract(value, '$.e'), json_extract(value, '$.i') FROM json_each(?1))`).bind(JSON.stringify(done)));
    }
    if (gone.length) {
        const g = JSON.stringify([...new Set(gone)]);
        stmts.push(env.DB.prepare('DELETE FROM reminders WHERE endpoint IN (SELECT value FROM json_each(?1))').bind(g));
        stmts.push(env.DB.prepare('DELETE FROM subs WHERE endpoint IN (SELECT value FROM json_each(?1))').bind(g));
        stmts.push(env.DB.prepare('DELETE FROM sub_users WHERE endpoint IN (SELECT value FROM json_each(?1))').bind(g));
    }
    if (stmts.length) await env.DB.batch(stmts);
}

// ===================== Друзья и сообщения =====================
const pairOf = (x, y) => (x < y ? [x, y] : [y, x]);

function relation(row, meId) {
    if (!row || !row.status) return 'none';
    if (row.status === 'accepted') return 'friend';
    return row.requester === meId ? 'outgoing' : 'incoming';
}

async function getFriendRow(env, meId, otherId) {
    const [a, b] = pairOf(meId, otherId);
    return env.DB.prepare('SELECT status, requester FROM friends WHERE a = ?1 AND b = ?2').bind(a, b).first();
}

function readUserId(v) {
    const n = Number(v);
    if (!Number.isSafeInteger(n) || n <= 0) throw new BadRequest('bad user');
    return n;
}

async function handleSocial(path, request, url, env, me, cors, ctx) {
    // --- поиск ---
    if (path === '/users/search') {
        const q = normLogin(url.searchParams.get('q')).toLowerCase();
        if ([...q].length < 2) return json({ users: [] }, 200, cors);
        const like = q.replace(/[\\%_]/g, c => '\\' + c) + '%';
        const { results } = await env.DB.prepare(`
            SELECT u.id, u.display AS login, COALESCE(av.version, 0) AS av, f.status, f.requester
            FROM users u
            LEFT JOIN avatars av ON av.user_id = u.id
            LEFT JOIN friends f ON f.a = min(u.id, ?1) AND f.b = max(u.id, ?1)
            WHERE u.login LIKE ?2 ESCAPE '\\' AND u.id != ?1
            ORDER BY (u.login = ?3) DESC, length(u.login), u.login
            LIMIT 20`).bind(me.id, like, q).all();
        return json({ users: results.map(r => ({ id: r.id, login: r.login, av: r.av, rel: relation(r, me.id) })) }, 200, cors);
    }

    // --- друзья, заявки, последние сообщения ---
    if (path === '/social') {
        const { results: rels } = await env.DB.prepare(`
            SELECT CASE WHEN f.a = ?1 THEN f.b ELSE f.a END AS id, f.status, f.requester, f.created,
                   u.display AS login, COALESCE(av.version, 0) AS av
            FROM friends f
            JOIN users u ON u.id = (CASE WHEN f.a = ?1 THEN f.b ELSE f.a END)
            LEFT JOIN avatars av ON av.user_id = u.id
            WHERE f.a = ?1 OR f.b = ?1`).bind(me.id).all();
        const { results: last } = await env.DB.prepare(`
            SELECT m.id, m.a, m.b, m.sender, m.body, m.created, EXISTS(SELECT 1 FROM msg_images i WHERE i.msg_id = m.id) AS img,
                (SELECT dur FROM msg_audio au WHERE au.msg_id = m.id) AS vdur FROM messages m
            JOIN (SELECT MAX(id) AS id FROM messages WHERE a = ?1 OR b = ?1 GROUP BY a, b) t ON t.id = m.id`).bind(me.id).all();
        const { results: unread } = await env.DB.prepare(`
            SELECT m.sender AS id, COUNT(*) AS n FROM messages m
            LEFT JOIN reads r ON r.user_id = ?1 AND r.peer_id = m.sender
            WHERE (m.a = ?1 OR m.b = ?1) AND m.sender != ?1 AND m.id > COALESCE(r.last_id, 0)
            GROUP BY m.sender`).bind(me.id).all();

        const lastBy = new Map(last.map(m => [m.a === me.id ? m.b : m.a, { id: m.id, sender: m.sender, body: m.body.slice(0, 200), created: m.created, img: !!m.img, voice: m.vdur ? { dur: m.vdur } : null }]));
        const unreadBy = new Map(unread.map(u => [u.id, u.n]));
        const person = r => ({ id: r.id, login: r.login, av: r.av });
        const friends = rels.filter(r => r.status === 'accepted')
            .map(r => ({ ...person(r), last: lastBy.get(r.id) || null, unread: unreadBy.get(r.id) || 0, since: r.created }))
            .sort((x, y) => ((y.last ? y.last.created : y.since) - (x.last ? x.last.created : x.since)));
        return json({
            me: me.id,
            friends,
            incoming: rels.filter(r => r.status === 'pending' && r.requester !== me.id).map(person),
            outgoing: rels.filter(r => r.status === 'pending' && r.requester === me.id).map(person)
        }, 200, cors);
    }

    // --- заявки в друзья ---
    if (path.startsWith('/friends/')) {
        const action = path.slice(9);
        const other = readUserId((await readJson(request)).userId);
        if (other === me.id) throw new BadRequest('Нельзя добавить в друзья самого себя');
        const [a, b] = pairOf(me.id, other);
        const row = await getFriendRow(env, me.id, other);
        const rel = relation(row, me.id);
        const now = Date.now();

        if (action === 'request') {
            if (rel === 'friend' || rel === 'outgoing') return json({ rel }, 200, cors);
            if (rel === 'incoming') {   // встречная заявка — сразу дружба
                await env.DB.prepare("UPDATE friends SET status = 'accepted', created = ?3 WHERE a = ?1 AND b = ?2").bind(a, b, now).run();
                return json({ rel: 'friend' }, 200, cors);
            }
            const exists = await env.DB.prepare('SELECT id FROM users WHERE id = ?1').bind(other).first();
            if (!exists) return json({ error: 'Пользователь не найден' }, 404, cors);
            const cnt = await env.DB.prepare("SELECT COUNT(*) AS n FROM friends WHERE requester = ?1 AND status = 'pending'").bind(me.id).first();
            if (cnt.n >= MAX_OUTGOING_REQUESTS) return json({ error: 'Слишком много заявок без ответа. Отмените часть из них.' }, 429, cors);
            await env.DB.prepare("INSERT OR IGNORE INTO friends (a, b, status, requester, created) VALUES (?1, ?2, 'pending', ?3, ?4)")
                .bind(a, b, me.id, now).run();
            return json({ rel: 'outgoing' }, 200, cors);
        }
        if (action === 'accept') {
            if (rel !== 'incoming') return json({ error: 'Заявка не найдена' }, 404, cors);
            await env.DB.prepare("UPDATE friends SET status = 'accepted', created = ?3 WHERE a = ?1 AND b = ?2").bind(a, b, now).run();
            return json({ rel: 'friend' }, 200, cors);
        }
        if (action === 'decline') {
            if (rel === 'incoming') await env.DB.prepare('DELETE FROM friends WHERE a = ?1 AND b = ?2').bind(a, b).run();
            return json({ rel: 'none' }, 200, cors);
        }
        if (action === 'remove') {   // удалить из друзей или отменить свою заявку; переписка удаляется у обоих
            await env.DB.batch([
                env.DB.prepare('DELETE FROM friends WHERE a = ?1 AND b = ?2').bind(a, b),
                env.DB.prepare('DELETE FROM msg_images WHERE msg_id IN (SELECT id FROM messages WHERE a = ?1 AND b = ?2)').bind(a, b),
                env.DB.prepare('DELETE FROM msg_audio WHERE msg_id IN (SELECT id FROM messages WHERE a = ?1 AND b = ?2)').bind(a, b),
                env.DB.prepare('DELETE FROM messages WHERE a = ?1 AND b = ?2').bind(a, b),
                env.DB.prepare('DELETE FROM reads WHERE (user_id = ?1 AND peer_id = ?2) OR (user_id = ?2 AND peer_id = ?1)').bind(a, b)
            ]);
            return json({ rel: 'none' }, 200, cors);
        }
        return json({ error: 'not found' }, 404, cors);
    }

    // --- переписка ---
    if (path === '/messages') {
        const other = readUserId(url.searchParams.get('with'));
        if (relation(await getFriendRow(env, me.id, other), me.id) !== 'friend') {
            return json({ error: 'Переписка доступна только с друзьями' }, 403, cors);
        }
        const [a, b] = pairOf(me.id, other);
        const after = Number(url.searchParams.get('after')) || 0;
        const before = Number(url.searchParams.get('before')) || 0;
        let messages, hasMore = false;
        if (after) {
            ({ results: messages } = await env.DB.prepare(
                `SELECT m.id, m.sender, m.body, m.created, i.w, i.h, au.dur, au.peaks FROM messages m
                 LEFT JOIN msg_images i ON i.msg_id = m.id LEFT JOIN msg_audio au ON au.msg_id = m.id
                 WHERE m.a = ?1 AND m.b = ?2 AND m.id > ?3 ORDER BY m.id LIMIT 200`
            ).bind(a, b, after).all());
        } else {
            const { results } = await env.DB.prepare(
                `SELECT m.id, m.sender, m.body, m.created, i.w, i.h, au.dur, au.peaks FROM messages m
                 LEFT JOIN msg_images i ON i.msg_id = m.id LEFT JOIN msg_audio au ON au.msg_id = m.id
                 WHERE m.a = ?1 AND m.b = ?2 AND m.id < ?3 ORDER BY m.id DESC LIMIT 51`
            ).bind(a, b, before || Number.MAX_SAFE_INTEGER).all();
            hasMore = results.length > 50;
            messages = results.slice(0, 50).reverse();
        }
        messages = messages.map(m => {
            const out = { id: m.id, sender: m.sender, body: m.body, created: m.created };
            if (m.w) out.img = { w: m.w, h: m.h };
            if (m.dur) out.voice = { dur: m.dur, peaks: m.peaks };
            return out;
        });
        const pr = await env.DB.prepare('SELECT last_id FROM reads WHERE user_id = ?1 AND peer_id = ?2').bind(other, me.id).first();
        return json({ messages, hasMore, peerRead: pr ? pr.last_id : 0 }, 200, cors);
    }

    if (path === '/messages/image' || path === '/messages/voice') {
        const id = readUserId(url.searchParams.get('id'));
        const table = path === '/messages/image' ? 'msg_images' : 'msg_audio';
        const row = await env.DB.prepare(`SELECT x.data FROM ${table} x JOIN messages m ON m.id = x.msg_id
            WHERE x.msg_id = ?1 AND (m.a = ?2 OR m.b = ?2)`).bind(id, me.id).first();
        if (!row) return json({ error: path === '/messages/image' ? 'Картинка не найдена' : 'Голосовое не найдено' }, 404, cors);
        return new Response(JSON.stringify({ data: row.data }), {
            status: 200,
            headers: { ...cors, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'private, max-age=31536000, immutable' }
        });
    }

    if (path === '/messages/send') {
        const { to, text, image, voice } = await readJson(request, Math.max(MAX_IMAGE_CHARS, MAX_VOICE_CHARS) + 30_000);
        const other = readUserId(to);
        const body = String(text || '').replace(/\r\n?/g, '\n').trim();
        let img = null;
        if (image) {
            const data = String(image.data || '');
            const w = Number(image.w), h = Number(image.h);
            if (data.length > MAX_IMAGE_CHARS) return json({ error: 'Картинка слишком большая' }, 413, cors);
            if (!IMAGE_RE.test(data)) throw new BadRequest('Это не похоже на картинку JPEG');
            if (!Number.isInteger(w) || !Number.isInteger(h) || w < 1 || h < 1 || w > 4096 || h > 4096) throw new BadRequest('bad size');
            img = { data, w, h };
        }
        let vc = null;
        if (voice) {
            if (img) throw new BadRequest('Нельзя отправить картинку и голосовое одним сообщением');
            const data = String(voice.data || '');
            const dur = Math.round(Number(voice.dur));
            const peaks = String(voice.peaks || '');
            if (data.length > MAX_VOICE_CHARS) return json({ error: 'Голосовое слишком длинное — запишите покороче' }, 413, cors);
            if (!VOICE_RE.test(data)) throw new BadRequest('Это не похоже на аудиозапись');
            if (!Number.isFinite(dur) || dur < 300 || dur > MAX_VOICE_MS) throw new BadRequest('Неверная длительность записи');
            if (!/^[0-9a-f]{0,64}$/.test(peaks)) throw new BadRequest('bad peaks');
            vc = { data, dur, peaks };
        }
        if (!body && !img && !vc) throw new BadRequest('Пустое сообщение');
        if ([...body].length > MAX_MSG_CHARS) return json({ error: `Сообщение длиннее ${MAX_MSG_CHARS} символов` }, 400, cors);
        if (relation(await getFriendRow(env, me.id, other), me.id) !== 'friend') {
            return json({ error: 'Писать можно только друзьям' }, 403, cors);
        }
        const now = Date.now();
        const recent = await env.DB.prepare('SELECT COUNT(*) AS n FROM messages WHERE sender = ?1 AND created > ?2').bind(me.id, now - 60000).first();
        if (recent.n >= MSG_PER_MINUTE) return json({ error: 'Слишком много сообщений подряд — подождите минуту' }, 429, cors);
        if (img) {
            const today = await env.DB.prepare(`SELECT COUNT(*) AS n FROM msg_images i JOIN messages m ON m.id = i.msg_id
                WHERE m.sender = ?1 AND m.created > ?2`).bind(me.id, now - 86400e3).first();
            if (today.n >= IMAGES_PER_DAY) return json({ error: `Можно отправить не больше ${IMAGES_PER_DAY} картинок в сутки` }, 429, cors);
        }
        if (vc) {
            const today = await env.DB.prepare(`SELECT COUNT(*) AS n FROM msg_audio x JOIN messages m ON m.id = x.msg_id
                WHERE m.sender = ?1 AND m.created > ?2`).bind(me.id, now - 86400e3).first();
            if (today.n >= VOICES_PER_DAY) return json({ error: `Можно отправить не больше ${VOICES_PER_DAY} голосовых в сутки` }, 429, cors);
        }
        const [a, b] = pairOf(me.id, other);
        const row = await env.DB.prepare('INSERT INTO messages (a, b, sender, body, created) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING id')
            .bind(a, b, me.id, body, now).first();
        if (img || vc) {
            try {
                if (img) await env.DB.prepare('INSERT INTO msg_images (msg_id, data, w, h) VALUES (?1, ?2, ?3, ?4)').bind(row.id, img.data, img.w, img.h).run();
                if (vc) await env.DB.prepare('INSERT INTO msg_audio (msg_id, data, dur, peaks) VALUES (?1, ?2, ?3, ?4)').bind(row.id, vc.data, vc.dur, vc.peaks).run();
            } catch (e) {
                await env.DB.prepare('DELETE FROM messages WHERE id = ?1').bind(row.id).run();
                throw e;
            }
        }
        const message = { id: row.id, sender: me.id, body, created: now };
        if (img) message.img = { w: img.w, h: img.h };
        if (vc) message.voice = { dur: vc.dur, peaks: vc.peaks };
        const textPreview = [...body].slice(0, 140).join('') + ([...body].length > 140 ? '…' : '');
        const fmtDur = ms => { const sec = Math.round(ms / 1000); return `${Math.floor(sec / 60)}:${String(sec % 60).padStart(2, '0')}`; };
        const preview = vc ? `🎤 Голосовое (${fmtDur(vc.dur)})`
            : img ? '📷 Фото' + (textPreview ? ' · ' + textPreview : '') : textPreview;
        const push = pushToUser(env, other, { kind: 'msg', fromId: me.id, title: me.display, body: preview, tag: 'chat-' + me.id });
        if (ctx && ctx.waitUntil) ctx.waitUntil(push); else await push;
        return json({ message }, 200, cors);
    }

    if (path === '/messages/read') {
        const { with: w, upTo } = await readJson(request);
        const other = readUserId(w);
        const last = Number(upTo);
        if (!Number.isSafeInteger(last) || last < 0) throw new BadRequest('bad upTo');
        await env.DB.prepare(`INSERT INTO reads (user_id, peer_id, last_id) VALUES (?1, ?2, ?3)
            ON CONFLICT(user_id, peer_id) DO UPDATE SET last_id = max(reads.last_id, excluded.last_id)`).bind(me.id, other, last).run();
        return json({ ok: true }, 200, cors);
    }

    return json({ error: 'not found' }, 404, cors);
}

// ===================== Профиль и посты =====================
// Профиль (ник, аватарку, обложку) видят все, кто вошёл в аккаунт.
// Посты — все или только друзья, как выберет владелец в настройках.
async function postsVisibleTo(env, meId, ownerId) {
    if (ownerId === meId) return true;
    const p = await env.DB.prepare('SELECT posts_vis FROM profiles WHERE user_id = ?1').bind(ownerId).first();
    if (!p || p.posts_vis !== 'friends') return true;
    return relation(await getFriendRow(env, meId, ownerId), meId) === 'friend';
}

async function handleProfile(path, request, url, env, me, cors) {
    if (path === '/profile') {
        const uid = url.searchParams.has('user') ? readUserId(url.searchParams.get('user')) : me.id;
        const row = await env.DB.prepare(`
            SELECT u.id, u.display AS login, u.created, COALESCE(av.version, 0) AS av,
                   COALESCE(p.cover_v, 0) AS cover, COALESCE(p.posts_vis, 'all') AS vis,
                   (SELECT COUNT(*) FROM posts WHERE user_id = u.id) AS posts,
                   (SELECT COUNT(*) FROM friends WHERE (a = u.id OR b = u.id) AND status = 'accepted') AS friends
            FROM users u
            LEFT JOIN avatars av ON av.user_id = u.id
            LEFT JOIN profiles p ON p.user_id = u.id
            WHERE u.id = ?1`).bind(uid).first();
        if (!row) return json({ error: 'Пользователь не найден' }, 404, cors);
        const rel = uid === me.id ? 'me' : relation(await getFriendRow(env, me.id, uid), me.id);
        const profile = {
            id: row.id, login: row.login, created: row.created, av: row.av, cover: row.cover, rel,
            posts: row.posts, friends: row.friends, postsVisible: uid === me.id || row.vis !== 'friends' || rel === 'friend'
        };
        if (uid === me.id) profile.postsVis = row.vis;
        return json({ profile }, 200, cors);
    }

    if (path === '/profile/cover') {
        if (request.method === 'GET') {
            const uid = url.searchParams.has('user') ? readUserId(url.searchParams.get('user')) : me.id;
            const row = await env.DB.prepare('SELECT cover, cover_v FROM profiles WHERE user_id = ?1').bind(uid).first();
            return json({ cover: row && row.cover ? row.cover_v : 0, data: row ? row.cover : null }, 200, cors);
        }
        if (request.method === 'DELETE') {
            await env.DB.prepare('UPDATE profiles SET cover = NULL, cover_v = 0 WHERE user_id = ?1').bind(me.id).run();
            return json({ cover: 0 }, 200, cors);
        }
        const raw = (await request.text()).trim();
        if (raw.length > MAX_COVER_CHARS) return json({ error: 'Картинка слишком большая' }, 413, cors);
        if (!IMAGE_RE.test(raw)) throw new BadRequest('Это не похоже на картинку JPEG');
        const v = Date.now();
        await env.DB.prepare(`INSERT INTO profiles (user_id, cover, cover_v) VALUES (?1, ?2, ?3)
            ON CONFLICT(user_id) DO UPDATE SET cover = excluded.cover, cover_v = excluded.cover_v`).bind(me.id, raw, v).run();
        return json({ cover: v }, 200, cors);
    }

    if (path === '/profile/settings') {
        const { postsVis } = await readJson(request);
        if (!['all', 'friends'].includes(postsVis)) throw new BadRequest('bad postsVis');
        await env.DB.prepare(`INSERT INTO profiles (user_id, posts_vis) VALUES (?1, ?2)
            ON CONFLICT(user_id) DO UPDATE SET posts_vis = excluded.posts_vis`).bind(me.id, postsVis).run();
        return json({ postsVis }, 200, cors);
    }

    if (path === '/posts') {
        const uid = url.searchParams.has('user') ? readUserId(url.searchParams.get('user')) : me.id;
        if (!(await postsVisibleTo(env, me.id, uid))) return json({ posts: [], hasMore: false, hidden: true }, 200, cors);
        const before = Number(url.searchParams.get('before')) || Number.MAX_SAFE_INTEGER;
        const { results } = await env.DB.prepare(`SELECT id, caption, w, h, created FROM posts
            WHERE user_id = ?1 AND id < ?2 ORDER BY id DESC LIMIT ?3`).bind(uid, before, POSTS_PAGE + 1).all();
        return json({ posts: results.slice(0, POSTS_PAGE), hasMore: results.length > POSTS_PAGE }, 200, cors);
    }

    if (path === '/posts/image') {
        const id = readUserId(url.searchParams.get('id'));
        const row = await env.DB.prepare('SELECT user_id, data FROM posts WHERE id = ?1').bind(id).first();
        if (!row || !(await postsVisibleTo(env, me.id, row.user_id))) return json({ error: 'Пост не найден' }, 404, cors);
        return new Response(JSON.stringify({ data: row.data }), {
            status: 200,
            headers: { ...cors, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'private, max-age=31536000, immutable' }
        });
    }

    if (path === '/posts/create') {
        const { image, caption } = await readJson(request, MAX_IMAGE_CHARS + 30_000);
        const text = String(caption || '').replace(/\r\n?/g, '\n').trim();
        if ([...text].length > MAX_CAPTION_CHARS) return json({ error: `Подпись длиннее ${MAX_CAPTION_CHARS} символов` }, 400, cors);
        if (!image) throw new BadRequest('Добавьте фото');
        const data = String(image.data || '');
        const w = Number(image.w), h = Number(image.h);
        if (data.length > MAX_IMAGE_CHARS) return json({ error: 'Картинка слишком большая' }, 413, cors);
        if (!IMAGE_RE.test(data)) throw new BadRequest('Это не похоже на картинку JPEG');
        if (!Number.isInteger(w) || !Number.isInteger(h) || w < 1 || h < 1 || w > 4096 || h > 4096) throw new BadRequest('bad size');
        const now = Date.now();
        const today = await env.DB.prepare('SELECT COUNT(*) AS n FROM posts WHERE user_id = ?1 AND created > ?2').bind(me.id, now - 86400e3).first();
        if (today.n >= POSTS_PER_DAY) return json({ error: `Можно опубликовать не больше ${POSTS_PER_DAY} постов в сутки` }, 429, cors);
        const row = await env.DB.prepare('INSERT INTO posts (user_id, caption, data, w, h, created) VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING id')
            .bind(me.id, text, data, w, h, now).first();
        return json({ post: { id: row.id, caption: text, w, h, created: now } }, 200, cors);
    }

    if (path === '/posts/delete') {
        const id = readUserId((await readJson(request)).id);
        const r = await env.DB.prepare('DELETE FROM posts WHERE id = ?1 AND user_id = ?2').bind(id, me.id).run();
        const changed = r && r.meta ? r.meta.changes : 1;
        if (!changed) return json({ error: 'Пост не найден' }, 404, cors);
        return json({ ok: true }, 200, cors);
    }

    return json({ error: 'not found' }, 404, cors);
}

// Push о новом сообщении на все устройства получателя, где он вошёл в аккаунт
async function pushToUser(env, userId, payload) {
    try {
        if (!env.SITE_URL) return;
        const { results } = await env.DB.prepare(`SELECT s.endpoint, s.p256dh, s.auth FROM sub_users su
            JOIN subs s ON s.endpoint = su.endpoint WHERE su.user_id = ?1 LIMIT 10`).bind(userId).all();
        if (!results.length) return;
        const vapid = await getVapid(env);
        const body = JSON.stringify(payload);
        const gone = [];
        await Promise.all(results.map(async s => {
            try {
                const r = await sendPush(s, body, vapid, env.SITE_URL);
                if (r.status === 404 || r.status === 410) gone.push(s.endpoint);
            } catch (e) { console.warn('msg push failed', e); }
        }));
        if (gone.length) {
            const g = JSON.stringify(gone);
            await env.DB.batch([
                env.DB.prepare('DELETE FROM reminders WHERE endpoint IN (SELECT value FROM json_each(?1))').bind(g),
                env.DB.prepare('DELETE FROM subs WHERE endpoint IN (SELECT value FROM json_each(?1))').bind(g),
                env.DB.prepare('DELETE FROM sub_users WHERE endpoint IN (SELECT value FROM json_each(?1))').bind(g)
            ]);
        }
    } catch (e) {
        console.warn('pushToUser', e);
    }
}

// ===================== VAPID-ключи =====================
let vapidCache = null;
async function getVapid(env) {
    if (vapidCache) return vapidCache;
    let row = await env.DB.prepare("SELECT v FROM config WHERE k = 'vapid'").first();
    if (!row) {
        const pair = await crypto.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, true, ['sign', 'verify']);
        const jwk = await crypto.subtle.exportKey('jwk', pair.privateKey);
        const pub = bytesToB64u(await crypto.subtle.exportKey('raw', pair.publicKey));
        await env.DB.prepare("INSERT OR IGNORE INTO config (k, v) VALUES ('vapid', ?1)")
            .bind(JSON.stringify({ jwk, publicKey: pub })).run();
        row = await env.DB.prepare("SELECT v FROM config WHERE k = 'vapid'").first();
    }
    const { jwk, publicKey } = JSON.parse(row.v);
    const privateKey = await crypto.subtle.importKey('jwk', jwk, { name: 'ECDSA', namedCurve: 'P-256' }, false, ['sign']);
    vapidCache = { privateKey, publicKey, jwts: new Map() };
    return vapidCache;
}

async function vapidJwt(vapid, audience, subject) {
    const cached = vapid.jwts.get(audience);
    const now = Math.floor(Date.now() / 1000);
    if (cached && cached.exp - now > 3600) return cached.token;
    const exp = now + 12 * 3600;
    const header = b64uJson({ typ: 'JWT', alg: 'ES256' });
    const claims = b64uJson({ aud: audience, exp, sub: subject });
    const unsigned = `${header}.${claims}`;
    const sig = await crypto.subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, vapid.privateKey, enc(unsigned));
    const token = `${unsigned}.${bytesToB64u(sig)}`;
    vapid.jwts.set(audience, { token, exp });
    return token;
}

// ===================== Web Push (RFC 8291 + RFC 8292) =====================
async function sendPush(sub, payload, vapid, siteUrl) {
    const audience = new URL(sub.endpoint).origin;
    const subject = siteUrl.startsWith('mailto:') ? siteUrl : new URL(siteUrl).origin;
    const jwt = await vapidJwt(vapid, audience, subject);
    const body = await encryptPayload(payload, sub.p256dh, sub.auth);
    return fetch(sub.endpoint, {
        method: 'POST',
        headers: {
            'Authorization': `vapid t=${jwt}, k=${vapid.publicKey}`,
            'Content-Encoding': 'aes128gcm',
            'Content-Type': 'application/octet-stream',
            'TTL': String(PUSH_TTL),
            'Urgency': 'high'
        },
        body
    });
}

async function encryptPayload(payload, p256dhB64, authB64) {
    const uaPublic = b64uToBytes(p256dhB64);
    const authSecret = b64uToBytes(authB64);

    const uaKey = await crypto.subtle.importKey('raw', uaPublic, { name: 'ECDH', namedCurve: 'P-256' }, false, []);
    const asKeys = await crypto.subtle.generateKey({ name: 'ECDH', namedCurve: 'P-256' }, true, ['deriveBits']);
    const asPublic = new Uint8Array(await crypto.subtle.exportKey('raw', asKeys.publicKey));
    const ecdhSecret = new Uint8Array(await crypto.subtle.deriveBits({ name: 'ECDH', public: uaKey }, asKeys.privateKey, 256));

    const ikm = await hkdf(authSecret, ecdhSecret, concat(enc('WebPush: info\0'), uaPublic, asPublic), 32);
    const salt = crypto.getRandomValues(new Uint8Array(16));
    const cek = await hkdf(salt, ikm, enc('Content-Encoding: aes128gcm\0'), 16);
    const nonce = await hkdf(salt, ikm, enc('Content-Encoding: nonce\0'), 12);

    const plain = concat(enc(payload), new Uint8Array([2]));   // 0x02 — последняя запись
    const aesKey = await crypto.subtle.importKey('raw', cek, 'AES-GCM', false, ['encrypt']);
    const cipher = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv: nonce }, aesKey, plain));

    const header = new Uint8Array(16 + 4 + 1 + asPublic.length);
    header.set(salt, 0);
    new DataView(header.buffer).setUint32(16, 4096);
    header[20] = asPublic.length;
    header.set(asPublic, 21);
    return concat(header, cipher);
}

async function hkdf(salt, ikm, info, length) {
    const key = await crypto.subtle.importKey('raw', ikm, 'HKDF', false, ['deriveBits']);
    return new Uint8Array(await crypto.subtle.deriveBits({ name: 'HKDF', hash: 'SHA-256', salt, info }, key, length * 8));
}

// ===================== Мелочи =====================
function enc(s) { return new TextEncoder().encode(s); }
function concat(...arrs) {
    const out = new Uint8Array(arrs.reduce((n, a) => n + a.length, 0));
    let o = 0;
    for (const a of arrs) { out.set(a, o); o += a.length; }
    return out;
}
function bytesToB64u(buf) {
    let s = '';
    for (const b of new Uint8Array(buf)) s += String.fromCharCode(b);
    return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}
function b64uToBytes(s) {
    const b = atob(s.replace(/-/g, '+').replace(/_/g, '/') + '==='.slice((s.length + 3) % 4));
    return Uint8Array.from(b, c => c.charCodeAt(0));
}
function b64uJson(o) { return bytesToB64u(enc(JSON.stringify(o))); }

function corsHeaders(env) {
    let origin = '*';
    try { if (env.SITE_URL && !env.SITE_URL.startsWith('mailto:')) origin = new URL(env.SITE_URL).origin; } catch {}
    return {
        'Access-Control-Allow-Origin': origin,
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Access-Control-Max-Age': '86400',
        'Vary': 'Origin'
    };
}
function json(obj, status, headers) {
    return new Response(JSON.stringify(obj), { status, headers: { ...headers, 'Content-Type': 'application/json; charset=utf-8' } });
}
function text(s, status, headers) {
    return new Response(s, { status, headers: { ...headers, 'Content-Type': 'text/plain; charset=utf-8' } });
}
