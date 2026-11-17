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
 *   REGISTRATION — значение closed полностью закрывает регистрацию новых аккаунтов;
 *   ADMIN_KEY    — ключ для страницы /admin, где разработчик публикует объявления (и рассылает их push).
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
 *   Вишлисты (нужен вход):
 *   GET  /wish/lists[?user=ID] | /wish/list?id=N (или ?t=токен) | /wish/image?id=N | /wish/preview?url=…
 *   POST /wish/save | /wish/delete | /wish/token | /wish/reset | /wish/item/save | /wish/item/delete | /wish/reserve
 *   Публичная страница (без входа): GET /w/<токен>, GET /w/<токен>/img/<id>, POST /w/<токен>/reserve
 *   Объявления разработчика: GET /notices?since=ID — для всех; GET /admin — страница публикации
 *   (нужна переменная ADMIN_KEY не короче 12 символов), POST /admin/list | /admin/notice | /admin/delete
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

            // объявления разработчика и страница их публикации
            if (url.pathname === '/notices' || url.pathname === '/admin' || url.pathname.startsWith('/admin/')) {
                const r = await handleNotices(request, url, env, cors, ctx);
                if (r) return r;
            }

            // публичная страница вишлиста — открывается по ссылке без аккаунта
            if (url.pathname.startsWith('/w/')) {
                const r = await handleWishPublic(request, url, env);
                if (r) return r;
            }

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
                        (SELECT COALESCE(SUM(length(cover)), 0) FROM profiles) AS cover_bytes,
                        (SELECT COUNT(*) FROM wishlists) AS wishlists,
                        (SELECT COALESCE(SUM(length(data)), 0) FROM wish_images) AS wish_bytes`
                ).first();
                const mb = n => (n / 1048576).toFixed(1);
                return text(`ProdU push: OK\nАккаунтов: ${stats.users}\nУстройств с push: ${stats.devices}\nОжидают отправки: ${stats.pending}\n` +
                    `Картинок в чатах: ${stats.images} (${mb(stats.image_bytes)} МБ)\nГолосовых: ${stats.voices} (${mb(stats.voice_bytes)} МБ)\n` +
                    `Постов: ${stats.posts} (${mb(stats.post_bytes)} МБ)\nОбложки профилей: ${mb(stats.cover_bytes)} МБ\n` +
                    `Вишлистов: ${stats.wishlists} (фото ${mb(stats.wish_bytes)} МБ)\n` +
                    `Всего вложений: ${mb(stats.image_bytes + stats.voice_bytes + stats.post_bytes + stats.cover_bytes + stats.wish_bytes)} МБ из 500 МБ бесплатной базы\nКлюч: ${publicKey.slice(0, 16)}…`, 200, cors);
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

            // данные спорт-версии (привычки и тренировки) — отдельная запись того же аккаунта
            if (url.pathname === '/sport' && (request.method === 'GET' || request.method === 'PUT')) {
                const user = await authUser(request, env);
                if (!user) return json({ error: 'Сессия истекла — войдите снова' }, 401, cors);
                return request.method === 'GET'
                    ? await getSportData(env, user, url, cors)
                    : await putSportData(env, user, url, request, cors);
            }

            // ProdU Team: команды, общий календарь, комментарии
            if ((request.method === 'GET' && ['/team', '/team/list', '/team/comments'].includes(url.pathname)) ||
                (request.method === 'POST' && url.pathname.startsWith('/team/'))) {
                const user = await authUser(request, env);
                if (!user) return json({ error: 'Сессия истекла — войдите снова' }, 401, cors);
                return await handleTeam(url.pathname, request, url, env, user, cors, ctx);
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

            if ((request.method === 'GET' && ['/profile', '/profile/cover', '/posts', '/posts/image', '/posts/likers'].includes(url.pathname)) ||
                (['PUT', 'DELETE'].includes(request.method) && url.pathname === '/profile/cover') ||
                (request.method === 'POST' && ['/profile/settings', '/posts/create', '/posts/delete', '/posts/like'].includes(url.pathname))) {
                const user = await authUser(request, env);
                if (!user) return json({ error: 'Сессия истекла — войдите снова' }, 401, cors);
                return await handleProfile(url.pathname, request, url, env, user, cors);
            }

            if ((request.method === 'GET' && ['/wish/lists', '/wish/list', '/wish/image', '/wish/preview'].includes(url.pathname)) ||
                (request.method === 'POST' && ['/wish/save', '/wish/delete', '/wish/token', '/wish/reset', '/wish/item/save', '/wish/item/delete', '/wish/reserve'].includes(url.pathname))) {
                const user = await authUser(request, env);
                if (!user) return json({ error: 'Сессия истекла — войдите снова' }, 401, cors);
                return await handleWish(url.pathname, request, url, env, user, cors);
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
        ctx.waitUntil(sendDue(env).then(() => sendDevQueue(env)));
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
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_posts_user ON posts (user_id, id)'),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS post_likes (
            post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, created INTEGER NOT NULL, PRIMARY KEY (post_id, user_id))`),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_post_likes_user ON post_likes (user_id)'),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS sportdata (
            user_id INTEGER PRIMARY KEY, version INTEGER NOT NULL, data TEXT, updated INTEGER NOT NULL)`),
        // ProdU Team
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, leader_id INTEGER NOT NULL, code TEXT NOT NULL UNIQUE,
            created INTEGER NOT NULL, version INTEGER NOT NULL DEFAULT 1)`),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS team_members (
            team_id INTEGER NOT NULL, user_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT '', joined INTEGER NOT NULL, PRIMARY KEY (team_id, user_id))`),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_team_members_user ON team_members (user_id)'),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS team_invites (
            team_id INTEGER NOT NULL, user_id INTEGER NOT NULL, inviter_id INTEGER NOT NULL, created INTEGER NOT NULL, PRIMARY KEY (team_id, user_id))`),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_team_invites_user ON team_invites (user_id)'),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS team_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT, team_id INTEGER NOT NULL, title TEXT NOT NULL, notes TEXT NOT NULL DEFAULT '',
            date TEXT NOT NULL, time TEXT NOT NULL DEFAULT '', dur INTEGER NOT NULL DEFAULT 0, assignee INTEGER,
            status TEXT NOT NULL DEFAULT 'todo', creator INTEGER NOT NULL, created INTEGER NOT NULL, updated INTEGER NOT NULL)`),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_team_tasks_team ON team_tasks (team_id, date)'),
        env.DB.prepare(`CREATE TABLE IF NOT EXISTS team_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER NOT NULL, team_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
            text TEXT NOT NULL, created INTEGER NOT NULL)`),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_team_comments_task ON team_comments (task_id, id)'),
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_team_comments_team ON team_comments (team_id)'),
        ...wishSchema(env),
        ...noticeSchema(env)
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
        await leaveAllTeams(env, me.id);
        await env.DB.batch([
            env.DB.prepare('DELETE FROM sessions WHERE user_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM userdata WHERE user_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM sportdata WHERE user_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM avatars WHERE user_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM friends WHERE a = ?1 OR b = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM msg_images WHERE msg_id IN (SELECT id FROM messages WHERE a = ?1 OR b = ?1)').bind(me.id),
            env.DB.prepare('DELETE FROM msg_audio WHERE msg_id IN (SELECT id FROM messages WHERE a = ?1 OR b = ?1)').bind(me.id),
            env.DB.prepare('DELETE FROM msg_wish WHERE msg_id IN (SELECT id FROM messages WHERE a = ?1 OR b = ?1)').bind(me.id),
            env.DB.prepare('DELETE FROM wish_images WHERE item_id IN (SELECT i.id FROM wish_items i JOIN wishlists w ON w.id = i.list_id WHERE w.user_id = ?1)').bind(me.id),
            env.DB.prepare('DELETE FROM wish_reserv WHERE user_id = ?1 OR list_id IN (SELECT id FROM wishlists WHERE user_id = ?1)').bind(me.id),
            env.DB.prepare('DELETE FROM wish_items WHERE list_id IN (SELECT id FROM wishlists WHERE user_id = ?1)').bind(me.id),
            env.DB.prepare('DELETE FROM wishlists WHERE user_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM messages WHERE a = ?1 OR b = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM reads WHERE user_id = ?1 OR peer_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM sub_users WHERE user_id = ?1').bind(me.id),
            env.DB.prepare('DELETE FROM post_likes WHERE user_id = ?1 OR post_id IN (SELECT id FROM posts WHERE user_id = ?1)').bind(me.id),
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

// ===================== ProdU Team: команды, общий календарь задач, комментарии =====================
const TEAM_MAX_MEMBERS = 50;
const TEAM_MAX_PER_USER = 20;
const TEAM_MAX_TASKS = 5000;
const TEAM_NAME_MAX = 60;
const TEAM_ROLE_MAX = 40;
const TASK_TITLE_MAX = 120;
const TASK_NOTES_MAX = 2000;
const COMMENT_MAX = 1000;
const TASK_STATUSES = ['todo', 'doing', 'done'];
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const TIME_RE = /^([01]\d|2[0-3]):[0-5]\d$/;
const CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

function teamCode() {
    const b = crypto.getRandomValues(new Uint8Array(8));
    return [...b].map(x => CODE_ALPHABET[x % CODE_ALPHABET.length]).join('');
}
const cleanText = (s, max) => [...String(s ?? '').replace(/\r\n?/g, '\n').trim()].slice(0, max).join('');
const oneLine = (s, max) => cleanText(String(s ?? '').replace(/\s+/g, ' '), max);

async function teamRow(env, teamId) {
    if (!Number.isSafeInteger(teamId)) throw new BadRequest('bad team');
    return env.DB.prepare('SELECT * FROM teams WHERE id = ?1').bind(teamId).first();
}
async function membership(env, teamId, userId) {
    return env.DB.prepare('SELECT role FROM team_members WHERE team_id = ?1 AND user_id = ?2').bind(teamId, userId).first();
}
const bump = (env, teamId) => env.DB.prepare('UPDATE teams SET version = version + 1 WHERE id = ?1').bind(teamId);

// Проверка доступа: команда существует и пользователь в ней. Возвращает { team, isLeader } или Response.
async function teamAccess(env, teamId, me, cors, needLeader = false) {
    const team = await teamRow(env, teamId);
    if (!team) return json({ error: 'Команда не найдена или удалена' }, 404, cors);
    const m = await membership(env, teamId, me.id);
    if (!m) return json({ error: 'Вы не состоите в этой команде' }, 403, cors);
    const isLeader = team.leader_id === me.id;
    if (needLeader && !isLeader) return json({ error: 'Это может сделать только лидер команды' }, 403, cors);
    return { team, isLeader };
}

async function handleTeam(path, request, url, env, me, cors, ctx) {
    const later = p => { if (ctx && ctx.waitUntil) ctx.waitUntil(p); };

    // --- мои команды и приглашения ---
    if (path === '/team/list') {
        const { results: teams } = await env.DB.prepare(`
            SELECT t.id, t.name, t.leader_id, t.version, m.role,
                (SELECT COUNT(*) FROM team_members x WHERE x.team_id = t.id) AS members
            FROM team_members m JOIN teams t ON t.id = m.team_id
            WHERE m.user_id = ?1 ORDER BY t.name COLLATE NOCASE`).bind(me.id).all();
        const { results: invites } = await env.DB.prepare(`
            SELECT i.team_id, t.name, u.display AS inviter, i.created
            FROM team_invites i JOIN teams t ON t.id = i.team_id LEFT JOIN users u ON u.id = i.inviter_id
            WHERE i.user_id = ?1 ORDER BY i.created DESC`).bind(me.id).all();
        return json({
            me: { id: me.id, login: me.display },
            teams: teams.map(t => ({ id: t.id, name: t.name, role: t.role, leader: t.leader_id === me.id, members: t.members, version: t.version })),
            invites
        }, 200, cors);
    }

    // --- одна команда целиком: участники, приглашённые, задачи ---
    if (path === '/team') {
        const teamId = Number(url.searchParams.get('id'));
        const acc = await teamAccess(env, teamId, me, cors);
        if (acc instanceof Response) return acc;
        const { team, isLeader } = acc;
        if (url.searchParams.get('since') === String(team.version)) return json({ unchanged: true, version: team.version }, 200, cors);
        const [members, invites, tasks] = await Promise.all([
            env.DB.prepare(`SELECT u.id, u.display AS login, m.role, m.joined, COALESCE(av.version, 0) AS av
                FROM team_members m JOIN users u ON u.id = m.user_id LEFT JOIN avatars av ON av.user_id = u.id
                WHERE m.team_id = ?1 ORDER BY m.joined`).bind(teamId).all(),
            env.DB.prepare(`SELECT u.id, u.display AS login, i.created FROM team_invites i JOIN users u ON u.id = i.user_id
                WHERE i.team_id = ?1 ORDER BY i.created`).bind(teamId).all(),
            env.DB.prepare(`SELECT t.id, t.title, t.notes, t.date, t.time, t.dur, t.assignee, t.status, t.creator, t.created, t.updated,
                (SELECT COUNT(*) FROM team_comments c WHERE c.task_id = t.id) AS comments
                FROM team_tasks t WHERE t.team_id = ?1 ORDER BY t.date, t.time`).bind(teamId).all()
        ]);
        return json({
            version: team.version,
            team: { id: team.id, name: team.name, leader: team.leader_id, code: isLeader ? team.code : null, created: team.created },
            me: { id: me.id, login: me.display },
            members: members.results, invites: invites.results, tasks: tasks.results
        }, 200, cors);
    }

    if (path === '/team/comments') {
        const teamId = Number(url.searchParams.get('team')), taskId = Number(url.searchParams.get('task'));
        const acc = await teamAccess(env, teamId, me, cors);
        if (acc instanceof Response) return acc;
        if (!Number.isSafeInteger(taskId)) throw new BadRequest('bad task');
        const { results } = await env.DB.prepare(`SELECT c.id, c.user_id AS user, COALESCE(u.display, 'удалённый аккаунт') AS login,
                COALESCE(av.version, 0) AS av, c.text, c.created
            FROM team_comments c LEFT JOIN users u ON u.id = c.user_id LEFT JOIN avatars av ON av.user_id = c.user_id
            WHERE c.team_id = ?1 AND c.task_id = ?2 ORDER BY c.id LIMIT 500`).bind(teamId, taskId).all();
        return json({ comments: results }, 200, cors);
    }

    // ---------- изменения (POST) ----------
    const body = await readJson(request, 20_000);
    const teamId = Number(body.team);

    if (path === '/team/create') {
        const name = oneLine(body.name, TEAM_NAME_MAX);
        if (!name) return json({ error: 'Введите название команды' }, 400, cors);
        const n = await env.DB.prepare('SELECT COUNT(*) AS n FROM team_members WHERE user_id = ?1').bind(me.id).first();
        if (n.n >= TEAM_MAX_PER_USER) return json({ error: `Можно состоять не более чем в ${TEAM_MAX_PER_USER} командах` }, 400, cors);
        const now = Date.now();
        const row = await env.DB.prepare('INSERT INTO teams (name, leader_id, code, created, version) VALUES (?1, ?2, ?3, ?4, 1) RETURNING id')
            .bind(name, me.id, teamCode(), now).first();
        await env.DB.prepare("INSERT INTO team_members (team_id, user_id, role, joined) VALUES (?1, ?2, 'Лидер', ?3)").bind(row.id, me.id, now).run();
        return json({ id: row.id }, 200, cors);
    }

    if (path === '/team/join') {
        const code = String(body.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
        if (code.length !== 8) return json({ error: 'Код состоит из 8 символов' }, 400, cors);
        const team = await env.DB.prepare('SELECT id, name FROM teams WHERE code = ?1').bind(code).first();
        if (!team) return json({ error: 'Команда с таким кодом не найдена' }, 404, cors);
        return await addMember(env, team, me, cors);
    }

    if (path === '/team/invite/respond') {
        const inv = await env.DB.prepare('SELECT 1 FROM team_invites WHERE team_id = ?1 AND user_id = ?2').bind(teamId, me.id).first();
        if (!inv) return json({ error: 'Приглашение не найдено — возможно, его отменили' }, 404, cors);
        await env.DB.prepare('DELETE FROM team_invites WHERE team_id = ?1 AND user_id = ?2').bind(teamId, me.id).run();
        const team = await teamRow(env, teamId);
        if (!body.accept || !team) { if (team) await bump(env, teamId).run(); return json({ ok: true }, 200, cors); }
        return await addMember(env, team, me, cors);
    }

    // всё остальное — только для участников команды
    const leaderOnly = ['/team/update', '/team/delete', '/team/invite', '/team/invite/cancel', '/team/code', '/team/member', '/team/member/remove', '/team/leader'].includes(path);
    const acc = await teamAccess(env, teamId, me, cors, leaderOnly);
    if (acc instanceof Response) return acc;
    const { team, isLeader } = acc;
    const now = Date.now();

    if (path === '/team/update') {
        const name = oneLine(body.name, TEAM_NAME_MAX);
        if (!name) return json({ error: 'Введите название команды' }, 400, cors);
        await env.DB.batch([env.DB.prepare('UPDATE teams SET name = ?1 WHERE id = ?2').bind(name, teamId), bump(env, teamId)]);
        return json({ ok: true }, 200, cors);
    }
    if (path === '/team/delete') {
        await deleteTeamStatements(env, teamId);
        return json({ ok: true }, 200, cors);
    }
    if (path === '/team/code') {
        const code = teamCode();
        await env.DB.batch([env.DB.prepare('UPDATE teams SET code = ?1 WHERE id = ?2').bind(code, teamId), bump(env, teamId)]);
        return json({ code }, 200, cors);
    }
    if (path === '/team/invite') {
        const login = normLogin(body.login).toLowerCase();
        const user = await env.DB.prepare('SELECT id, display FROM users WHERE login = ?1').bind(login).first();
        if (!user) return json({ error: 'Пользователь с таким никнеймом не найден' }, 404, cors);
        if (await membership(env, teamId, user.id)) return json({ error: `${user.display} уже в команде` }, 400, cors);
        const cnt = await env.DB.prepare('SELECT (SELECT COUNT(*) FROM team_members WHERE team_id = ?1) + (SELECT COUNT(*) FROM team_invites WHERE team_id = ?1) AS n').bind(teamId).first();
        if (cnt.n >= TEAM_MAX_MEMBERS) return json({ error: `В команде может быть не больше ${TEAM_MAX_MEMBERS} человек` }, 400, cors);
        await env.DB.batch([
            env.DB.prepare('INSERT OR IGNORE INTO team_invites (team_id, user_id, inviter_id, created) VALUES (?1, ?2, ?3, ?4)').bind(teamId, user.id, me.id, now),
            bump(env, teamId)
        ]);
        later(pushToUser(env, user.id, { kind: 'team', title: 'Приглашение в команду', body: `${me.display} зовёт вас в «${team.name}»`, tag: 'team-inv-' + teamId, url: './team.html' }));
        return json({ ok: true, user: { id: user.id, login: user.display } }, 200, cors);
    }
    if (path === '/team/invite/cancel') {
        await env.DB.batch([env.DB.prepare('DELETE FROM team_invites WHERE team_id = ?1 AND user_id = ?2').bind(teamId, Number(body.user)), bump(env, teamId)]);
        return json({ ok: true }, 200, cors);
    }
    if (path === '/team/member') {
        const uid = Number(body.user);
        if (!(await membership(env, teamId, uid))) return json({ error: 'Участник не найден' }, 404, cors);
        const role = oneLine(body.role, TEAM_ROLE_MAX);
        await env.DB.batch([env.DB.prepare('UPDATE team_members SET role = ?1 WHERE team_id = ?2 AND user_id = ?3').bind(role, teamId, uid), bump(env, teamId)]);
        return json({ ok: true }, 200, cors);
    }
    if (path === '/team/member/remove' || path === '/team/leave') {
        const uid = path === '/team/leave' ? me.id : Number(body.user);
        if (uid === team.leader_id) {
            const others = await env.DB.prepare('SELECT COUNT(*) AS n FROM team_members WHERE team_id = ?1 AND user_id != ?2').bind(teamId, uid).first();
            if (others.n) return json({ error: 'Сначала передайте лидерство другому участнику' }, 400, cors);
            await deleteTeamStatements(env, teamId);          // лидер был последним — команда удаляется
            return json({ ok: true, deleted: true }, 200, cors);
        }
        await env.DB.batch([
            env.DB.prepare('DELETE FROM team_members WHERE team_id = ?1 AND user_id = ?2').bind(teamId, uid),
            env.DB.prepare('UPDATE team_tasks SET assignee = NULL, updated = ?3 WHERE team_id = ?1 AND assignee = ?2').bind(teamId, uid, now),
            bump(env, teamId)
        ]);
        return json({ ok: true }, 200, cors);
    }
    if (path === '/team/leader') {
        const uid = Number(body.user);
        if (!(await membership(env, teamId, uid))) return json({ error: 'Участник не найден' }, 404, cors);
        await env.DB.batch([env.DB.prepare('UPDATE teams SET leader_id = ?1 WHERE id = ?2').bind(uid, teamId), bump(env, teamId)]);
        return json({ ok: true }, 200, cors);
    }

    // --- задачи ---
    if (path === '/team/task/save') {
        const t = body.task || {};
        const id = t.id == null ? null : Number(t.id);
        const old = id ? await env.DB.prepare('SELECT * FROM team_tasks WHERE id = ?1 AND team_id = ?2').bind(id, teamId).first() : null;
        if (id && !old) return json({ error: 'Задача не найдена — возможно, её удалили' }, 404, cors);
        const status = TASK_STATUSES.includes(t.status) ? t.status : 'todo';
        const fullEdit = !old || isLeader || old.creator === me.id;
        if (old && !fullEdit) {
            // исполнитель может менять только статус
            if (old.assignee !== me.id) return json({ error: 'Изменять задачу могут автор, исполнитель (статус) и лидер' }, 403, cors);
            await env.DB.batch([env.DB.prepare('UPDATE team_tasks SET status = ?1, updated = ?2 WHERE id = ?3').bind(status, now, id), bump(env, teamId)]);
            return json({ id }, 200, cors);
        }
        const title = oneLine(t.title, TASK_TITLE_MAX);
        if (!title) return json({ error: 'Введите название задачи' }, 400, cors);
        if (!DATE_RE.test(String(t.date))) throw new BadRequest('bad date');
        const time = t.time ? String(t.time) : '';
        if (time && !TIME_RE.test(time)) throw new BadRequest('bad time');
        const dur = Math.round(Number(t.dur) || 0);
        if (dur < 0 || dur > 60 * 24 * 60) return json({ error: 'Длительность — от 0 минут до 60 дней' }, 400, cors);
        let assignee = t.assignee == null || t.assignee === '' ? null : Number(t.assignee);
        if (assignee !== null && !(await membership(env, teamId, assignee))) return json({ error: 'Исполнитель должен быть участником команды' }, 400, cors);
        const notes = cleanText(t.notes, TASK_NOTES_MAX);
        let newId = id;
        if (old) {
            await env.DB.batch([
                env.DB.prepare('UPDATE team_tasks SET title = ?1, notes = ?2, date = ?3, time = ?4, dur = ?5, assignee = ?6, status = ?7, updated = ?8 WHERE id = ?9')
                    .bind(title, notes, t.date, time, dur, assignee, status, now, id),
                bump(env, teamId)
            ]);
        } else {
            const cnt = await env.DB.prepare('SELECT COUNT(*) AS n FROM team_tasks WHERE team_id = ?1').bind(teamId).first();
            if (cnt.n >= TEAM_MAX_TASKS) return json({ error: 'В команде слишком много задач — удалите старые' }, 400, cors);
            const row = await env.DB.prepare(`INSERT INTO team_tasks (team_id, title, notes, date, time, dur, assignee, status, creator, created, updated)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10) RETURNING id`).bind(teamId, title, notes, t.date, time, dur, assignee, status, me.id, now).first();
            newId = row.id;
            await bump(env, teamId).run();
        }
        if (assignee && assignee !== me.id && (!old || old.assignee !== assignee)) {
            later(pushToUser(env, assignee, { kind: 'team', title: `Новая задача · ${team.name}`, body: `${me.display}: ${title}`, tag: 'team-task-' + newId, url: './team.html' }));
        }
        return json({ id: newId }, 200, cors);
    }
    if (path === '/team/task/delete') {
        const id = Number(body.id);
        const old = await env.DB.prepare('SELECT creator FROM team_tasks WHERE id = ?1 AND team_id = ?2').bind(id, teamId).first();
        if (!old) return json({ ok: true }, 200, cors);
        if (!isLeader && old.creator !== me.id) return json({ error: 'Удалить задачу могут её автор и лидер' }, 403, cors);
        await env.DB.batch([
            env.DB.prepare('DELETE FROM team_comments WHERE task_id = ?1').bind(id),
            env.DB.prepare('DELETE FROM team_tasks WHERE id = ?1').bind(id),
            bump(env, teamId)
        ]);
        return json({ ok: true }, 200, cors);
    }

    // --- комментарии ---
    if (path === '/team/comment') {
        const taskId = Number(body.task);
        const task = await env.DB.prepare('SELECT id, title, assignee, creator FROM team_tasks WHERE id = ?1 AND team_id = ?2').bind(taskId, teamId).first();
        if (!task) return json({ error: 'Задача не найдена — возможно, её удалили' }, 404, cors);
        const textBody = cleanText(body.text, COMMENT_MAX);
        if (!textBody) return json({ error: 'Пустой комментарий' }, 400, cors);
        const recent = await env.DB.prepare('SELECT COUNT(*) AS n FROM team_comments WHERE user_id = ?1 AND created > ?2').bind(me.id, now - 60_000).first();
        if (recent.n >= 30) return json({ error: 'Слишком часто — подождите минуту' }, 429, cors);
        const row = await env.DB.prepare('INSERT INTO team_comments (task_id, team_id, user_id, text, created) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING id')
            .bind(taskId, teamId, me.id, textBody, now).first();
        await bump(env, teamId).run();
        const notify = new Set([task.assignee, task.creator].filter(u => u && u !== me.id));
        const preview = [...textBody].slice(0, 120).join('') + ([...textBody].length > 120 ? '…' : '');
        notify.forEach(u => later(pushToUser(env, u, { kind: 'team', title: `${me.display} · ${task.title}`, body: preview, tag: 'team-cm-' + taskId, url: './team.html' })));
        return json({ comment: { id: row.id, user: me.id, login: me.display, text: textBody, created: now } }, 200, cors);
    }
    if (path === '/team/comment/delete') {
        const id = Number(body.id);
        const c = await env.DB.prepare('SELECT user_id FROM team_comments WHERE id = ?1 AND team_id = ?2').bind(id, teamId).first();
        if (!c) return json({ ok: true }, 200, cors);
        if (!isLeader && c.user_id !== me.id) return json({ error: 'Удалить комментарий могут его автор и лидер' }, 403, cors);
        await env.DB.batch([env.DB.prepare('DELETE FROM team_comments WHERE id = ?1').bind(id), bump(env, teamId)]);
        return json({ ok: true }, 200, cors);
    }

    return json({ error: 'not found' }, 404, cors);
}

async function addMember(env, team, me, cors) {
    if (await membership(env, team.id, me.id)) return json({ id: team.id, already: true }, 200, cors);
    const [n, mine] = await Promise.all([
        env.DB.prepare('SELECT COUNT(*) AS n FROM team_members WHERE team_id = ?1').bind(team.id).first(),
        env.DB.prepare('SELECT COUNT(*) AS n FROM team_members WHERE user_id = ?1').bind(me.id).first()
    ]);
    if (n.n >= TEAM_MAX_MEMBERS) return json({ error: `В команде уже ${TEAM_MAX_MEMBERS} человек — это максимум` }, 400, cors);
    if (mine.n >= TEAM_MAX_PER_USER) return json({ error: `Можно состоять не более чем в ${TEAM_MAX_PER_USER} командах` }, 400, cors);
    await env.DB.batch([
        env.DB.prepare("INSERT INTO team_members (team_id, user_id, role, joined) VALUES (?1, ?2, '', ?3)").bind(team.id, me.id, Date.now()),
        env.DB.prepare('DELETE FROM team_invites WHERE team_id = ?1 AND user_id = ?2').bind(team.id, me.id),
        bump(env, team.id)
    ]);
    return json({ id: team.id }, 200, cors);
}

async function deleteTeamStatements(env, teamId) {
    await env.DB.batch([
        env.DB.prepare('DELETE FROM team_comments WHERE team_id = ?1').bind(teamId),
        env.DB.prepare('DELETE FROM team_tasks WHERE team_id = ?1').bind(teamId),
        env.DB.prepare('DELETE FROM team_invites WHERE team_id = ?1').bind(teamId),
        env.DB.prepare('DELETE FROM team_members WHERE team_id = ?1').bind(teamId),
        env.DB.prepare('DELETE FROM teams WHERE id = ?1').bind(teamId)
    ]);
}

// при удалении аккаунта: команды, где он лидер, переходят старейшему участнику (или удаляются, если он был один)
async function leaveAllTeams(env, userId) {
    const { results } = await env.DB.prepare('SELECT id FROM teams WHERE leader_id = ?1').bind(userId).all();
    for (const t of results) {
        const next = await env.DB.prepare('SELECT user_id FROM team_members WHERE team_id = ?1 AND user_id != ?2 ORDER BY joined LIMIT 1').bind(t.id, userId).first();
        if (next) await env.DB.prepare('UPDATE teams SET leader_id = ?1, version = version + 1 WHERE id = ?2').bind(next.user_id, t.id).run();
        else await deleteTeamStatements(env, t.id);
    }
    await env.DB.batch([
        env.DB.prepare('UPDATE teams SET version = version + 1 WHERE id IN (SELECT team_id FROM team_members WHERE user_id = ?1)').bind(userId),
        env.DB.prepare('UPDATE team_tasks SET assignee = NULL WHERE assignee = ?1').bind(userId),
        env.DB.prepare('DELETE FROM team_comments WHERE user_id = ?1').bind(userId),
        env.DB.prepare('DELETE FROM team_invites WHERE user_id = ?1 OR inviter_id = ?1').bind(userId),
        env.DB.prepare('DELETE FROM team_members WHERE user_id = ?1').bind(userId)
    ]);
}

// ===================== Спорт-версия =====================
// Так же, как основные данные: сервер хранит строку как есть, объединяет изменения приложение.
async function getSportData(env, user, url, cors) {
    await ensureSchema(env);
    const row = await env.DB.prepare('SELECT version, data FROM sportdata WHERE user_id = ?1').bind(user.id).first();
    const version = row ? row.version : 0;
    if (url.searchParams.get('since') === String(version)) return json({ version, unchanged: true }, 200, cors);
    return json({ version, data: row ? row.data : null }, 200, cors);
}

async function putSportData(env, user, url, request, cors) {
    await ensureSchema(env);
    const base = Number(url.searchParams.get('base'));
    if (!Number.isSafeInteger(base) || base < 0) throw new BadRequest('bad base');
    const raw = await request.text();
    if (enc(raw).length > MAX_DATA_BYTES) {
        return json({ error: 'Слишком много данных спорт-версии (больше 1,8 МБ)' }, 413, cors);
    }
    if (!raw.startsWith('{') || !raw.endsWith('}')) throw new BadRequest('bad data');
    const res = await env.DB.prepare(`INSERT INTO sportdata (user_id, version, data, updated) VALUES (?1, 1, ?2, ?3)
        ON CONFLICT(user_id) DO UPDATE SET data = excluded.data, version = sportdata.version + 1, updated = excluded.updated
        WHERE sportdata.version = ?4`).bind(user.id, raw, Date.now(), base).run();
    if (res.meta && res.meta.changes === 1) return json({ version: base + 1 }, 200, cors);
    const row = await env.DB.prepare('SELECT version FROM sportdata WHERE user_id = ?1').bind(user.id).first();
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
                (SELECT wl.title FROM msg_wish mw JOIN wishlists wl ON wl.id = mw.list_id WHERE mw.msg_id = m.id) AS wish_title,
                EXISTS(SELECT 1 FROM msg_wish mw WHERE mw.msg_id = m.id) AS wish,
                (SELECT dur FROM msg_audio au WHERE au.msg_id = m.id) AS vdur FROM messages m
            JOIN (SELECT MAX(id) AS id FROM messages WHERE a = ?1 OR b = ?1 GROUP BY a, b) t ON t.id = m.id`).bind(me.id).all();
        const { results: unread } = await env.DB.prepare(`
            SELECT m.sender AS id, COUNT(*) AS n FROM messages m
            LEFT JOIN reads r ON r.user_id = ?1 AND r.peer_id = m.sender
            WHERE (m.a = ?1 OR m.b = ?1) AND m.sender != ?1 AND m.id > COALESCE(r.last_id, 0)
            GROUP BY m.sender`).bind(me.id).all();

        const lastBy = new Map(last.map(m => [m.a === me.id ? m.b : m.a, { id: m.id, sender: m.sender, body: m.body.slice(0, 200), created: m.created, img: !!m.img, voice: m.vdur ? { dur: m.vdur } : null, wish: m.wish ? { title: m.wish_title || '' } : null }]));
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
                `SELECT m.id, m.sender, m.body, m.created, i.w, i.h, au.dur, au.peaks, mw.list_id AS wish_id, wl.title AS wish_title, wl.emoji AS wish_emoji, wl.vis AS wish_vis, wl.user_id AS wish_owner,
                    (SELECT COUNT(*) FROM wish_items wi WHERE wi.list_id = wl.id) AS wish_n FROM messages m
                 LEFT JOIN msg_images i ON i.msg_id = m.id LEFT JOIN msg_audio au ON au.msg_id = m.id
                 LEFT JOIN msg_wish mw ON mw.msg_id = m.id LEFT JOIN wishlists wl ON wl.id = mw.list_id
                 WHERE m.a = ?1 AND m.b = ?2 AND m.id > ?3 ORDER BY m.id LIMIT 200`
            ).bind(a, b, after).all());
        } else {
            const { results } = await env.DB.prepare(
                `SELECT m.id, m.sender, m.body, m.created, i.w, i.h, au.dur, au.peaks, mw.list_id AS wish_id, wl.title AS wish_title, wl.emoji AS wish_emoji, wl.vis AS wish_vis, wl.user_id AS wish_owner,
                    (SELECT COUNT(*) FROM wish_items wi WHERE wi.list_id = wl.id) AS wish_n FROM messages m
                 LEFT JOIN msg_images i ON i.msg_id = m.id LEFT JOIN msg_audio au ON au.msg_id = m.id
                 LEFT JOIN msg_wish mw ON mw.msg_id = m.id LEFT JOIN wishlists wl ON wl.id = mw.list_id
                 WHERE m.a = ?1 AND m.b = ?2 AND m.id < ?3 ORDER BY m.id DESC LIMIT 51`
            ).bind(a, b, before || Number.MAX_SAFE_INTEGER).all();
            hasMore = results.length > 50;
            messages = results.slice(0, 50).reverse();
        }
        messages = messages.map(m => {
            const out = { id: m.id, sender: m.sender, body: m.body, created: m.created };
            if (m.w) out.img = { w: m.w, h: m.h };
            if (m.dur) out.voice = { dur: m.dur, peaks: m.peaks };
            // открытка вишлиста: если список удалили или закрыли — показываем, что он недоступен
            if (m.wish_id) out.wish = m.wish_title != null && (m.wish_vis !== 'private' || m.wish_owner === me.id)
                ? { id: m.wish_id, title: m.wish_title, emoji: m.wish_emoji, count: m.wish_n } : { id: m.wish_id, gone: true };
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
        const { to, text, image, voice, wish } = await readJson(request, Math.max(MAX_IMAGE_CHARS, MAX_VOICE_CHARS) + 30_000);
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
        let wl = null;
        if (wish) {
            if (img || vc) throw new BadRequest('Вишлист отправляется отдельным сообщением');
            wl = await wishList(env, readUserId(wish));
            if (!wl || wl.user_id !== me.id) return json({ error: 'Список не найден' }, 404, cors);
            if (wl.vis === 'private') return json({ error: 'Этот список виден только вам — откройте его друзьям, чтобы поделиться' }, 403, cors);
        }
        if (!body && !img && !vc && !wl) throw new BadRequest('Пустое сообщение');
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
        if (img || vc || wl) {
            try {
                if (wl) await env.DB.prepare('INSERT INTO msg_wish (msg_id, list_id) VALUES (?1, ?2)').bind(row.id, wl.id).run();
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
        if (wl) message.wish = { id: wl.id, title: wl.title, emoji: wl.emoji, count: (await env.DB.prepare('SELECT COUNT(*) AS n FROM wish_items WHERE list_id = ?1').bind(wl.id).first()).n };
        const textPreview = [...body].slice(0, 140).join('') + ([...body].length > 140 ? '…' : '');
        const fmtDur = ms => { const sec = Math.round(ms / 1000); return `${Math.floor(sec / 60)}:${String(sec % 60).padStart(2, '0')}`; };
        const preview = wl ? `🎁 Вишлист «${wl.title}»` + (textPreview ? ' · ' + textPreview : '') : vc ? `🎤 Голосовое (${fmtDur(vc.dur)})`
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
        const { results } = await env.DB.prepare(`SELECT p.id, p.caption, p.w, p.h, p.created,
                (SELECT COUNT(*) FROM post_likes l WHERE l.post_id = p.id) AS likes,
                EXISTS(SELECT 1 FROM post_likes l WHERE l.post_id = p.id AND l.user_id = ?4) AS liked
            FROM posts p WHERE p.user_id = ?1 AND p.id < ?2 ORDER BY p.id DESC LIMIT ?3`).bind(uid, before, POSTS_PAGE + 1, me.id).all();
        const posts = results.slice(0, POSTS_PAGE).map(p => ({ ...p, liked: !!p.liked }));
        return json({ posts, hasMore: results.length > POSTS_PAGE }, 200, cors);
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
        return json({ post: { id: row.id, caption: text, w, h, created: now, likes: 0, liked: false } }, 200, cors);
    }

    if (path === '/posts/delete') {
        const id = readUserId((await readJson(request)).id);
        const r = await env.DB.prepare('DELETE FROM posts WHERE id = ?1 AND user_id = ?2').bind(id, me.id).run();
        const changed = r && r.meta ? r.meta.changes : 1;
        if (!changed) return json({ error: 'Пост не найден' }, 404, cors);
        await env.DB.prepare('DELETE FROM post_likes WHERE post_id = ?1').bind(id).run();
        return json({ ok: true }, 200, cors);
    }

    // --- лайки: поставить можно любому посту, который вы видите (и своему тоже) ---
    if (path === '/posts/like') {
        const body = await readJson(request);
        const id = readUserId(body.id);
        const post = await env.DB.prepare('SELECT user_id FROM posts WHERE id = ?1').bind(id).first();
        if (!post || !(await postsVisibleTo(env, me.id, post.user_id))) return json({ error: 'Пост не найден' }, 404, cors);
        if (body.like) {
            await env.DB.prepare('INSERT OR IGNORE INTO post_likes (post_id, user_id, created) VALUES (?1, ?2, ?3)').bind(id, me.id, Date.now()).run();
        } else {
            await env.DB.prepare('DELETE FROM post_likes WHERE post_id = ?1 AND user_id = ?2').bind(id, me.id).run();
        }
        const c = await env.DB.prepare('SELECT COUNT(*) AS n FROM post_likes WHERE post_id = ?1').bind(id).first();
        return json({ likes: c.n, liked: !!body.like }, 200, cors);
    }

    if (path === '/posts/likers') {
        const id = readUserId(url.searchParams.get('id'));
        const post = await env.DB.prepare('SELECT user_id FROM posts WHERE id = ?1').bind(id).first();
        if (!post || !(await postsVisibleTo(env, me.id, post.user_id))) return json({ error: 'Пост не найден' }, 404, cors);
        const { results } = await env.DB.prepare(`SELECT u.id, u.display AS login, COALESCE(av.version, 0) AS av
            FROM post_likes l JOIN users u ON u.id = l.user_id LEFT JOIN avatars av ON av.user_id = u.id
            WHERE l.post_id = ?1 ORDER BY l.created DESC LIMIT 200`).bind(id).all();
        return json({ users: results }, 200, cors);
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
// ===================== Вишлисты =====================
// Список хранится на сервере (как посты): его можно показать друзьям в ProdU и открыть по ссылке /w/<токен> без аккаунта.
// Брони подарков («Я подарю») видят гости и друзья, но не владелец — чтобы не испортить сюрприз.
const WISH_LISTS_MAX = 50;
const WISH_ITEMS_MAX = 200;
const WISH_IMAGE_CHARS = 700_000;     // приложение сжимает фото до 900 px — обычно 60–200 КБ
const WISH_IMAGES_PER_DAY = 150;
const WISH_TITLE = 120, WISH_NOTE = 1000, WISH_URL = 2000, WISH_NAME = 40;
const WISH_VIS = ['private', 'friends', 'link'];
const WISH_ANON_PER_HOUR = 30;        // броней от гостей по ссылке на один список в час
const WISH_PREVIEW_HTML = 600_000;    // сколько читаем со страницы товара
const WISH_PREVIEW_IMG = 2_000_000;

const wishSchema = env => [
    env.DB.prepare(`CREATE TABLE IF NOT EXISTS wishlists (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, title TEXT NOT NULL, note TEXT NOT NULL DEFAULT '',
        emoji TEXT NOT NULL DEFAULT '🎁', vis TEXT NOT NULL DEFAULT 'friends', token TEXT NOT NULL UNIQUE, date TEXT NOT NULL DEFAULT '',
        created INTEGER NOT NULL, updated INTEGER NOT NULL)`),
    env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_wishlists_user ON wishlists (user_id)'),
    env.DB.prepare(`CREATE TABLE IF NOT EXISTS wish_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT, list_id INTEGER NOT NULL, title TEXT NOT NULL, url TEXT NOT NULL DEFAULT '',
        price INTEGER NOT NULL DEFAULT 0, note TEXT NOT NULL DEFAULT '', want INTEGER NOT NULL DEFAULT 0, pos INTEGER NOT NULL DEFAULT 0,
        img_v INTEGER NOT NULL DEFAULT 0, w INTEGER NOT NULL DEFAULT 0, h INTEGER NOT NULL DEFAULT 0, created INTEGER NOT NULL, updated INTEGER NOT NULL)`),
    env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_wish_items_list ON wish_items (list_id, pos)'),
    env.DB.prepare('CREATE TABLE IF NOT EXISTS wish_images (item_id INTEGER PRIMARY KEY, data TEXT NOT NULL)'),
    env.DB.prepare(`CREATE TABLE IF NOT EXISTS wish_reserv (
        item_id INTEGER PRIMARY KEY, list_id INTEGER NOT NULL, user_id INTEGER, name TEXT NOT NULL DEFAULT '', secret TEXT NOT NULL DEFAULT '', created INTEGER NOT NULL)`),
    env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_wish_reserv_list ON wish_reserv (list_id, created)'),
    env.DB.prepare('CREATE TABLE IF NOT EXISTS msg_wish (msg_id INTEGER PRIMARY KEY, list_id INTEGER NOT NULL)')
];

const wishText = (v, max) => {
    const s = String(v ?? '').replace(/\r\n?/g, '\n').replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, '').trim();
    if ([...s].length > max) throw new BadRequest(`Слишком длинный текст (больше ${max} символов)`);
    return s;
};
function cleanUrl(v) {
    const s = String(v ?? '').trim();
    if (!s) return '';
    if (s.length > WISH_URL) throw new BadRequest('Слишком длинная ссылка');
    let u;
    try { u = new URL(/^[a-z][a-z0-9+.-]*:/i.test(s) ? s : 'https://' + s); } catch { throw new BadRequest('Ссылка выглядит неправильно'); }
    if (u.protocol !== 'https:' && u.protocol !== 'http:') throw new BadRequest('Ссылка должна начинаться с http:// или https://');
    return u.href;
}
const cleanEmoji = v => { const s = [...String(v || '🎁').trim()].slice(0, 4).join(''); return s || '🎁'; };

async function wishList(env, id) {
    return env.DB.prepare('SELECT * FROM wishlists WHERE id = ?1').bind(id).first();
}
// кто что может: владелец — всё; друг — смотреть списки «для друзей» и «по ссылке»; остальные — только по токену
async function wishAccess(env, meId, list, token) {
    if (!list) return 'none';
    if (meId && list.user_id === meId) return 'owner';
    if (list.vis === 'private') return 'none';
    if (list.vis === 'link' && token && sameString(token, list.token)) return 'guest';
    if (meId && relation(await getFriendRow(env, meId, list.user_id), meId) === 'friend') return 'friend';
    return 'none';
}
function listOut(l, extra = {}) {
    return { id: l.id, title: l.title, note: l.note, emoji: l.emoji, vis: l.vis, date: l.date, created: l.created, updated: l.updated, ...extra };
}
const itemOut = i => ({ id: i.id, title: i.title, url: i.url, price: i.price, note: i.note, want: !!i.want, pos: i.pos, img: i.img_v ? { v: i.img_v, w: i.w, h: i.h } : null });

async function wishItemsWithReserv(env, listId, viewerId, showReserv) {
    const { results } = await env.DB.prepare(`SELECT i.*, r.user_id AS r_user, r.name AS r_name, r.created AS r_at, (r.item_id IS NOT NULL) AS r_on
        FROM wish_items i LEFT JOIN wish_reserv r ON r.item_id = i.id WHERE i.list_id = ?1 ORDER BY i.want DESC, i.pos, i.id`).bind(listId).all();
    return results.map(i => {
        const o = itemOut(i);
        if (showReserv && i.r_on) o.reserved = { mine: !!viewerId && i.r_user === viewerId, name: i.r_name || '', anon: i.r_user == null };
        return o;
    });
}

async function handleWish(path, request, url, env, me, cors) {
    // --- списки человека: свои — все, друга — те, что открыты друзьям ---
    if (path === '/wish/lists') {
        const uid = url.searchParams.has('user') ? readUserId(url.searchParams.get('user')) : me.id;
        let where = 'w.user_id = ?1';
        if (uid !== me.id) {
            if (relation(await getFriendRow(env, me.id, uid), me.id) !== 'friend') return json({ lists: [], hidden: true }, 200, cors);
            where += " AND w.vis != 'private'";
        }
        const { results } = await env.DB.prepare(`SELECT w.*, (SELECT COUNT(*) FROM wish_items i WHERE i.list_id = w.id) AS n,
                (SELECT i.id || ':' || i.img_v FROM wish_items i WHERE i.list_id = w.id AND i.img_v > 0 ORDER BY i.want DESC, i.pos, i.id LIMIT 1) AS cover
            FROM wishlists w WHERE ${where} ORDER BY w.updated DESC`).bind(uid).all();
        return json({ lists: results.map(l => listOut(l, { count: l.n, cover: l.cover || '', ...(uid === me.id ? { token: l.token } : {}) })) }, 200, cors);
    }

    // --- один список с подарками ---
    if (path === '/wish/list') {
        const token = url.searchParams.get('t') || '';
        const list = token ? await env.DB.prepare('SELECT * FROM wishlists WHERE token = ?1').bind(token).first()
            : await wishList(env, readUserId(url.searchParams.get('id')));
        const access = await wishAccess(env, me.id, list, token);
        if (access === 'none') return json({ error: 'Список не найден или закрыт владельцем' }, 404, cors);
        const owner = await env.DB.prepare('SELECT u.id, u.display AS login, COALESCE(av.version, 0) AS av FROM users u LEFT JOIN avatars av ON av.user_id = u.id WHERE u.id = ?1').bind(list.user_id).first();
        const items = await wishItemsWithReserv(env, list.id, me.id, access !== 'owner');
        return json({ list: listOut(list, access === 'owner' ? { token: list.token } : {}), owner, access, items }, 200, cors);
    }

    if (path === '/wish/image') {
        const id = readUserId(url.searchParams.get('id'));
        const row = await env.DB.prepare('SELECT w.*, d.data FROM wish_items i JOIN wishlists w ON w.id = i.list_id JOIN wish_images d ON d.item_id = i.id WHERE i.id = ?1').bind(id).first();
        if (!row || (await wishAccess(env, me.id, row, url.searchParams.get('t') || '')) === 'none') return json({ error: 'Картинка не найдена' }, 404, cors);
        return new Response(JSON.stringify({ data: row.data }), {
            status: 200, headers: { ...cors, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'private, max-age=31536000, immutable' }
        });
    }

    // --- создать / изменить список ---
    if (path === '/wish/save') {
        const b = await readJson(request);
        const title = wishText(b.title, WISH_TITLE);
        if (!title) throw new BadRequest('Назовите список');
        const note = wishText(b.note, WISH_NOTE);
        const vis = WISH_VIS.includes(b.vis) ? b.vis : 'friends';
        const date = /^\d{4}-\d{2}-\d{2}$/.test(String(b.date || '')) ? b.date : '';
        const emoji = cleanEmoji(b.emoji);
        const now = Date.now();
        if (b.id) {
            const id = readUserId(b.id);
            const r = await env.DB.prepare('UPDATE wishlists SET title = ?1, note = ?2, vis = ?3, date = ?4, emoji = ?5, updated = ?6 WHERE id = ?7 AND user_id = ?8')
                .bind(title, note, vis, date, emoji, now, id, me.id).run();
            if (r && r.meta && !r.meta.changes) return json({ error: 'Список не найден' }, 404, cors);
            return json({ list: listOut(await wishList(env, id), { token: (await wishList(env, id)).token }) }, 200, cors);
        }
        const c = await env.DB.prepare('SELECT COUNT(*) AS n FROM wishlists WHERE user_id = ?1').bind(me.id).first();
        if (c.n >= WISH_LISTS_MAX) return json({ error: `Можно завести не больше ${WISH_LISTS_MAX} списков` }, 429, cors);
        const token = randomB64u(12);
        const row = await env.DB.prepare(`INSERT INTO wishlists (user_id, title, note, emoji, vis, token, date, created, updated)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8) RETURNING *`).bind(me.id, title, note, emoji, vis, token, date, now).first();
        return json({ list: listOut(row, { token, count: 0, cover: '' }) }, 200, cors);
    }

    if (path === '/wish/delete' || path === '/wish/token' || path === '/wish/reset') {
        const id = readUserId((await readJson(request)).id);
        const list = await wishList(env, id);
        if (!list || list.user_id !== me.id) return json({ error: 'Список не найден' }, 404, cors);
        if (path === '/wish/token') {
            // новая ссылка: старая перестаёт открываться
            const token = randomB64u(12);
            await env.DB.prepare('UPDATE wishlists SET token = ?1 WHERE id = ?2').bind(token, id).run();
            return json({ token }, 200, cors);
        }
        if (path === '/wish/reset') {
            await env.DB.prepare('DELETE FROM wish_reserv WHERE list_id = ?1').bind(id).run();
            return json({ ok: true }, 200, cors);
        }
        await env.DB.batch([
            env.DB.prepare('DELETE FROM wish_images WHERE item_id IN (SELECT id FROM wish_items WHERE list_id = ?1)').bind(id),
            env.DB.prepare('DELETE FROM wish_reserv WHERE list_id = ?1').bind(id),
            env.DB.prepare('DELETE FROM wish_items WHERE list_id = ?1').bind(id),
            env.DB.prepare('DELETE FROM wishlists WHERE id = ?1').bind(id)
        ]);
        return json({ ok: true }, 200, cors);
    }

    // --- подарок: создать / изменить (image: {data,w,h} — новое фото, null — убрать, нет поля — не трогать) ---
    if (path === '/wish/item/save') {
        const b = await readJson(request, WISH_IMAGE_CHARS + 20_000);
        const title = wishText(b.title, WISH_TITLE);
        if (!title) throw new BadRequest('Напишите, что это за подарок');
        const note = wishText(b.note, WISH_NOTE);
        const link = cleanUrl(b.url);
        const price = Math.round(Number(b.price) || 0);
        if (price < 0 || price > 1e10) throw new BadRequest('Неверная цена');
        const want = b.want ? 1 : 0;
        let img;
        if (b.image) {
            const data = String(b.image.data || ''), w = Number(b.image.w), h = Number(b.image.h);
            if (data.length > WISH_IMAGE_CHARS) return json({ error: 'Картинка слишком большая' }, 413, cors);
            if (!IMAGE_RE.test(data)) throw new BadRequest('Это не похоже на картинку JPEG');
            if (!Number.isInteger(w) || !Number.isInteger(h) || w < 1 || h < 1 || w > 4096 || h > 4096) throw new BadRequest('bad size');
            const today = await env.DB.prepare(`SELECT COUNT(*) AS n FROM wish_items i JOIN wishlists w ON w.id = i.list_id
                WHERE w.user_id = ?1 AND i.img_v > ?2`).bind(me.id, Date.now() - 86400e3).first();
            if (today.n >= WISH_IMAGES_PER_DAY) return json({ error: `Можно загрузить не больше ${WISH_IMAGES_PER_DAY} фото в сутки` }, 429, cors);
            img = { data, w, h };
        } else if (b.image === null) img = null;
        const now = Date.now();
        let item;
        if (b.id) {
            const id = readUserId(b.id);
            const cur = await env.DB.prepare('SELECT i.*, w.user_id FROM wish_items i JOIN wishlists w ON w.id = i.list_id WHERE i.id = ?1').bind(id).first();
            if (!cur || cur.user_id !== me.id) return json({ error: 'Подарок не найден' }, 404, cors);
            const iv = img ? now : img === null ? 0 : cur.img_v, iw = img ? img.w : img === null ? 0 : cur.w, ih = img ? img.h : img === null ? 0 : cur.h;
            item = await env.DB.prepare(`UPDATE wish_items SET title = ?1, url = ?2, price = ?3, note = ?4, want = ?5, img_v = ?6, w = ?7, h = ?8, updated = ?9
                WHERE id = ?10 RETURNING *`).bind(title, link, price, note, want, iv, iw, ih, now, id).first();
        } else {
            const list = await wishList(env, readUserId(b.list));
            if (!list || list.user_id !== me.id) return json({ error: 'Список не найден' }, 404, cors);
            const c = await env.DB.prepare('SELECT COUNT(*) AS n, COALESCE(MAX(pos), 0) AS p FROM wish_items WHERE list_id = ?1').bind(list.id).first();
            if (c.n >= WISH_ITEMS_MAX) return json({ error: `В одном списке — не больше ${WISH_ITEMS_MAX} подарков` }, 429, cors);
            item = await env.DB.prepare(`INSERT INTO wish_items (list_id, title, url, price, note, want, pos, img_v, w, h, created, updated)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11) RETURNING *`)
                .bind(list.id, title, link, price, note, want, c.p + 1, img ? now : 0, img ? img.w : 0, img ? img.h : 0, now).first();
        }
        if (img) await env.DB.prepare('INSERT INTO wish_images (item_id, data) VALUES (?1, ?2) ON CONFLICT(item_id) DO UPDATE SET data = excluded.data').bind(item.id, img.data).run();
        else if (img === null) await env.DB.prepare('DELETE FROM wish_images WHERE item_id = ?1').bind(item.id).run();
        await env.DB.prepare('UPDATE wishlists SET updated = ?1 WHERE id = ?2').bind(now, item.list_id).run();
        return json({ item: itemOut(item) }, 200, cors);
    }

    if (path === '/wish/item/delete') {
        const id = readUserId((await readJson(request)).id);
        const cur = await env.DB.prepare('SELECT i.list_id, w.user_id FROM wish_items i JOIN wishlists w ON w.id = i.list_id WHERE i.id = ?1').bind(id).first();
        if (!cur || cur.user_id !== me.id) return json({ error: 'Подарок не найден' }, 404, cors);
        await env.DB.batch([
            env.DB.prepare('DELETE FROM wish_images WHERE item_id = ?1').bind(id),
            env.DB.prepare('DELETE FROM wish_reserv WHERE item_id = ?1').bind(id),
            env.DB.prepare('DELETE FROM wish_items WHERE id = ?1').bind(id),
            env.DB.prepare('UPDATE wishlists SET updated = ?1 WHERE id = ?2').bind(Date.now(), cur.list_id)
        ]);
        return json({ ok: true }, 200, cors);
    }

    // --- «Я подарю»: друг в ProdU (или кто открыл ссылку в приложении) бронирует подарок ---
    if (path === '/wish/reserve') {
        const b = await readJson(request);
        const id = readUserId(b.item);
        const row = await env.DB.prepare('SELECT w.*, i.id AS item_id FROM wish_items i JOIN wishlists w ON w.id = i.list_id WHERE i.id = ?1').bind(id).first();
        const access = await wishAccess(env, me.id, row, String(b.t || ''));
        if (access === 'none') return json({ error: 'Подарок не найден' }, 404, cors);
        if (access === 'owner') throw new BadRequest('Свои подарки бронировать не нужно');
        const cur = await env.DB.prepare('SELECT user_id FROM wish_reserv WHERE item_id = ?1').bind(id).first();
        if (b.reserve) {
            if (cur && cur.user_id !== me.id) return json({ error: 'Этот подарок уже кто-то дарит' }, 409, cors);
            await env.DB.prepare(`INSERT INTO wish_reserv (item_id, list_id, user_id, name, created) VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(item_id) DO NOTHING`).bind(id, row.id, me.id, me.display, Date.now()).run();
        } else if (cur) {
            if (cur.user_id !== me.id) return json({ error: 'Отменить бронь может только тот, кто её сделал' }, 403, cors);
            await env.DB.prepare('DELETE FROM wish_reserv WHERE item_id = ?1').bind(id).run();
        }
        return json({ reserved: b.reserve ? { mine: true, name: me.display, anon: false } : null }, 200, cors);
    }

    // --- подтянуть название, цену и фото по ссылке на товар ---
    if (path === '/wish/preview') {
        return json(await wishPreview(cleanUrl(url.searchParams.get('url'))), 200, cors);
    }

    return json({ error: 'not found' }, 404, cors);
}

// Метаданные страницы товара (Open Graph, schema.org). Магазины часто не отдают их роботам — тогда просто вернём пусто.
async function wishPreview(link) {
    if (!link) return {};
    const out = {};
    const get = async (u, limit, accept) => {
        const ctl = new AbortController();
        const t = setTimeout(() => ctl.abort(), 7000);
        try {
            const r = await fetch(u, { redirect: 'follow', signal: ctl.signal, headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; ProdU-Wishlist/1.0; +link preview)', 'Accept': accept, 'Accept-Language': 'ru,en;q=0.8' } });
            if (!r.ok || !r.body) return null;
            const reader = r.body.getReader();
            const parts = [];
            let n = 0;
            for (;;) {
                const { done, value } = await reader.read();
                if (done) break;
                n += value.length;
                if (n > limit) { if (accept.startsWith('image')) { try { reader.cancel(); } catch {} return null; } parts.push(value.subarray(0, value.length - (n - limit))); try { reader.cancel(); } catch {} break; }
                parts.push(value);
            }
            const buf = new Uint8Array(parts.reduce((s, p) => s + p.length, 0));
            let o = 0;
            for (const p of parts) { buf.set(p, o); o += p.length; }
            return { buf, type: (r.headers.get('content-type') || '').toLowerCase(), url: r.url || u };
        } catch { return null; } finally { clearTimeout(t); }
    };
    const page = await get(link, WISH_PREVIEW_HTML, 'text/html,application/xhtml+xml');
    if (!page || !/html|xml/.test(page.type)) return out;
    const html = new TextDecoder('utf-8', { fatal: false }).decode(page.buf);
    const decode = s => s.replace(/&(#x?[0-9a-f]+|amp|lt|gt|quot|apos|nbsp);/gi, (m, e) => {
        const map = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ' };
        if (map[e.toLowerCase()]) return map[e.toLowerCase()];
        const code = e[1] === 'x' || e[1] === 'X' ? parseInt(e.slice(2), 16) : parseInt(e.slice(1), 10);
        return Number.isFinite(code) && code > 0 && code < 0x110000 ? String.fromCodePoint(code) : m;
    }).replace(/\s+/g, ' ').trim();
    const meta = name => {
        const re = new RegExp(`<meta[^>]+(?:property|name|itemprop)=["']${name}["'][^>]*>`, 'i');
        const tag = re.exec(html);
        if (!tag) return '';
        const c = /content=["']([^"']*)["']/i.exec(tag[0]);
        return c ? decode(c[1]) : '';
    };
    const title = meta('og:title') || meta('twitter:title') || ((/<title[^>]*>([^<]*)<\/title>/i.exec(html) || [])[1] ? decode(/<title[^>]*>([^<]*)<\/title>/i.exec(html)[1]) : '');
    if (title) out.title = [...title].slice(0, WISH_TITLE).join('');
    const priceRaw = meta('product:price:amount') || meta('og:price:amount') || meta('price') || ((/"price"\s*:\s*"?(\d[\d\s.,]*)/i.exec(html) || [])[1] || '');
    const price = Number(String(priceRaw).replace(/\s/g, '').replace(',', '.'));
    if (price > 0 && price < 1e10) out.price = Math.round(price);
    let img = meta('og:image:secure_url') || meta('og:image') || meta('twitter:image');
    if (img) {
        try {
            img = new URL(img, page.url).href;
            if (/^https?:/.test(img)) {
                const pic = await get(img, WISH_PREVIEW_IMG, 'image/avif,image/webp,image/jpeg,image/png,image/*');
                if (pic && /^image\/(jpeg|png|webp|gif|avif)/.test(pic.type)) {
                    let s = '';
                    for (let i = 0; i < pic.buf.length; i += 0x8000) s += String.fromCharCode.apply(null, pic.buf.subarray(i, i + 0x8000));
                    out.image = `data:${pic.type.split(';')[0]};base64,${btoa(s)}`;
                }
            }
        } catch {}
    }
    return out;
}

// ---------- публичная страница списка: /w/<токен>, /w/<токен>/img/<id>, /w/<токен>/reserve ----------
async function handleWishPublic(request, url, env) {
    const m = /^\/w\/([A-Za-z0-9_-]{8,40})(?:\/(img)\/(\d+)|\/(reserve))?\/?$/.exec(url.pathname);
    if (!m) return null;
    const token = m[1];
    const list = await env.DB.prepare('SELECT * FROM wishlists WHERE token = ?1').bind(token).first();
    const ok = list && list.vis === 'link';
    if (m[2]) {
        if (!ok) return new Response('Not found', { status: 404 });
        const row = await env.DB.prepare('SELECT d.data FROM wish_items i JOIN wish_images d ON d.item_id = i.id WHERE i.id = ?1 AND i.list_id = ?2').bind(Number(m[3]), list.id).first();
        if (!row) return new Response('Not found', { status: 404 });
        const b64 = row.data.slice(row.data.indexOf(',') + 1);
        return new Response(Uint8Array.from(atob(b64), c => c.charCodeAt(0)), {
            headers: { 'Content-Type': 'image/jpeg', 'Cache-Control': 'public, max-age=86400', 'X-Content-Type-Options': 'nosniff' }
        });
    }
    if (m[4]) {
        if (request.method !== 'POST') return new Response('Method not allowed', { status: 405 });
        if (!ok) return json({ error: 'Список закрыт владельцем' }, 404, {});
        try {
            return await wishGuestReserve(env, list, await readJson(request));
        } catch (e) {
            return json({ error: String(e.message || e) }, e instanceof BadRequest ? 400 : 500, {});
        }
    }
    if (request.method !== 'GET' && request.method !== 'HEAD') return new Response('Method not allowed', { status: 405 });
    if (!ok) return wishHtml(wishClosedPage(env), 404);
    const owner = await env.DB.prepare('SELECT display AS login FROM users WHERE id = ?1').bind(list.user_id).first();
    const items = await wishItemsWithReserv(env, list.id, 0, true);
    return wishHtml(wishPage(env, url, list, owner ? owner.login : '', items), 200);
}

async function wishGuestReserve(env, list, b) {
    const id = Number(b.item);
    if (!Number.isSafeInteger(id) || id <= 0) throw new BadRequest('bad item');
    const item = await env.DB.prepare('SELECT id FROM wish_items WHERE id = ?1 AND list_id = ?2').bind(id, list.id).first();
    if (!item) return json({ error: 'Подарок не найден' }, 404, {});
    const cur = await env.DB.prepare('SELECT user_id, secret FROM wish_reserv WHERE item_id = ?1').bind(id).first();
    if (b.cancel) {
        // отменить может только тот, кто бронировал: у него в браузере хранится секрет
        if (!cur) return json({ reserved: null }, 200, {});
        if (cur.user_id != null || !sameString(await sha256b64u(enc(String(b.cancel))), cur.secret)) return json({ error: 'Эту бронь сделал кто-то другой' }, 403, {});
        await env.DB.prepare('DELETE FROM wish_reserv WHERE item_id = ?1').bind(id).run();
        return json({ reserved: null }, 200, {});
    }
    if (cur) return json({ error: 'Этот подарок уже кто-то дарит' }, 409, {});
    const recent = await env.DB.prepare('SELECT COUNT(*) AS n FROM wish_reserv WHERE list_id = ?1 AND user_id IS NULL AND created > ?2').bind(list.id, Date.now() - 3600e3).first();
    if (recent.n >= WISH_ANON_PER_HOUR) return json({ error: 'Слишком много броней за час — попробуйте позже' }, 429, {});
    const name = wishText(b.name, WISH_NAME);
    const secret = randomB64u(16);
    await env.DB.prepare('INSERT INTO wish_reserv (item_id, list_id, user_id, name, secret, created) VALUES (?1, ?2, NULL, ?3, ?4, ?5) ON CONFLICT(item_id) DO NOTHING')
        .bind(id, list.id, name, await sha256b64u(enc(secret)), Date.now()).run();
    return json({ reserved: { mine: true, name, anon: true }, secret }, 200, {});
}

const hEsc = s => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
function wishHtml(body, status) {
    return new Response(body, { status, headers: {
        'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer',
        'Content-Security-Policy': "default-src 'none'; img-src 'self' data:; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    } });
}
const WISH_CSS = `
:root { color-scheme: light dark; --bg: #f4f1ec; --card: rgba(255,255,255,.78); --tile: #fff; --text: #1a1a1a; --muted: rgba(26,26,26,.6); --edge: rgba(0,0,0,.07);
  --accent: #70e000; --accent2: #9ef01a; --chip: rgba(0,0,0,.06); --danger: #ff4757; }
@media (prefers-color-scheme: dark) { :root { --bg: #1d1c1c; --card: rgba(48,48,48,.82); --tile: #2a2a2a; --text: #fff; --muted: rgba(255,255,255,.62); --edge: rgba(255,255,255,.08); --chip: rgba(255,255,255,.1); } }
* { box-sizing: border-box; }
body { margin: 0; font: 16px/1.45 -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background: var(--bg); color: var(--text);
  background-image: radial-gradient(60% 40% at 15% 0%, rgba(158,240,26,.25), transparent 70%), radial-gradient(50% 35% at 100% 20%, rgba(112,224,0,.14), transparent 70%); background-attachment: fixed; }
.wrap { max-width: 980px; margin: 0 auto; padding: 28px 16px 60px; }
.head { background: var(--card); border: 1px solid var(--edge); border-radius: 28px; padding: 22px 22px 20px; backdrop-filter: blur(18px) saturate(160%); -webkit-backdrop-filter: blur(18px) saturate(160%);
  box-shadow: inset 0 1px 0 rgba(255,255,255,.5), 0 10px 30px rgba(0,0,0,.08); }
.emoji { font-size: 44px; line-height: 1; }
h1 { margin: 10px 0 4px; font-size: 30px; line-height: 1.15; letter-spacing: -.02em; overflow-wrap: anywhere; }
.by { color: var(--muted); font-size: 15px; }
.note { margin-top: 12px; white-space: pre-wrap; overflow-wrap: anywhere; }
.chips { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; }
.chip { background: var(--chip); border-radius: 999px; padding: 6px 12px; font-size: 14px; font-weight: 600; }
.banner { margin-top: 14px; padding: 10px 14px; border-radius: 16px; background: rgba(255,165,2,.16); font-size: 14px; }
.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 14px; margin-top: 16px; }
.item { background: var(--card); border: 1px solid var(--edge); border-radius: 24px; overflow: hidden; display: flex; flex-direction: column;
  backdrop-filter: blur(14px); -webkit-backdrop-filter: blur(14px); box-shadow: 0 6px 20px rgba(0,0,0,.06); }
.item.res { opacity: .62; }
.pic { aspect-ratio: 4 / 3; background: var(--tile); display: grid; place-items: center; font-size: 46px; overflow: hidden; }
.pic img { width: 100%; height: 100%; object-fit: cover; display: block; }
.body { padding: 12px 14px 14px; display: flex; flex-direction: column; gap: 6px; flex: 1; }
.t { font-weight: 700; font-size: 17px; overflow-wrap: anywhere; }
.want { color: #e0a100; font-size: 13px; font-weight: 700; }
.price { font-weight: 700; font-variant-numeric: tabular-nums; }
.d { color: var(--muted); font-size: 14px; white-space: pre-wrap; overflow-wrap: anywhere; }
.acts { display: flex; flex-wrap: wrap; gap: 8px; margin-top: auto; padding-top: 6px; }
.btn { appearance: none; border: 0; border-radius: 999px; padding: 9px 14px; font: inherit; font-size: 14px; font-weight: 700; cursor: pointer; text-decoration: none;
  display: inline-flex; align-items: center; gap: 6px; color: #1a1a1a; background: linear-gradient(180deg, var(--accent2), var(--accent)); }
.btn.sec { background: var(--chip); color: var(--text); }
.btn:disabled { opacity: .6; cursor: default; }
.btn:focus-visible { outline: 3px solid var(--accent); outline-offset: 2px; }
.state { font-size: 13.5px; font-weight: 600; color: var(--muted); }
.empty { text-align: center; color: var(--muted); padding: 40px 10px; }
.foot { text-align: center; color: var(--muted); font-size: 14px; margin-top: 30px; }
.foot a { color: inherit; font-weight: 700; }
dialog { border: 0; border-radius: 24px; padding: 20px; max-width: 360px; width: calc(100% - 32px); background: var(--tile); color: var(--text); box-shadow: 0 20px 60px rgba(0,0,0,.3); }
dialog::backdrop { background: rgba(0,0,0,.35); backdrop-filter: blur(3px); }
dialog h2 { margin: 0 0 6px; font-size: 20px; }
dialog p { margin: 0 0 12px; color: var(--muted); font-size: 14.5px; }
dialog input { width: 100%; font: inherit; padding: 11px 14px; border-radius: 14px; border: 1px solid var(--edge); background: var(--bg); color: var(--text); margin-bottom: 12px; }
dialog .row { display: flex; gap: 8px; justify-content: flex-end; }
.msg { color: var(--danger); font-size: 14px; min-height: 1em; margin-bottom: 8px; }
@media (max-width: 520px) { .grid { grid-template-columns: 1fr 1fr; gap: 10px; } .t { font-size: 15.5px; } .body { padding: 10px 11px 12px; } h1 { font-size: 26px; } }
@media (max-width: 360px) { .grid { grid-template-columns: 1fr; } }
`;
function wishSiteLink(env) {
    try { return env.SITE_URL && !env.SITE_URL.startsWith('mailto:') ? new URL(env.SITE_URL).href : ''; } catch { return ''; }
}
function wishClosedPage(env) {
    const site = wishSiteLink(env);
    return `<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="robots" content="noindex">
<title>Вишлист недоступен</title><style>${WISH_CSS}</style></head><body><div class="wrap"><div class="head"><div class="emoji">🔒</div><h1>Список недоступен</h1>
<div class="by">Владелец закрыл доступ по ссылке или удалил список.</div></div>${site ? `<div class="foot"><a href="${hEsc(site)}">ProdU</a></div>` : ''}</div></body></html>`;
}
const fmtRub = n => String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ' ') + ' ₽';
function wishPage(env, url, list, ownerLogin, items) {
    const base = `${url.origin}/w/${list.token}`;
    const cover = items.find(i => i.img);
    const site = wishSiteLink(env);
    const months = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'];
    const date = list.date ? (([y, mo, d]) => `${+d} ${months[mo - 1]} ${y}`)(list.date.split('-').map(Number)) : '';
    const plural = (n, a, b, c) => { const m = n % 100, k = n % 10; return m > 10 && m < 20 ? c : k === 1 ? a : k > 1 && k < 5 ? b : c; };
    const desc = `${items.length} ${plural(items.length, 'подарок', 'подарка', 'подарков')}${ownerLogin ? ` · ${ownerLogin}` : ''}${list.note ? ' — ' + [...list.note].slice(0, 140).join('') : ''}`;
    const card = i => {
        const r = i.reserved;
        return `<article class="item${r ? ' res' : ''}" data-id="${i.id}">
<div class="pic">${i.img ? `<img src="${base}/img/${i.id}?v=${i.img.v}" alt="" loading="lazy" width="${i.img.w}" height="${i.img.h}">` : hEsc(list.emoji)}</div>
<div class="body">${i.want ? '<div class="want">★ Очень хочу</div>' : ''}<div class="t">${hEsc(i.title)}</div>
${i.price ? `<div class="price">${fmtRub(i.price)}</div>` : ''}${i.note ? `<div class="d">${hEsc(i.note)}</div>` : ''}
<div class="acts">${i.url ? `<a class="btn sec" href="${hEsc(i.url)}" target="_blank" rel="noopener noreferrer nofollow">Где купить ↗</a>` : ''}
<span class="rv" data-res="${r ? 1 : 0}" data-name="${r ? hEsc(r.name) : ''}"></span></div></div></article>`;
    };
    return `<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="robots" content="noindex"><title>${hEsc(list.title)} — вишлист${ownerLogin ? ' ' + hEsc(ownerLogin) : ''}</title>
<meta name="description" content="${hEsc(desc)}">
<meta property="og:type" content="website"><meta property="og:title" content="${hEsc(list.emoji + ' ' + list.title)}"><meta property="og:description" content="${hEsc(desc)}">
<meta property="og:url" content="${hEsc(base)}">${cover ? `<meta property="og:image" content="${base}/img/${cover.id}?v=${cover.img.v}"><meta name="twitter:card" content="summary_large_image">` : ''}
<meta name="theme-color" content="#70e000"><style>${WISH_CSS}</style></head><body><main class="wrap">
<header class="head"><div class="emoji" aria-hidden="true">${hEsc(list.emoji)}</div><h1>${hEsc(list.title)}</h1>
<div class="by">Вишлист${ownerLogin ? ` <b>${hEsc(ownerLogin)}</b>` : ''}</div>${list.note ? `<div class="note">${hEsc(list.note)}</div>` : ''}
<div class="chips"><span class="chip">${items.length} ${plural(items.length, 'подарок', 'подарка', 'подарков')}</span>${date ? `<span class="chip">📅 ${date}</span>` : ''}</div>
<div class="banner" id="ownerBanner" hidden>Так список видят гости. Брони скрыты, чтобы не испортить вам сюрприз.</div>
<div class="banner" id="guestHint">Нажмите «Я подарю» на подарке, который выбрали, — остальные увидят, что он занят, и не купят то же самое. Владелец списка брони не видит.</div></header>
<section class="grid" aria-label="Подарки">${items.length ? items.map(card).join('') : '<div class="empty">Здесь пока нет подарков.</div>'}</section>
<div class="foot">Сделано в ${site ? `<a href="${hEsc(site)}">ProdU</a>` : 'ProdU'} — планер, дневник и вишлисты</div></main>
<dialog id="dlg"><h2>Я подарю это</h2><p>Подарок отметится как занятый, чтобы его не купил кто-то ещё. Владелец списка этого не увидит.</p>
<input id="nm" maxlength="40" placeholder="Ваше имя (необязательно)" autocomplete="name"><div class="msg" id="err" role="alert"></div>
<div class="row"><button class="btn sec" id="no" type="button">Отмена</button><button class="btn" id="yes" type="button">Забронировать</button></div></dialog>
<script>
(function () {
  var base = ${JSON.stringify(base)}, KEY = 'produ_wish_' + ${JSON.stringify(list.token)};
  var owner = /[?&]owner=1/.test(location.search);
  var mine = {}; try { mine = JSON.parse(localStorage.getItem(KEY) || '{}') || {}; } catch (e) {}
  var save = function () { try { localStorage.setItem(KEY, JSON.stringify(mine)); } catch (e) {} };
  if (owner) { document.getElementById('ownerBanner').hidden = false; document.getElementById('guestHint').hidden = true; }
  function paint(el) {
    var id = el.closest('.item').dataset.id, res = el.dataset.res === '1', name = el.dataset.name, my = !!mine[id];
    var art = el.closest('.item');
    el.textContent = '';
    if (owner) { art.classList.remove('res'); return; }
    art.classList.toggle('res', res && !my);
    if (res && my) {
      var s = document.createElement('span'); s.className = 'state'; s.textContent = 'Вы дарите это 🎁'; el.appendChild(s);
      var c = document.createElement('button'); c.className = 'btn sec'; c.type = 'button'; c.textContent = 'Отменить'; c.onclick = function () { act(id, true, null, c); }; el.appendChild(c);
    } else if (res) {
      var s2 = document.createElement('span'); s2.className = 'state'; s2.textContent = name ? 'Уже дарит: ' + name : 'Уже кто-то дарит'; el.appendChild(s2);
    } else {
      var b = document.createElement('button'); b.className = 'btn'; b.type = 'button'; b.textContent = 'Я подарю'; b.onclick = function () { ask(id); }; el.appendChild(b);
    }
  }
  var dlg = document.getElementById('dlg'), nm = document.getElementById('nm'), err = document.getElementById('err'), cur = null;
  try { nm.value = localStorage.getItem('produ_wish_name') || ''; } catch (e) {}
  function ask(id) { cur = id; err.textContent = ''; if (dlg.showModal) { dlg.showModal(); nm.focus(); } else act(id, false, '', null); }
  document.getElementById('no').onclick = function () { dlg.close(); };
  document.getElementById('yes').onclick = function () {
    var n = nm.value.trim(); try { localStorage.setItem('produ_wish_name', n); } catch (e) {}
    act(cur, false, n, document.getElementById('yes'));
  };
  function act(id, cancel, name, btn) {
    if (btn) btn.disabled = true;
    fetch(base + '/reserve', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cancel ? { item: +id, cancel: mine[id] } : { item: +id, name: name || '' }) })
      .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, status: r.status, j: j }; }); })
      .then(function (r) {
        var el = document.querySelector('.item[data-id="' + id + '"] .rv');
        if (r.ok) {
          if (cancel) { delete mine[id]; el.dataset.res = '0'; }
          else { mine[id] = r.j.secret; el.dataset.res = '1'; el.dataset.name = name || ''; }
          save(); paint(el); if (dlg.open) dlg.close();
        } else {
          if (r.status === 409) { el.dataset.res = '1'; paint(el); }
          if (dlg.open) err.textContent = r.j.error || 'Не получилось'; else alert(r.j.error || 'Не получилось');
        }
      })
      .catch(function () { if (dlg.open) err.textContent = 'Нет связи — попробуйте ещё раз'; })
      .finally(function () { if (btn) btn.disabled = false; });
  }
  document.querySelectorAll('.rv').forEach(paint);
})();
</script></body></html>`;
}

// ===================== Объявления разработчика =====================
// Разработчик пишет объявление на странице /admin (нужна переменная ADMIN_KEY) — оно появляется в разделе
// «Уведомления» у всех и, по желанию, приходит push-уведомлением на все устройства с включёнными уведомлениями.
const NOTICE_TITLE = 120, NOTICE_BODY = 4000, NOTICES_LIST = 30;
const DEV_PUSH_PER_RUN = 40;      // push за один раз (лимит подзапросов бесплатного тарифа — 50)

const noticeSchema = env => [
    env.DB.prepare(`CREATE TABLE IF NOT EXISTS notices (
        id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, body TEXT NOT NULL, popup INTEGER NOT NULL DEFAULT 0, created INTEGER NOT NULL)`),
    env.DB.prepare('CREATE TABLE IF NOT EXISTS dev_queue (endpoint TEXT PRIMARY KEY, notice_id INTEGER NOT NULL)')
];

async function handleNotices(request, url, env, cors, ctx) {
    const path = url.pathname;
    if (path === '/notices' && request.method === 'GET') {
        const since = Math.max(0, Number(url.searchParams.get('since')) || 0);
        const { results } = await env.DB.prepare('SELECT id, title, body, popup, created FROM notices WHERE id > ?1 ORDER BY id DESC LIMIT ?2')
            .bind(since, NOTICES_LIST).all();
        return new Response(JSON.stringify({ notices: results.map(n => ({ ...n, popup: !!n.popup })) }), {
            status: 200, headers: { ...cors, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'public, max-age=60' }
        });
    }
    if (path === '/admin' && request.method === 'GET') return adminPage();
    if (path.startsWith('/admin/') && request.method === 'POST') {
        const b = await readJson(request, 20_000);
        if (!env.ADMIN_KEY || String(env.ADMIN_KEY).length < 12) return json({ error: 'Задайте переменную ADMIN_KEY (не короче 12 символов) в настройках Worker' }, 403, {});
        if (!sameString(String(b.key || ''), String(env.ADMIN_KEY))) {
            await new Promise(r => setTimeout(r, 800));      // перебор ключа — медленно
            return json({ error: 'Неверный ключ' }, 403, {});
        }
        if (path === '/admin/list') {
            const { results } = await env.DB.prepare('SELECT id, title, body, popup, created FROM notices ORDER BY id DESC LIMIT 50').all();
            const st = await env.DB.prepare('SELECT (SELECT COUNT(*) FROM subs) AS devices, (SELECT COUNT(*) FROM dev_queue) AS queued').first();
            return json({ notices: results, ...st }, 200, {});
        }
        if (path === '/admin/notice') {
            const title = [...String(b.title || '').trim()].slice(0, NOTICE_TITLE).join('');
            const body = [...String(b.body || '').replace(/\r\n?/g, '\n').trim()].slice(0, NOTICE_BODY).join('');
            if (!title || !body) throw new BadRequest('Нужны заголовок и текст');
            const row = await env.DB.prepare('INSERT INTO notices (title, body, popup, created) VALUES (?1, ?2, ?3, ?4) RETURNING id')
                .bind(title, body, b.popup ? 1 : 0, Date.now()).first();
            let queued = 0;
            if (b.push) {
                await env.DB.prepare('INSERT OR REPLACE INTO dev_queue (endpoint, notice_id) SELECT endpoint, ?1 FROM subs').bind(row.id).run();
                queued = (await env.DB.prepare('SELECT COUNT(*) AS n FROM dev_queue').first()).n;
                const p = sendDevQueue(env);
                if (ctx && ctx.waitUntil) ctx.waitUntil(p); else await p;
            }
            return json({ id: row.id, queued }, 200, {});
        }
        if (path === '/admin/delete') {
            const id = readUserId(b.id);
            await env.DB.batch([
                env.DB.prepare('DELETE FROM notices WHERE id = ?1').bind(id),
                env.DB.prepare('DELETE FROM dev_queue WHERE notice_id = ?1').bind(id)
            ]);
            return json({ ok: true }, 200, {});
        }
    }
    return null;
}

// Рассылка push по очереди: часть сразу, остальное — каждую минуту из Cron Trigger
async function sendDevQueue(env) {
    try {
        if (!env.SITE_URL) return;
        const { results } = await env.DB.prepare(`SELECT q.endpoint, q.notice_id, s.p256dh, s.auth, n.title, n.body FROM dev_queue q
            JOIN subs s ON s.endpoint = q.endpoint JOIN notices n ON n.id = q.notice_id LIMIT ?1`).bind(DEV_PUSH_PER_RUN).all();
        // подписки, которых уже нет, и удалённые объявления просто выбрасываем из очереди
        await env.DB.prepare('DELETE FROM dev_queue WHERE endpoint NOT IN (SELECT endpoint FROM subs) OR notice_id NOT IN (SELECT id FROM notices)').run();
        if (!results.length) return;
        const vapid = await getVapid(env);
        const gone = [];
        await Promise.all(results.map(async r => {
            try {
                const body = JSON.stringify({ kind: 'dev', id: r.notice_id, title: r.title, body: [...r.body].slice(0, 180).join('') });
                const res = await sendPush(r, body, vapid, env.SITE_URL);
                if (res.status === 404 || res.status === 410) gone.push(r.endpoint);
            } catch (e) { console.warn('dev push failed', e); }
        }));
        const done = JSON.stringify(results.map(r => r.endpoint));
        const g = JSON.stringify(gone);
        await env.DB.batch([
            env.DB.prepare('DELETE FROM dev_queue WHERE endpoint IN (SELECT value FROM json_each(?1))').bind(done),
            env.DB.prepare('DELETE FROM reminders WHERE endpoint IN (SELECT value FROM json_each(?1))').bind(g),
            env.DB.prepare('DELETE FROM sub_users WHERE endpoint IN (SELECT value FROM json_each(?1))').bind(g),
            env.DB.prepare('DELETE FROM subs WHERE endpoint IN (SELECT value FROM json_each(?1))').bind(g)
        ]);
    } catch (e) {
        console.warn('sendDevQueue', e);
    }
}

function adminPage() {
    return new Response(`<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex"><title>ProdU — объявления</title><style>
:root { color-scheme: light dark; --bg: #f4f1ec; --card: #fff; --text: #1a1a1a; --muted: rgba(26,26,26,.6); --edge: rgba(0,0,0,.1); }
@media (prefers-color-scheme: dark) { :root { --bg: #1d1c1c; --card: #2a2a2a; --text: #fff; --muted: rgba(255,255,255,.6); --edge: rgba(255,255,255,.12); } }
body { margin: 0; font: 16px/1.45 -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--bg); color: var(--text); }
main { max-width: 640px; margin: 0 auto; padding: 24px 16px 60px; display: grid; gap: 14px; }
.card { background: var(--card); border: 1px solid var(--edge); border-radius: 20px; padding: 18px; display: grid; gap: 10px; }
h1 { margin: 0; font-size: 24px; } label { font-size: 14px; font-weight: 600; } small, .muted { color: var(--muted); font-size: 13.5px; }
input[type=text], input[type=password], textarea { font: inherit; padding: 10px 12px; border-radius: 12px; border: 1px solid var(--edge); background: var(--bg); color: var(--text); width: 100%; box-sizing: border-box; }
textarea { min-height: 140px; resize: vertical; }
.row { display: flex; gap: 14px; flex-wrap: wrap; align-items: center; }
button { font: inherit; font-weight: 700; border: 0; border-radius: 999px; padding: 10px 18px; cursor: pointer; background: linear-gradient(#9ef01a, #70e000); color: #1a1a1a; }
button.sec { background: rgba(128,128,128,.18); color: var(--text); padding: 6px 12px; font-size: 14px; }
.n { border-top: 1px solid var(--edge); padding-top: 10px; display: grid; gap: 4px; } .n p { margin: 0; white-space: pre-wrap; }
.msg { min-height: 1.2em; font-weight: 600; }
</style></head><body><main>
<h1>📣 Объявления ProdU</h1>
<div class="card"><label for="key">Ключ администратора (переменная ADMIN_KEY)</label><input type="password" id="key" autocomplete="current-password">
<div class="row"><button type="button" class="sec" id="load">Показать объявления</button><span class="muted" id="stat"></span></div></div>
<div class="card"><label for="t">Заголовок</label><input type="text" id="t" maxlength="120" placeholder="Например: Важное от разработчика">
<label for="b">Текст</label><textarea id="b" maxlength="4000" placeholder="Что хотите сообщить пользователям"></textarea>
<div class="row"><label><input type="checkbox" id="popup"> Показать окном при запуске</label><label><input type="checkbox" id="push" checked> Отправить push</label></div>
<small>Объявление появится в разделе «Уведомления» у всех. Push придёт тем, у кого включены уведомления (рассылка идёт порциями по ${DEV_PUSH_PER_RUN} раз в минуту).</small>
<div class="row"><button type="button" id="send">Опубликовать</button><span class="msg" id="msg" role="status"></span></div></div>
<div class="card" id="list" hidden></div>
</main><script>
var $ = function (id) { return document.getElementById(id); };
try { $('key').value = sessionStorage.getItem('produ_admin') || ''; } catch (e) {}
function post(path, body) {
  body.key = $('key').value; try { sessionStorage.setItem('produ_admin', body.key); } catch (e) {}
  return fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
    .then(function (r) { return r.json().then(function (j) { if (!r.ok) throw new Error(j.error || r.status); return j; }); });
}
function esc(s) { return String(s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
function load() {
  post('/admin/list', {}).then(function (j) {
    $('stat').textContent = 'Устройств с push: ' + j.devices + (j.queued ? ' · в очереди рассылки: ' + j.queued : '');
    $('list').hidden = false;
    $('list').innerHTML = '<b>Опубликовано</b>' + (j.notices.length ? j.notices.map(function (n) {
      return '<div class="n"><div class="row"><b>' + esc(n.title) + '</b><small>' + new Date(n.created).toLocaleString('ru-RU') + (n.popup ? ' · окно при запуске' : '') + '</small>' +
        '<button type="button" class="sec" data-del="' + n.id + '">Удалить</button></div><p>' + esc(n.body) + '</p></div>';
    }).join('') : '<p class="muted">Пока ничего.</p>');
  }).catch(function (e) { $('stat').textContent = e.message; });
}
$('load').onclick = load;
$('list').onclick = function (e) {
  var id = e.target.dataset && e.target.dataset.del;
  if (id && confirm('Удалить объявление? У тех, кто его уже получил, оно останется в приложении.')) post('/admin/delete', { id: +id }).then(load);
};
$('send').onclick = function () {
  $('msg').textContent = '';
  if (!$('t').value.trim() || !$('b').value.trim()) { $('msg').textContent = 'Нужны заголовок и текст'; return; }
  $('send').disabled = true;
  post('/admin/notice', { title: $('t').value, body: $('b').value, popup: $('popup').checked, push: $('push').checked })
    .then(function (j) { $('msg').textContent = '✅ Опубликовано' + (j.queued ? ', push в очереди: ' + j.queued : ''); $('t').value = ''; $('b').value = ''; load(); })
    .catch(function (e) { $('msg').textContent = '⚠️ ' + e.message; })
    .finally(function () { $('send').disabled = false; });
};
</script></body></html>`, { status: 200, headers: {
        'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer',
        'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    } });
}

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
