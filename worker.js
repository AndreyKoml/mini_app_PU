/* ProdU — сервер push-уведомлений (Cloudflare Worker)
 *
 * Что делает:
 *   - хранит напоминания каждого устройства (база D1);
 *   - каждую минуту (Cron Trigger) отправляет push тем, у кого подошло время;
 *   - ключи VAPID создаёт сам при первом запуске и хранит в базе.
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
 *   GET  /vapid   — публичный ключ для подписки
 *   POST /sync    — { subscription, reminders: [{ id, due, title, body }] }
 */

const MAX_REMINDERS = 500;        // напоминаний с одного устройства
const SEND_PER_RUN = 25;          // push за один запуск (лимиты бесплатного тарифа)
const MAX_LATE_MS = 12 * 3600e3;  // старше 12 часов — уже не отправляем
const PUSH_TTL = 12 * 3600;       // сколько push-сервис хранит сообщение, если телефон офлайн (сек.)

// Разрешённые push-сервисы (Chrome/Android, Firefox, Edge/Windows, Safari/iPhone)
const PUSH_HOSTS = ['fcm.googleapis.com', 'android.googleapis.com', 'push.services.mozilla.com', 'notify.windows.com', 'push.apple.com'];

export default {
    async fetch(request, env) {
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
                    'SELECT (SELECT COUNT(*) FROM subs) AS devices, (SELECT COUNT(*) FROM reminders WHERE sent = 0) AS pending'
                ).first();
                return text(`ProdU push: OK\nУстройств: ${stats.devices}\nОжидают отправки: ${stats.pending}\nКлюч: ${publicKey.slice(0, 16)}…`, 200, cors);
            }

            if (request.method === 'GET' && url.pathname === '/vapid') {
                const { publicKey } = await getVapid(env);
                return json({ publicKey }, 200, cors);
            }

            if (request.method === 'POST' && url.pathname === '/sync') {
                const raw = await request.text();
                if (raw.length > 300_000) return json({ error: 'too large' }, 413, cors);
                const body = JSON.parse(raw);
                const count = await syncDevice(env, body);
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
        env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_reminders_due ON reminders (sent, due)')
    ]);
    schemaReady = true;
}

class BadRequest extends Error {}

function validSubscription(sub) {
    if (!sub || typeof sub.endpoint !== 'string' || !sub.keys) return false;
    if (typeof sub.keys.p256dh !== 'string' || typeof sub.keys.auth !== 'string') return false;
    let u;
    try { u = new URL(sub.endpoint); } catch { return false; }
    if (u.protocol !== 'https:') return false;
    return PUSH_HOSTS.some(h => u.hostname === h || u.hostname.endsWith('.' + h));
}

async function syncDevice(env, body) {
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
            .bind(sub.endpoint, now - 3 * 86400e3)
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
    }
    if (stmts.length) await env.DB.batch(stmts);
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
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
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
