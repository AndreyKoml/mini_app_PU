/* ProdU — service worker
 * Положите этот файл в ту же папку, что и index.html.
 *
 * Что он делает:
 *  1. Показывает системные уведомления (на Android без service worker они не работают).
 *  2. Открывает приложение по нажатию на уведомление.
 *  3. Сохраняет страницу и библиотеки в кэш — приложение открывается без интернета.
 *  4. Фоновая проверка напоминаний (periodicsync) — только для установленного приложения
 *     в Chrome/Edge; частоту проверок выбирает браузер, поэтому возможны опоздания.
 *  5. Принимает push-сообщения от сервера (на будущее: для точных уведомлений
 *     при закрытой странице нужен сервер, который пришлёт push в нужное время).
 */

const CACHE = 'produ-v2';
const PRECACHE = ['./', './index.html', './manifest.json', './icon_newyear.png', './icon-192.png', './icon-512.png'];

// ---------- IndexedDB (страница кладёт сюда список напоминаний) ----------
function openDB() {
    return new Promise((res, rej) => {
        const r = indexedDB.open('produ-sw', 1);
        r.onupgradeneeded = () => r.result.createObjectStore('kv');
        r.onsuccess = () => res(r.result);
        r.onerror = () => rej(r.error);
    });
}
async function idbGet(key) {
    const db = await openDB();
    return new Promise((res, rej) => {
        const q = db.transaction('kv').objectStore('kv').get(key);
        q.onsuccess = () => res(q.result);
        q.onerror = () => rej(q.error);
    });
}
async function idbSet(key, val) {
    const db = await openDB();
    return new Promise((res, rej) => {
        const tx = db.transaction('kv', 'readwrite');
        tx.objectStore('kv').put(val, key);
        tx.oncomplete = () => res();
        tx.onerror = () => rej(tx.error);
    });
}

// ---------- Установка и обновление ----------
self.addEventListener('install', event => {
    event.waitUntil(
        caches.open(CACHE)
            .then(cache => Promise.all(PRECACHE.map(url => cache.add(url).catch(() => {}))))
            .then(() => self.skipWaiting())
    );
});

self.addEventListener('activate', event => {
    event.waitUntil(
        caches.keys()
            .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
            .then(() => self.clients.claim())
    );
});

// ---------- Работа без интернета: сначала сеть, при ошибке — кэш ----------
self.addEventListener('fetch', event => {
    const req = event.request;
    if (req.method !== 'GET') return;
    const url = new URL(req.url);
    if (url.protocol !== 'http:' && url.protocol !== 'https:') return;

    event.respondWith(
        fetch(req)
            .then(res => {
                if (res && (res.ok || res.type === 'opaque')) {
                    const copy = res.clone();
                    caches.open(CACHE).then(c => c.put(req, copy)).catch(() => {});
                }
                return res;
            })
            .catch(async () => {
                const cached = await caches.match(req);
                if (cached) return cached;
                if (req.mode === 'navigate') {
                    return (await caches.match('./index.html')) || (await caches.match('./')) || Response.error();
                }
                return Response.error();
            })
    );
});

// ---------- Фоновая проверка напоминаний ----------
async function checkDueReminders() {
    const list = (await idbGet('reminders')) || [];
    const fired = (await idbGet('fired')) || [];
    const now = Date.now();
    const remaining = [];

    for (const r of list) {
        if (fired.includes(r.id)) continue;
        if (r.due <= now) {
            // Слишком старые (больше 12 часов) не показываем — страница отметит их сама
            if (now - r.due < 12 * 3600 * 1000) {
                try {
                    await self.registration.showNotification(r.title, {
                        body: r.body,
                        tag: 'produ-' + r.id,
                        icon: 'icon_newyear.png',
                        data: { url: './index.html' }
                    });
                } catch (e) {
                    // Нет разрешения на уведомления — не помечаем как показанное, страница покажет сама
                    remaining.push(r);
                    continue;
                }
            }
            fired.push(r.id);
        } else {
            remaining.push(r);
        }
    }
    await idbSet('fired', fired);
    await idbSet('reminders', remaining);
}

self.addEventListener('periodicsync', event => {
    if (event.tag === 'produ-reminders') event.waitUntil(checkDueReminders());
});

// ---------- Push от сервера (для будущего сервера напоминаний) ----------
self.addEventListener('push', event => {
    let payload = {};
    try { payload = event.data ? event.data.json() : {}; }
    catch (e) { payload = { body: event.data ? event.data.text() : '' }; }

    const tasks = [
        self.registration.showNotification(payload.title || 'ProdU', {
            body: payload.body || '',
            tag: payload.tag || (payload.id ? 'produ-' + payload.id : undefined),
            icon: 'icon_newyear.png',
            data: { url: payload.url || './index.html' }
        })
    ];
    // Если сервер прислал id напоминания — запоминаем, чтобы страница не показала его повторно
    if (payload.id) {
        tasks.push(idbGet('fired').then(f => idbSet('fired', [...(f || []), payload.id])).catch(() => {}));
    }
    event.waitUntil(Promise.all(tasks));
});

// ---------- Браузер сменил push-подписку: подписываемся заново и сообщаем серверу ----------
function b64uToBytes(s) {
    const b = atob(s.replace(/-/g, '+').replace(/_/g, '/') + '==='.slice((s.length + 3) % 4));
    return Uint8Array.from(b, c => c.charCodeAt(0));
}

self.addEventListener('pushsubscriptionchange', event => {
    event.waitUntil((async () => {
        const cfg = await idbGet('pushConfig');
        if (!cfg || !cfg.server || !cfg.publicKey) return;
        const sub = await self.registration.pushManager.subscribe({
            userVisibleOnly: true,
            applicationServerKey: b64uToBytes(cfg.publicKey)
        });
        const reminders = (await idbGet('reminders')) || [];
        await fetch(cfg.server + '/sync', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ subscription: sub.toJSON(), reminders })
        });
    })().catch(() => {}));
});

// ---------- Нажатие на уведомление: открыть или показать приложение ----------
self.addEventListener('notificationclick', event => {
    event.notification.close();
    const target = (event.notification.data && event.notification.data.url) || './index.html';
    event.waitUntil(
        self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
            for (const client of list) {
                if ('focus' in client) return client.focus();
            }
            return self.clients.openWindow(target);
        })
    );
});
