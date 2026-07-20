/* Border Force — réception des alertes trains théoriques */
"use strict";

self.addEventListener("push", event => {
  let payload = {};
  try {
    payload = event.data ? event.data.json() : {};
  } catch (_) {
    payload = {
      title: "🚆 Train prévu",
      body: event.data ? event.data.text() : "Un train théorique est bientôt prévu."
    };
  }

  const title = payload.title || "🚆 Train prévu";
  const options = {
    body: payload.body || "Un train théorique est bientôt prévu.",
    icon: payload.icon || "../icon-192.png",
    badge: payload.badge || "../icon-192.png",
    tag: payload.tag || "borderforce-train-alert",
    renotify: false,
    requireInteraction: Boolean(payload.requireInteraction),
    data: {
      url: payload.url || "../index.html",
      eventId: payload.eventId || ""
    }
  };

  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", event => {
  event.notification.close();
  const targetUrl = new URL(
    event.notification.data?.url || "../index.html",
    self.registration.scope
  ).href;

  event.waitUntil((async () => {
    const windows = await clients.matchAll({
      type: "window",
      includeUncontrolled: true
    });

    for (const client of windows) {
      if ("focus" in client) {
        if ("navigate" in client) {
          await client.navigate(targetUrl).catch(() => {});
        }
        return client.focus();
      }
    }

    return clients.openWindow ? clients.openWindow(targetUrl) : undefined;
  })());
});
