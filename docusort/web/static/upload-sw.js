// DocuSort upload service worker.
//
// Files picked on /upload are stashed as Blobs in IndexedDB. This worker
// drains that queue against POST /upload — independently of whether the
// page is still open. The browser keeps the worker alive for ~30–120 s
// after the last client closes, which is plenty for a few dozen PDFs
// over a LAN/Tailscale link.
//
// If the worker does get killed mid-drain, the next page load re-sends
// whichever items still have a file blob but no inbox_name. Server-side
// SHA256 dedup catches any double-uploads cleanly.

const DB_NAME = "docusort-uploads";
const DB_VERSION = 1;
const STORE = "items";
const MAX_CONCURRENT = 4;
const BC_NAME = "docusort-upload";

// ---------- IndexedDB helpers ----------

function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE)) {
        db.createObjectStore(STORE, { keyPath: "id", autoIncrement: true });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function txStore(db, mode) {
  return db.transaction(STORE, mode).objectStore(STORE);
}

async function listPending() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const results = [];
    const req = txStore(db, "readonly").openCursor();
    req.onsuccess = () => {
      const cursor = req.result;
      if (!cursor) return resolve(results);
      const it = cursor.value;
      if (it.stage === "pending-upload" && it.file) results.push(it);
      cursor.continue();
    };
    req.onerror = () => reject(req.error);
  });
}

async function putItem(item) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const req = txStore(db, "readwrite").put(item);
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error);
  });
}

// ---------- Broadcast to pages ----------

function broadcast(msg) {
  try {
    new BroadcastChannel(BC_NAME).postMessage(msg);
  } catch (e) { /* ignore */ }
}

// ---------- Drain loop ----------

let draining = false;

async function drain() {
  if (draining) return;
  draining = true;
  broadcast({ type: "drain-start" });

  try {
    while (true) {
      const pending = await listPending();
      if (pending.length === 0) break;

      const batch = pending.slice(0, MAX_CONCURRENT);
      await Promise.all(batch.map(uploadOne));
    }
  } catch (e) {
    console.error("drain error", e);
  } finally {
    draining = false;
    broadcast({ type: "drain-done" });
  }
}

async function uploadOne(item) {
  // Flip to 'uploading' and tell open pages.
  item.stage = "uploading";
  item.started_at = Date.now();
  await putItem({ ...item });
  broadcast({ type: "update", id: item.id, stage: "uploading" });

  try {
    const form = new FormData();
    form.append("files", item.file, item.originalName);
    const r = await fetch("/upload", { method: "POST", body: form });
    if (!r.ok) throw new Error("HTTP " + r.status);
    const res = await r.json();

    if (res.rejected && res.rejected.includes(item.originalName)) {
      item.stage = "error";
      item.message = "format";
    } else {
      const entry = (res.saved || []).find(
        (s) => s.original_name === item.originalName
      ) || (res.saved || [])[0];
      if (entry && entry.inbox_name) {
        item.inbox_name = entry.inbox_name;
        item.stage = "server-queued";
        item.file = null; // free IDB space — server has the bytes now
      } else {
        item.stage = "error";
        item.message = "unclear";
      }
    }
  } catch (e) {
    item.stage = "error";
    item.message = String(e && e.message ? e.message : e);
  }

  item.updated_at = Date.now();
  await putItem({ ...item });
  broadcast({
    type: "update",
    id: item.id,
    stage: item.stage,
    inbox_name: item.inbox_name || null,
    message: item.message || null,
  });
}

// ---------- Lifecycle ----------

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (evt) => evt.waitUntil(self.clients.claim()));

self.addEventListener("message", (evt) => {
  if (!evt.data) return;
  if (evt.data.type === "drain") {
    evt.waitUntil(drain());
  } else if (evt.data.type === "ping") {
    evt.source && evt.source.postMessage({ type: "pong" });
  }
});
