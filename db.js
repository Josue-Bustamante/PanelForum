/*  ═══════════════════════════════════════════════════════════
    Firebase Realtime Database — REST API Client
    No SDK needed. Works with just the database URL.
    ═══════════════════════════════════════════════════════════ */

const DB_URL = 'https://panelforum-ffc93-default-rtdb.firebaseio.com';

const DB = {

  /* ── Write Operations ─────────────────────────────── */

  /** Add a new item with auto-generated key. Returns { name: "key" } */
  async push(path, data) {
    const res = await fetch(`${DB_URL}/${path}.json`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    });
    if (!res.ok) throw new Error(`DB push failed: ${res.status}`);
    return res.json();
  },

  /** Overwrite data at path */
  async set(path, data) {
    const res = await fetch(`${DB_URL}/${path}.json`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    });
    if (!res.ok) throw new Error(`DB set failed: ${res.status}`);
    return res.json();
  },

  /** Partial update at path */
  async update(path, data) {
    const res = await fetch(`${DB_URL}/${path}.json`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    });
    if (!res.ok) throw new Error(`DB update failed: ${res.status}`);
    return res.json();
  },

  /** Delete data at path */
  async remove(path) {
    const res = await fetch(`${DB_URL}/${path}.json`, { method: 'DELETE' });
    if (!res.ok) throw new Error(`DB remove failed: ${res.status}`);
  },

  /** Read data at path (one-time) */
  async get(path) {
    const res = await fetch(`${DB_URL}/${path}.json`);
    if (!res.ok) throw new Error(`DB get failed: ${res.status}`);
    return res.json();
  },

  /* ── Real-Time Listener (Server-Sent Events) ──────── */

  /**
   * Listen for real-time changes at a path.
   * @param {string} path - Database path (e.g. 'questions')
   * @param {function} onData - Called with current data on every change
   * @param {function} [onConnection] - Called with true/false for connection status
   * @returns {{ close: function }} - Call .close() to stop listening
   */
  listen(path, onData, onConnection) {
    let cache = null;
    const source = new EventSource(`${DB_URL}/${path}.json`);

    source.addEventListener('put', (e) => {
      try {
        const { path: eventPath, data } = JSON.parse(e.data);

        if (eventPath === '/') {
          cache = data;
        } else {
          if (cache === null) cache = {};
          const keys = eventPath.split('/').filter(k => k);
          let current = cache;
          for (let i = 0; i < keys.length - 1; i++) {
            if (!current[keys[i]]) current[keys[i]] = {};
            current = current[keys[i]];
          }
          const lastKey = keys[keys.length - 1];
          if (data === null) {
            delete current[lastKey];
          } else {
            current[lastKey] = data;
          }
        }

        onData(cache !== null ? JSON.parse(JSON.stringify(cache)) : null);
      } catch (err) {
        console.error('DB listen put error:', err);
      }
    });

    source.addEventListener('patch', (e) => {
      try {
        const { path: eventPath, data } = JSON.parse(e.data);

        if (cache === null) cache = {};

        if (eventPath === '/') {
          if (typeof cache === 'object' && typeof data === 'object') {
            Object.assign(cache, data);
          }
        } else {
          const keys = eventPath.split('/').filter(k => k);
          let current = cache;
          for (let i = 0; i < keys.length; i++) {
            if (!current[keys[i]]) current[keys[i]] = {};
            current = current[keys[i]];
          }
          if (typeof current === 'object' && typeof data === 'object') {
            Object.assign(current, data);
          }
        }

        onData(cache !== null ? JSON.parse(JSON.stringify(cache)) : null);
      } catch (err) {
        console.error('DB listen patch error:', err);
      }
    });

    source.onopen = () => {
      if (onConnection) onConnection(true);
    };

    source.onerror = () => {
      if (onConnection) onConnection(false);
    };

    return {
      close: () => source.close()
    };
  }
};
