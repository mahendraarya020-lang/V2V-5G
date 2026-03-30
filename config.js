/**
 * config.js — Shared configuration untuk semua halaman frontend
 * WAJIB di-load SETELAH jQuery dan SEBELUM script halaman.
 *
 * Ganti nilai API_BASE dengan URL Render Anda setelah deploy:
 *   https://nama-app-anda.onrender.com
 *
 * Untuk development lokal: kosongkan string → pakai URL relatif.
 */
(function () {
    'use strict';

    // ── 1. API Base URL ──────────────────────────────────────────
    // Jika di-set, semua request ke '/api/...' otomatis diubah ke
    // 'https://...onrender.com/api/...'
    window.API_BASE = window.__API_BASE__ || '';
    // window.API_BASE = 'https://NAMA-APP-ANDA.onrender.com';  // ← uncomment & isi setelah deploy Render

    // ── 2. Auth helpers ──────────────────────────────────────────
    window.AUTH = {
        getToken:   function () { return localStorage.getItem('v2v_token'); },
        setToken:   function (t) { localStorage.setItem('v2v_token', t); },
        getName:    function () { return localStorage.getItem('v2v_name') || ''; },
        getNim:     function () { return localStorage.getItem('v2v_nim') || ''; },
        setUser:    function (token, name, nim) {
            localStorage.setItem('v2v_token', token);
            localStorage.setItem('v2v_name', name || '');
            localStorage.setItem('v2v_nim', nim || '');
        },
        clear:      function () {
            localStorage.removeItem('v2v_token');
            localStorage.removeItem('v2v_name');
            localStorage.removeItem('v2v_nim');
        },
        isLoggedIn: function () { return !!localStorage.getItem('v2v_token'); },
        requireLogin: function () {
            if (!localStorage.getItem('v2v_token')) {
                window.location.href = '/login.html';
                return false;
            }
            return true;
        },
        logout: function () {
            this.clear();
            window.location.href = '/login.html';
        }
    };

    // ── 3. jQuery interceptor (auto-auth + URL rewrite) ──────────
    // Dipasang saat jQuery sudah tersedia (bisa di-load setelahnya).
    function setupJQuery() {
        if (typeof $ === 'undefined') return;

        // Ubah URL relatif → absolute (Render URL) jika API_BASE di-set
        $.ajaxPrefilter(function (options) {
            if (window.API_BASE && options.url && options.url.charAt(0) === '/') {
                options.url = window.API_BASE + options.url;
            }
        });

        // Tambahkan Authorization header ke semua request
        $(document).ajaxSend(function (evt, xhr) {
            var token = window.AUTH.getToken();
            if (token) {
                xhr.setRequestHeader('Authorization', 'Bearer ' + token);
            }
        });

        // Tangkap 401 global → redirect ke login
        $(document).ajaxError(function (evt, xhr) {
            if (xhr.status === 401) {
                window.AUTH.clear();
                window.location.href = '/login.html';
            }
        });
    }

    // Coba pasang sekarang; kalau jQuery belum ada, tunggu DOMContentLoaded
    if (typeof $ !== 'undefined') {
        setupJQuery();
    } else {
        document.addEventListener('DOMContentLoaded', setupJQuery);
    }

    // ── 4. SocketIO helper ───────────────────────────────────────
    // Gunakan window.createSocket() agar semua halaman pakai URL + auth yang sama.
    window.createSocket = function (opts) {
        var base = window.API_BASE || window.location.origin;
        var token = window.AUTH.getToken();
        var defaults = {
            transports: ['websocket', 'polling'],
            reconnection: true,
            auth: token ? { token: token } : {}
        };
        return io(base, Object.assign({}, defaults, opts || {}));
    };

})();
