-- ============================================================
-- SUPABASE SETUP — Platform Platooning V2V & 5G
-- Jalankan file ini di Supabase SQL Editor
-- https://supabase.com/dashboard → project → SQL Editor
-- ============================================================

-- 1. Enable pgcrypto untuk SHA256 hashing
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 2. Tabel users
CREATE TABLE IF NOT EXISTS public.users (
    nim        TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Tabel simulation_history
CREATE TABLE IF NOT EXISTS public.simulation_history (
    id                BIGSERIAL PRIMARY KEY,
    nim               TEXT NOT NULL REFERENCES public.users(nim),
    experiment_id     TEXT NOT NULL,
    timestamp         TIMESTAMPTZ DEFAULT NOW(),
    num_vehicles      INTEGER,
    num_platoons      INTEGER,
    initial_spacing   REAL,
    desired_speed     REAL,
    latency_ms        REAL,
    packet_loss       REAL,
    network_slicing   TEXT,
    collision_occurred INTEGER,
    duration_seconds  REAL,
    avg_spacing_error REAL,
    final_stability   REAL,
    config_json       TEXT,
    results_json      TEXT
);

-- 4. Row Level Security — hanya service_role yang bisa akses (backend Python)
ALTER TABLE public.users            ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.simulation_history ENABLE ROW LEVEL SECURITY;

-- Izinkan service_role (backend) akses penuh
CREATE POLICY "service_role_all_users"
    ON public.users FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY "service_role_all_history"
    ON public.simulation_history FOR ALL TO service_role USING (true) WITH CHECK (true);

-- 5. Seed default users (password = NIM masing-masing, di-hash SHA256)
INSERT INTO public.users (nim, password_hash, name) VALUES
    ('1101223157', encode(digest('1101223157', 'sha256'), 'hex'), 'Mahendra Aryaputra Fitrianto'),
    ('1101223332', encode(digest('1101223332', 'sha256'), 'hex'), 'Muhammad Abduh'),
    ('1101223172', encode(digest('1101223172', 'sha256'), 'hex'), 'Ahmad Zulfikar')
ON CONFLICT (nim) DO NOTHING;

-- 6. Index untuk query cepat
CREATE INDEX IF NOT EXISTS idx_history_nim ON public.simulation_history(nim);
CREATE INDEX IF NOT EXISTS idx_history_ts  ON public.simulation_history(timestamp DESC);
