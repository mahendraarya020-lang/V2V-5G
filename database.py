"""
Database Management — Supabase PostgreSQL
Menggantikan SQLite. Gunakan environment variables:
  SUPABASE_URL          = https://xxxx.supabase.co
  SUPABASE_SERVICE_KEY  = service_role key (bukan anon key)
"""
import os
import hashlib
import json
from supabase import create_client, Client

_client: Client = None

def _get_client() -> Client:
    global _client
    if _client is None:
        url = os.environ.get('SUPABASE_URL', '')
        key = os.environ.get('SUPABASE_SERVICE_KEY', '')
        if not url or not key:
            raise RuntimeError(
                'SUPABASE_URL dan SUPABASE_SERVICE_KEY harus di-set sebagai environment variables.'
            )
        _client = create_client(url, key)
    return _client


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def verify_password(password: str, hashed: str) -> bool:
    return hashlib.sha256(password.encode('utf-8')).hexdigest() == hashed


def verify_user(nim: str, password: str):
    """Verifikasi kredensial. Return dict {nim, name} atau None."""
    try:
        sb = _get_client()
        res = sb.table('users').select('password_hash, name').eq('nim', nim).execute()
        if res.data:
            row = res.data[0]
            if verify_password(password, row['password_hash']):
                return {'nim': nim, 'name': row['name']}
    except Exception as e:
        print(f'[DB] verify_user error: {e}')
    return None


def get_user_name(nim: str):
    """Ambil nama user berdasarkan NIM."""
    try:
        sb = _get_client()
        res = sb.table('users').select('name').eq('nim', nim).execute()
        if res.data:
            return res.data[0]['name']
    except Exception as e:
        print(f'[DB] get_user_name error: {e}')
    return None


def save_simulation(nim: str, experiment_id: str, config: dict, results: dict):
    """Simpan hasil simulasi ke Supabase."""
    try:
        sb = _get_client()
        sb.table('simulation_history').insert({
            'nim':               nim,
            'experiment_id':     experiment_id,
            'num_vehicles':      config.get('num_vehicles', 0),
            'num_platoons':      config.get('num_platoons', 1),
            'initial_spacing':   config.get('initial_spacing', 0),
            'desired_speed':     config.get('desired_speed', 0),
            'latency_ms':        config.get('latency_ms', 0),
            'packet_loss':       config.get('packet_loss', 0),
            'network_slicing':   config.get('network_slicing', 'URLLC'),
            'collision_occurred': 1 if results.get('collision_occurred', False) else 0,
            'duration_seconds':  results.get('duration', 0),
            'avg_spacing_error': results.get('avg_spacing_error', 0),
            'final_stability':   results.get('final_stability', 0),
            'config_json':       json.dumps(config),
            'results_json':      json.dumps(results),
        }).execute()
        print(f'[DB] Saved simulation: {experiment_id[:20]}...')
    except Exception as e:
        print(f'[DB] save_simulation error: {e}')


def get_user_history(nim: str) -> list:
    """Ambil semua riwayat simulasi milik user."""
    try:
        sb = _get_client()
        res = (sb.table('simulation_history')
                 .select('*')
                 .eq('nim', nim)
                 .order('timestamp', desc=True)
                 .execute())
        return res.data or []
    except Exception as e:
        print(f'[DB] get_user_history error: {e}')
        return []


def get_simulation_detail(sim_id: int, nim: str):
    """Ambil detail satu simulasi (hanya milik nim tersebut)."""
    try:
        sb = _get_client()
        res = (sb.table('simulation_history')
                 .select('*')
                 .eq('id', sim_id)
                 .eq('nim', nim)
                 .execute())
        return res.data[0] if res.data else None
    except Exception as e:
        print(f'[DB] get_simulation_detail error: {e}')
        return None


def user_owns_experiment(nim: str, experiment_id: str) -> bool:
    """True jika experiment_id ada di riwayat simulasi user."""
    if not nim or not experiment_id:
        return False
    try:
        sb = _get_client()
        res = (sb.table('simulation_history')
                 .select('id')
                 .eq('nim', nim)
                 .eq('experiment_id', experiment_id)
                 .limit(1)
                 .execute())
        return bool(res.data)
    except Exception as e:
        print(f'[DB] user_owns_experiment error: {e}')
        return False
