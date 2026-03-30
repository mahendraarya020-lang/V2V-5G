"""
V2V 5G PLATOONING RESEARCH SIMULATOR
Research-Grade Academic Build v5.0
=====================================
Semua gap sebelumnya tetap ada, ditambah:
  NEW-01: API & Socket untuk tambah platoon saat simulasi berjalan
  NEW-02: API & Socket transfer kendaraan antar platoon (A->B)
  NEW-03: Perbaikan logika predecessor saat platoon berubah dinamis
  NEW-04: Tukar leader antar platoon (swap_leaders) via API + Socket
  NEW-05: Promosi leader baru dalam platoon (promote_next_leader) via API + Socket
  VIS-01: Jalan multi-lajur â€” setiap platoon punya lajur sendiri
  VIS-02: Kendaraan berjalan berdampingan sesuai platoon masing-masing

Referensi Teori:
  - Ploeg et al. (2011) CACC PID+FF
  - Naus et al. (2010) String stability: AR_i < 1
  - 3GPP TR 38.885 V2V path loss
  - Brecht et al. (2018) ECDSA-256 delay
  - Kaul et al. (2012) AoI M/D/1
  - Bergenhem et al. (2012) Platoon split/merge/leader election
"""

from flask import Flask, render_template, request, jsonify, session, send_file, redirect, url_for, abort
from werkzeug.utils import safe_join
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from flask_jwt_extended import (
    JWTManager, create_access_token, jwt_required,
    get_jwt_identity, verify_jwt_in_request, decode_token
)
import os, sys, json, time, threading, random, math, csv
from datetime import datetime, timedelta
from collections import deque
from functools import wraps
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
except ImportError:
    pass

from database import verify_user, get_user_name, save_simulation, get_user_history, get_simulation_detail, user_owns_experiment

app = Flask(__name__, template_folder='../frontend', static_folder='../frontend')
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'v2v-research-platform-2025')
app.config['JWT_SECRET_KEY'] = os.environ.get('JWT_SECRET_KEY', 'v2v-jwt-secret-2025')
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(hours=12)

# CORS: izinkan Netlify domain dan localhost
_origins = os.environ.get('ALLOWED_ORIGINS', 'http://localhost:5500,http://127.0.0.1:5500').split(',')
CORS(app, origins=_origins, supports_credentials=True)
JWTManager(app)

socketio = SocketIO(app, async_mode='threading', cors_allowed_origins="*",
                    ping_timeout=120, ping_interval=30)

# Menyimpan nim user per SocketIO session id (untuk stop_simulation)
_socket_users: dict = {}

LOGS_DIR = '../data/logs'
ANALYSIS_DIR = '../data/analysis'
for d in [LOGS_DIR, ANALYSIS_DIR]:
    os.makedirs(d, exist_ok=True)

# Logo & aset kampus: folder ASET di root proyek (samping folder v11_fixed), bukan di dalam frontend
ASET_PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'ASET'))


@app.route('/ASET/<path:filename>')
def serve_project_aset(filename):
    """Layani file dari EKSPERIMEN 41/ASET/ (nama file boleh berisi spasi)."""
    if not os.path.isdir(ASET_PROJECT_DIR):
        abort(404)
    path = safe_join(ASET_PROJECT_DIR, filename)
    if path is None or not os.path.isfile(path):
        abort(404)
    return send_file(path, max_age=86400)

# ============================================================
# PACKET BUFFER
# ============================================================
class PacketBuffer:
    def __init__(self):
        self.packets = deque()
    def add_packet(self, packet, release_time):
        self.packets.append({'data': packet, 'release_time': release_time})
    def get_ready_packets(self, current_time):
        ready = []
        while self.packets and self.packets[0]['release_time'] <= current_time:
            ready.append(self.packets.popleft()['data'])
        return ready
    def clear(self):
        self.packets.clear()

# ============================================================
# G-02: 3GPP TR 38.885 CHANNEL MODEL
# ============================================================
class ChannelModel5G:
    PL_0         = 38.77
    n_los        = 1.77
    n_nlos       = 3.50
    sigma_shadow = 3.0
    TX_power_dBm = 23.0
    noise_dBm    = -101.0
    SINR_min_dB  = 5.0

    def packet_loss_prob(self, dist_m, is_los=True, base_loss=0.01):
        if dist_m < 1.0: dist_m = 1.0
        n   = self.n_los if is_los else self.n_nlos
        PL  = self.PL_0 + 10.0 * n * math.log10(dist_m)
        PL += random.gauss(0, self.sigma_shadow)
        rx_power = self.TX_power_dBm - PL
        sinr_dB  = rx_power - self.noise_dBm
        sinr_gap = sinr_dB - self.SINR_min_dB
        phy_loss = 1.0 / (1.0 + math.exp(sinr_gap * 0.8))
        return min(0.95, max(base_loss, phy_loss))

channel_model = ChannelModel5G()

# ============================================================
# MANEUVER QUEUE â€” Deadlock prevention for N>2 platoons
# Ref: Dokumen Â§7.5 Theorem 4.1 (Isolation Transfer)
# ============================================================
class ManeuverQueue:
    """
    Serializes concurrent maneuver requests. Prevents concurrent operations
    on overlapping platoons (race condition / deadlock prevention).
    Ref: Â§7.5.1 Pencegahan Deadlock, Â§4.6 Teorema 4.1
    """
    def __init__(self):
        self._active_platoons = set()   # platoon IDs currently in maneuver
        self._lock = threading.Lock()
        self._history = deque(maxlen=50)

    def try_acquire(self, platoon_ids: list) -> bool:
        """Returns True if all platoon_ids are free (no ongoing maneuver)."""
        with self._lock:
            for pid in platoon_ids:
                if pid in self._active_platoons:
                    return False
            for pid in platoon_ids:
                self._active_platoons.add(pid)
            return True

    def release(self, platoon_ids: list):
        with self._lock:
            for pid in platoon_ids:
                self._active_platoons.discard(pid)

    def log_maneuver(self, maneuver_type, platoon_ids, result):
        with self._lock:
            self._history.append({
                'time': time.time(), 'type': maneuver_type,
                'platoons': platoon_ids, 'success': result
            })

    def get_status(self):
        with self._lock:
            return {
                'active_platoons': list(self._active_platoons),
                'queue_busy': len(self._active_platoons) > 0,
                'history': list(self._history)[-10:]
            }

# ============================================================
# 5G NETWORK
# ============================================================
class Network5G:
    def __init__(self, config):
        self.latency_ms      = config.get('latency_ms', 10.0)
        self.packet_loss     = config.get('packet_loss', 1.0) / 100.0
        self.jitter_ms       = config.get('jitter_ms', 2.0)
        self.bandwidth_mbps  = config.get('bandwidth_mbps', 100)
        self.network_slicing = config.get('network_slicing', 'URLLC')
        self.rsu_enabled     = config.get('rsu_enabled', True)
        # --- V2I / edge (Bab 1: bantuan infrastruktur mengurangi beban multi-hop) ---
        self.rsu_position_m   = float(config.get('rsu_position_m', 460))
        self.rsu_radius_m     = float(config.get('rsu_radius_m', 95))
        self.rsu_latency_mult = float(config.get('rsu_latency_mult', 0.78))
        self.rsu_loss_mult    = float(config.get('rsu_loss_mult', 0.82))
        # --- Lingkungan dinamis: segmen "tikungan" pada sumbu posisi 1D (Bab 1: NLOS, interferensi) ---
        self.curve_enabled     = bool(config.get('curve_enabled', False))
        self.curve_start_m     = float(config.get('curve_start_m', 380))
        self.curve_end_m       = float(config.get('curve_end_m', 500))
        self.curve_loss_add    = float(config.get('curve_loss_add', 0.06))
        self.curve_delay_add_ms = float(config.get('curve_delay_add_ms', 5.0))
        self.curve_forced_nlos = bool(config.get('curve_forced_nlos', True))
        self._last_env         = {'in_curve': False, 'rsu_relay': False}
        self.security_delay_mean = config.get('security_delay_ms', 3.0)
        self.security_delay_std  = 0.5
        self.propagation_delay  = 2.0
        self.transmission_delay = 1.0
        self.processing_delay   = 3.0
        # RSU aktif → antrian edge lebih pendek (disederhanakan)
        self.queuing_delay      = (config.get('rsu_queuing_ms', 0.9) if self.rsu_enabled else 2.0)
        self.packet_buffers = {}
        self.packets_sent = 0
        self.packets_received = 0
        self.packets_lost = 0
        self.delay_history = deque(maxlen=200)
        self.current_delay = self.latency_ms
        self.last_delay_components = {}
        self.degraded = False
        self.degradation_timer = 0.0
        self.link_latency = {}
        self.sec_delay_samples = deque(maxlen=200)
        # P-01 FIX: lock khusus untuk operasi buffer agar atomis
        # Mencegah race condition antara Thread-A (step/read) dan Thread-B (transfer/inject)
        self._buffer_lock = threading.Lock()

    def update(self, dt):
        if self.degraded:
            self.degradation_timer -= dt
            if self.degradation_timer <= 0:
                self.degraded = False
                self.packet_loss = 0.01
                self.queuing_delay = (0.9 if self.rsu_enabled else 2.0)  # konsisten RSU

    @staticmethod
    def _segment_overlaps(lo, hi, a, b):
        """True jika [lo,hi] berpotongan dengan [a,b] pada garis 1D."""
        return not (hi < a or lo > b)

    def transmit(self, packet, current_time, follower_id, sender_pos=0, receiver_pos=0, num_blockers=0):
        self.packets_sent += 1
        dist    = abs(sender_pos - receiver_pos) if abs(sender_pos - receiver_pos) > 1 else 15.0
        lo, hi  = min(sender_pos, receiver_pos), max(sender_pos, receiver_pos)
        in_curve = self.curve_enabled and self._segment_overlaps(lo, hi, self.curve_start_m, self.curve_end_m)
        rsu_lo, rsu_hi = self.rsu_position_m - self.rsu_radius_m, self.rsu_position_m + self.rsu_radius_m
        rsu_relay = self.rsu_enabled and self._segment_overlaps(lo, hi, rsu_lo, rsu_hi)
        self._last_env = {'in_curve': in_curve, 'rsu_relay': rsu_relay}

        eff_loss = self.packet_loss
        if in_curve:
            eff_loss = min(0.92, eff_loss + self.curve_loss_add)
        if rsu_relay:
            eff_loss = min(0.92, eff_loss * self.rsu_loss_mult)

        is_los = (num_blockers == 0) and not (in_curve and self.curve_forced_nlos)
        phys_loss = channel_model.packet_loss_prob(dist, is_los, eff_loss)
        if random.random() < phys_loss:
            self.packets_lost += 1
            return None
        net_delay = self._calc_network_delay()
        if in_curve:
            net_delay += self.curve_delay_add_ms
        if rsu_relay:
            net_delay *= self.rsu_latency_mult
        net_delay = max(1.0, net_delay)
        sec_delay = max(1.0, min(10.0, random.gauss(self.security_delay_mean, self.security_delay_std)))
        self.sec_delay_samples.append(sec_delay)
        total_delay_ms = net_delay + sec_delay
        self.current_delay = total_delay_ms
        self.delay_history.append(total_delay_ms)
        self.packets_received += 1
        if follower_id not in self.link_latency:
            self.link_latency[follower_id] = deque(maxlen=100)
        self.link_latency[follower_id].append(total_delay_ms)
        self.last_delay_components = {
            'network':  round(net_delay, 2),
            'security': round(sec_delay, 2),
            'total':    round(total_delay_ms, 2)
        }
        release_time = current_time + (total_delay_ms / 1000.0)
        pkt = packet.copy()
        pkt['network_delay']  = net_delay
        pkt['security_delay'] = sec_delay
        pkt['total_delay']    = total_delay_ms
        pkt['sent_time']      = current_time
        # P-01 FIX: proteksi buffer dengan lock agar atomis dengan inject dan read
        with self._buffer_lock:
            if follower_id not in self.packet_buffers:
                self.packet_buffers[follower_id] = PacketBuffer()
            self.packet_buffers[follower_id].add_packet(pkt, release_time)
        return total_delay_ms

    def get_packets_for_follower(self, follower_id, current_time):
        # P-01 FIX: proteksi dengan lock agar tidak berpotongan dengan inject/clear
        with self._buffer_lock:
            if follower_id not in self.packet_buffers:
                return []
            return self.packet_buffers[follower_id].get_ready_packets(current_time)

    def inject_immediate_atomic(self, follower_id, pkt, current_time):
        """
        P-01 FIX: Operasi clear + inject dalam SATU lock yang sama.
        Ini memastikan Thread-A tidak bisa membaca buffer di antara clear dan inject.
        Kunci: _buffer_lock yang sama digunakan oleh transmit(), get_packets_for_follower(),
        dan inject_immediate_atomic() sehingga tidak ada interleaving yang tidak aman.
        """
        with self._buffer_lock:
            if follower_id not in self.packet_buffers:
                self.packet_buffers[follower_id] = PacketBuffer()
            # Clear paket lama dan langsung inject paket baru dalam satu operasi atomis
            self.packet_buffers[follower_id].clear()
            self.packet_buffers[follower_id].add_packet(pkt, current_time)

    def clear_buffer_atomic(self, follower_id):
        """P-01 FIX: Clear buffer dengan proteksi lock."""
        with self._buffer_lock:
            if follower_id in self.packet_buffers:
                self.packet_buffers[follower_id].clear()

    def _calc_network_delay(self):
        total = (self.propagation_delay + self.transmission_delay +
                 self.processing_delay + self.queuing_delay)
        total += random.uniform(-self.jitter_ms, self.jitter_ms)
        if self.network_slicing == 'eMBB':   total *= 1.5
        elif self.network_slicing == 'mMTC': total *= 2.0
        return max(1.0, total)

    def get_current_delay(self):
        return self.current_delay

    def inject_degradation(self, duration=5.0):
        self.degraded = True
        self.degradation_timer = duration
        self.packet_loss = min(0.3, self.packet_loss * 10)
        self.queuing_delay *= 3

    def get_metrics(self):
        pdr = (self.packets_received / self.packets_sent * 100) if self.packets_sent > 0 else 100
        avg_delay = sum(self.delay_history)/len(self.delay_history) if self.delay_history else self.latency_ms
        sec_mean = sum(self.sec_delay_samples)/len(self.sec_delay_samples) if self.sec_delay_samples else self.security_delay_mean
        link_avg = {str(k): round(sum(v)/len(v), 1) for k, v in self.link_latency.items() if v}
        env = dict(self._last_env)
        env.update({
            'curve_enabled': self.curve_enabled,
            'curve_m': [self.curve_start_m, self.curve_end_m],
            'rsu_m': self.rsu_position_m, 'rsu_radius_m': self.rsu_radius_m,
        })
        return {
            'latency_ms': self.latency_ms, 'packet_loss': round(self.packet_loss*100, 2),
            'jitter_ms': self.jitter_ms, 'bandwidth_mbps': self.bandwidth_mbps,
            'network_slicing': self.network_slicing, 'rsu_enabled': self.rsu_enabled,
            'environment': env,
            'packets_sent': self.packets_sent, 'packets_received': self.packets_received,
            'packets_lost': self.packets_lost, 'pdr': round(pdr, 1),
            'avg_delay': round(avg_delay, 1), 'degraded': self.degraded,
            'security_delay_ms': round(sec_mean, 2), 'security_delay_std': round(self.security_delay_std, 2),
            'delay_components': self.last_delay_components, 'link_latency': link_avg,
            'delay_breakdown': {
                'propagation': self.propagation_delay, 'transmission': self.transmission_delay,
                'processing':  self.processing_delay,  'queuing':      self.queuing_delay,
                'security':    round(sec_mean, 2)
            }
        }

# ============================================================
# FSM - 4 STATE BERBASIS AOI
# Ref: Kaul et al. (2012), Ploeg et al. (2011)
# ============================================================
class VehicleFSM:
    STATE_CACC      = 'CACC'
    STATE_DEGRADED  = 'DEGRADED'
    STATE_ACC       = 'ACC'
    STATE_EMERGENCY = 'EMERGENCY'
    STATE_TRANSFER  = 'TRANSFER'   # Cooldown setelah manuver (Ref: Â§4.3.4, Â§8.1)
    # h_maneuver = 1.5 Ã— h_normal saat cooldown â€” Ref: Â§8.6 Proposisi 8.1
    HEADWAY_MULT = {'CACC':1.0,'DEGRADED':1.3,'ACC':1.8,'EMERGENCY':2.5,'TRANSFER':1.5}

    def __init__(self, thresholds=None):
        self.state = self.STATE_CACC
        self.aoi   = 0.0
        self.last_packet_sim_time = None
        self.emergency_braking = False
        t = thresholds or {}
        self.thr_cacc_degrade = t.get('aoi_cacc_degrade', 0.100)
        self.thr_degrade_acc  = t.get('aoi_degrade_acc',  0.500)
        self.thr_rec_cacc     = 0.080
        self.thr_rec_degrade  = 0.400
        self.EMERGENCY_GAP    = 12.0  # FIX-COL-1: 5m->12m deteksi bahaya lebih awal
        # Justifikasi: rv_max~10m/s, rel_decel~6m/s^2 -> min stopping dist=rv^2/2a=8.3m + margin 3m
        # Ref: GCDC (2016) S-2 safety envelope.
        self.EMERGENCY_ACCEL  = -3.5  # FIX-COL-2: threshold lebih ringan agar cepat aktif

    def packet_received(self, sim_time):
        self.last_packet_sim_time = sim_time
        self.aoi = 0.0

    def update(self, dt, sim_time, gap, a_actual, transfer_cooldown=0.0, stabilization_window=0.0):
        if self.last_packet_sim_time is not None:
            self.aoi = sim_time - self.last_packet_sim_time
        else:
            self.aoi += dt
        prev = self.state
        # P-03 FIX: Selama stabilization_window aktif (pasca-transfer), longgarkan threshold EMERGENCY.
        # Ini mencegah data posisi stale memicu EMERGENCY yang tidak perlu.
        # Threshold dikembalikan ke normal setelah window habis.
        if stabilization_window > 0.0:
            eff_emrg_gap   = self.EMERGENCY_GAP * 0.3    # 12m -> 3.6m: hanya bahaya fisik nyata
            eff_emrg_accel = self.EMERGENCY_ACCEL * 2.0  # -3.5 -> -7.0: hanya rem darurat penuh
        else:
            eff_emrg_gap   = self.EMERGENCY_GAP
            eff_emrg_accel = self.EMERGENCY_ACCEL
        # TRANSFER state overrides normal FSM saat cooldown aktif
        if transfer_cooldown > 0:
            if gap < eff_emrg_gap or a_actual < eff_emrg_accel:
                self.state = self.STATE_EMERGENCY
                self.emergency_braking = True
            else:
                self.emergency_braking = False
                # FIX-TRANSFER-RECOVERY: Recovery dari EMERGENCY di TRANSFER branch.
                if self.state == self.STATE_EMERGENCY:
                    if gap > self.EMERGENCY_GAP * 1.2:  # FIX3: 1.8 -> 1.2 (14.4m)
                        self.state = self.STATE_TRANSFER
                        self.emergency_braking = False
                else:
                    self.state = self.STATE_TRANSFER
            return self.state, prev
        if gap < eff_emrg_gap or a_actual < eff_emrg_accel:
            self.state = self.STATE_EMERGENCY
            self.emergency_braking = True
        else:
            self.emergency_braking = False
            if self.state == self.STATE_EMERGENCY:
                # FIX3: Recovery threshold diturunkan 1.8 -> 1.2 agar bisa pulih lebih cepat
                if gap > self.EMERGENCY_GAP * 1.2:
                    self.state = self.STATE_CACC if self.aoi < self.thr_cacc_degrade else self.STATE_DEGRADED
                    self.emergency_braking = False
            elif self.state in (self.STATE_CACC, self.STATE_TRANSFER):
                if self.aoi > self.thr_cacc_degrade: self.state = self.STATE_DEGRADED
                else: self.state = self.STATE_CACC
            elif self.state == self.STATE_DEGRADED:
                if self.aoi > self.thr_degrade_acc: self.state = self.STATE_ACC
                elif self.aoi < self.thr_rec_cacc:  self.state = self.STATE_CACC
            elif self.state == self.STATE_ACC:
                if self.aoi < self.thr_rec_degrade: self.state = self.STATE_DEGRADED
        return self.state, prev

    def get_headway_multiplier(self):
        return self.HEADWAY_MULT.get(self.state, 1.0)

    def reset(self):
        """Reset FSM ke CACC - dipakai saat transfer platoon."""
        self.state = self.STATE_CACC
        self.aoi   = 0.0
        self.last_packet_sim_time = None
        self.emergency_braking = False

def _platoon_chain_key(v):
    """Urutan rantai CACC: leader selalu indeks 0 meski reposisi longitudinal belum selesai (§5.2)."""
    return (-int(v.is_leader), -v.position)


# ============================================================
# VEHICLE - PID+FF + Actuation Lag + FSM
# Ref: Ploeg et al. (2011)
# ============================================================
class Vehicle:
    def __init__(self, vid, position, velocity, is_leader, platoon_id, config):
        self.id = vid; self.position = position; self.velocity = velocity
        self.is_leader = is_leader; self.platoon_id = platoon_id; self.length = 5.0
        self.a_desired = 0.0; self.a_actual = 0.0
        self.tau_act   = config.get('tau_act', 0.25)
        self.acceleration = 0.0
        self.kp = config.get('kp', 0.5); self.kd = config.get('kd', 0.5)  # FIX-COL-3: kd 0.3->0.5 lebih stabil
        self.ki = config.get('ki', 0.01); self.alpha_ff = config.get('alpha', 0.9)
        self.integral_e = 0.0; self.I_MAX = 5.0
        self.A_CMD_MAX = 3.0; self.A_CMD_MIN = -6.0  # FIX-COL-4: sesuaikan dengan max_decel
        self.time_headway = config.get('time_headway', 0.8)
        self.time_headway_base = self.time_headway  # untuk kontrol adaptif joint comm–control
        self.max_accel = config.get('max_accel', 3.0)
        self.max_decel = -config.get('max_decel', 6.0)
        self.max_velocity = 35.0
        self.desired_gap  = config.get('initial_spacing', 15.0)
        self.spacing_error = 0.0
        self.topology = config.get('topology', 'predecessor')
        self.leader_data = None
        self.fsm = VehicleFSM({'aoi_cacc_degrade': config.get('aoi_cacc_degrade',0.100),
                                'aoi_degrade_acc':  config.get('aoi_degrade_acc',0.500)})
        self.control_mode = 'LEADER' if is_leader else 'CACC'
        self.communication_ok = True
        self.last_packet_time = None; self.last_leader_data = None
        self.timeout_threshold = config.get('v2v_timeout', 200) / 1000.0
        self.collided = False
        self.aoi_peak_history = deque(maxlen=500)
        self.aoi_running_peak = 0.0
        self.transfer_cooldown = 0.0   # NEW-02: cooldown setelah transfer
        # â”€â”€ MULTI-PHASE TRANSFER (v7) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # maneuver_phase: None | 'DEPARTING' | 'IN_TRANSIT'
        # Sesuai Â§4.3 Protokol Transfer 4 Fase
        self.maneuver_phase = None
        self.transit_data   = {}   # {target_pid, depart_elapsed, depart_duration, ...}
        # P-02 FIX: grace period sebelum comm_ok=False â€” bedakan transisi sementara vs putus nyata
        # Selama _no_data_grace detik tanpa paket, communication_ok tetap True
        self._no_data_grace = 0.0   # detik; diset saat transfer, kembali 0 setelah stabil
        # P-03 FIX: window stabilisasi pasca-transfer â€” longgarkan threshold EMERGENCY sementara
        self.stabilization_window = 0.0   # detik; dikurangi setiap step
        # FIX4: Counter untuk multi-frame packet re-injection pasca-transfer
        self._post_transfer_inject_frames = 0  # jumlah frame sisa untuk re-inject paket
        # Reposisi halus menuju slot baru (swap leader §6.4 / promosi §5.2) — hindari teleportasi visual
        self._reposition_target = None
        self._reposition_velocity = None
        self._reposition_delay_remaining = 0.0   # penundaan sebelum gerak (skenario-bergantung)
        self._reposition_speed_cap = None        # batas kecepatan reposisi m/s; None = pakai global engine

    def _apply_actuation_lag(self, dt, bypass=False):
        # FIX-COL-5: mode bypass untuk EMERGENCY agar rem langsung penuh tanpa lag
        if bypass or self.tau_act <= 0:
            self.a_actual = self.a_desired
        else:
            self.a_actual += (dt/self.tau_act)*(self.a_desired - self.a_actual)
        self.a_actual = max(self.max_decel, min(self.max_accel, self.a_actual))
        self.acceleration = self.a_actual

    def _update_integral(self, e_pos, a_cmd, dt):
        if a_cmd >= self.A_CMD_MAX or a_cmd <= self.A_CMD_MIN:
            self.integral_e *= 0.95
        else:
            self.integral_e = max(-self.I_MAX, min(self.I_MAX, self.integral_e + e_pos*dt))

    def _pid_ff_control(self, l_pos, l_vel, l_accel, dt, net_delay_s=0.0, intruder=None):
        """
        CACC PID+FF - Ploeg et al. (2011) Eq.(5)-(7)
        Desired gap: d_i = r + h_i * v_i  (FOLLOWER velocity)
        FIX-COL-6: kompensasi posisi predecessor berdasarkan network delay
        FIX-COL-7: zona rem pre-emergency saat gap menyusut cepat
        Ref Â§8.6 Prop 8.1: h_maneuver = 1.5 * h_normal saat cooldown
        """
        # Boost headway saat cooldown aktif (Ref: Â§8.6 Proposisi 8.1)
        h_base = self.time_headway * (1.5 if self.transfer_cooldown > 0 else 1.0)
        hw  = h_base * self.fsm.get_headway_multiplier()
        d_g = self.desired_gap + hw * self.velocity

        # FIX-COL-6: estimasi posisi predecessor saat ini berdasarkan delay
        l_pos_est = l_pos + l_vel * net_delay_s
        a_g = l_pos_est - self.position - self.length
        if intruder and intruder.get('position') is not None:
            ip = intruder['position']
            if self.position < ip < l_pos_est:
                a_g = min(a_g, ip - self.position - self.length)
        e_pos = a_g - d_g
        e_vel = l_vel - self.velocity
        self.spacing_error = a_g - (self.desired_gap + self.time_headway * self.velocity)

        u_pid  = self.kp*e_pos + self.kd*e_vel + self.ki*self.integral_e
        a_cmd  = max(self.A_CMD_MIN, min(self.A_CMD_MAX, u_pid + self.alpha_ff*l_accel))

        # FIX-COL-7: pre-emergency braking â€” paksa rem jika jarak menyusut berbahaya
        PRE_EMRG = self.fsm.EMERGENCY_GAP * 2.0
        if a_g < PRE_EMRG and e_vel < -1.0:
            urgency = max(0.0, min(1.0, (PRE_EMRG - a_g) / PRE_EMRG))
            soft_brake = -urgency * 4.0 * (-e_vel / 5.0)
            a_cmd = min(a_cmd, soft_brake)

        self._update_integral(e_pos, a_cmd, dt)
        return a_cmd

    def update_leader(self, target_speed, dt, profile='normal', cut_in=None, accel_override=None):
        if profile == 'smooth':
            t = getattr(self, '_t', 0.0) + dt
            self._t = t
            target_speed = target_speed + 1.0 * math.sin(2*math.pi*0.1*t)
        elif profile == 'aggressive':
            if not hasattr(self, '_agg_timer') or self._agg_timer <= 0:
                self._agg_offset = random.uniform(-2.0, 2.0)
                self._agg_timer  = random.uniform(1.0, 3.0)
            self._agg_timer -= dt
            target_speed = target_speed + self._agg_offset
        speed_err = target_speed - self.velocity
        self.a_desired = max(self.max_decel, min(self.max_accel, 0.5*speed_err))
        if accel_override is not None:
            self.a_desired = max(self.max_decel, min(self.max_accel, accel_override))
        if cut_in and cut_in.get('position') is not None and cut_in['position'] > self.position:
            gap_i = cut_in['position'] - self.position - self.length
            if gap_i < 55.0:
                self.a_desired = min(self.a_desired, -min(6.0, max(0.0, (55.0 - gap_i) * 0.22)))
        self._apply_actuation_lag(dt)
        self.velocity  = max(0, min(self.max_velocity, self.velocity + self.a_actual*dt))
        self.position += self.velocity * dt
        if self.transfer_cooldown > 0: self.transfer_cooldown -= dt

    def update_follower(self, leader_packet, current_time, dt, leader_direct_packet=None, net_delay_s=0.0, intruder=None):
        """G-06: hybrid topology menggunakan akselerasi leader langsung."""
        # FIX-PRED-3: Tolak paket stale jika posisi predecessor ternyata di belakang
        # kendaraan ini â€” bisa terjadi saat predecessor baru saja berpindah platoon.
        l_pos_check = leader_packet.get('position', self.position)
        if l_pos_check <= self.position + self.length:
            # Paket tidak valid (predecessor di belakang kita) â†’ fallback ke ACC
            if self.id in (2, 3) and self.stabilization_window > 0:
                print(f'[DBG-STALE] V{self.id} REJECTED pkt: l_pos={l_pos_check:.1f} <= my_pos+len={self.position+self.length:.1f}')
            self.communication_ok = False
            self.last_packet_time = None
            self.update_without_data(dt, current_time, intruder=intruder)
            return

        self.last_packet_time = current_time
        self.last_leader_data = leader_packet
        self.communication_ok = True
        self.fsm.packet_received(current_time)
        l_pos   = leader_packet['position']
        l_vel   = leader_packet['velocity']
        l_accel = leader_packet.get('acceleration', 0.0)
        if self.topology == 'hybrid' and leader_direct_packet:
            l_accel = leader_direct_packet.get('acceleration', l_accel)
        gap = l_pos - self.position - self.length
        if intruder and intruder.get('position') is not None:
            ip = intruder['position']
            if self.position < ip < l_pos:
                gap = min(gap, ip - self.position - self.length)
        # DEBUG: trace EMERGENCY triggers for V2/V3 during transfer
        if self.id in (2, 3) and self.stabilization_window > 0:
            print(f'[DBG-UF] V{self.id} t={current_time:.2f} gap={gap:.1f} a_act={self.a_actual:.2f} stab={self.stabilization_window:.2f} grace={self._no_data_grace:.2f} cooldown={self.transfer_cooldown:.2f} pkt_pos={l_pos:.1f} my_pos={self.position:.1f}')
        state, _ = self.fsm.update(dt, current_time, gap, self.a_actual,
                                   self.transfer_cooldown, self.stabilization_window)
        self.control_mode = state
        self.aoi_running_peak = max(self.aoi_running_peak, self.fsm.aoi * 1000)

        # FIX2+5: EMERGENCY dengan rem proporsional berdasarkan gap sebenarnya
        if state == 'EMERGENCY':
            if gap < 5.0:
                # Bahaya nyata: rem penuh tanpa lag
                self.a_desired = self.max_decel   # -6 m/sÂ²
                self._apply_actuation_lag(dt, bypass=True)
            elif gap < self.fsm.EMERGENCY_GAP:
                # Zona transisi: rem proporsional
                urgency = max(0.0, 1.0 - (gap - 5.0) / (self.fsm.EMERGENCY_GAP - 5.0))
                self.a_desired = urgency * self.max_decel  # -6..0 proporsional
                self._apply_actuation_lag(dt)
            else:
                # Gap sudah aman (> EMERGENCY_GAP) tapi FSM belum recovery:
                # Gunakan ACC control untuk mengejar predecessor, JANGAN rem
                self.a_desired = self._acc_control(l_pos, l_vel, intruder)
                self._apply_actuation_lag(dt)
        elif state in ('CACC', 'DEGRADED'):
            ff = l_accel if state == 'CACC' else 0.0
            self.a_desired = self._pid_ff_control(l_pos, l_vel, ff, dt, net_delay_s, intruder)
            self._apply_actuation_lag(dt)
        else:
            self.a_desired = self._acc_control(l_pos, l_vel, intruder)
            self._apply_actuation_lag(dt)
        self.velocity  = max(0, min(self.max_velocity, self.velocity + self.a_actual*dt))
        self.position += self.velocity * dt
        if self.transfer_cooldown > 0: self.transfer_cooldown -= dt
        if self.stabilization_window > 0: self.stabilization_window -= dt
        if self._no_data_grace > 0: self._no_data_grace -= dt

    def update_without_data(self, dt, current_time, intruder=None):
        # P-02 FIX: Grace period â€” jangan langsung vonis comm=False saat transisi sementara.
        if self._no_data_grace > 0.0 and self.last_packet_time is not None:
            time_since_last = current_time - self.last_packet_time
            if time_since_last <= self._no_data_grace:
                self.communication_ok = True   # masih dalam grace, anggap terhubung
            else:
                self.communication_ok = False
                self._no_data_grace = 0.0      # grace habis
        elif self.stabilization_window > 0.0:
            # Selama stabilization_window aktif, paksa comm_ok = True
            self.communication_ok = True
        else:
            self.communication_ok = False
        if self.last_leader_data:
            gap  = self.last_leader_data['position'] - self.position - self.length
            lvel = self.last_leader_data.get('velocity', None)
            lp = self.last_leader_data['position']
            if intruder and intruder.get('position') is not None:
                ip = intruder['position']
                if self.position < ip < lp:
                    gap = min(gap, ip - self.position - self.length)
        else:
            gap  = 999.0
            lvel = None
        # FIX-TRANSFER-DISCONNECT: Selama stabilization_window aktif, gunakan gap aman
        if self.stabilization_window > 0.0 and gap < self.fsm.EMERGENCY_GAP:
            gap = max(gap, self.fsm.EMERGENCY_GAP * 2.0)  # override gap ke nilai aman
        # DEBUG: trace update_without_data for V2/V3
        if self.id in (2, 3) and self.stabilization_window > 0:
            print(f'[DBG-UWD] V{self.id} t={current_time:.2f} gap={gap:.1f} a_act={self.a_actual:.2f} stab={self.stabilization_window:.2f} comm_ok={self.communication_ok} last_data={"YES" if self.last_leader_data else "NO"}')
        state, _ = self.fsm.update(dt, current_time, gap, self.a_actual,
                                   self.transfer_cooldown, self.stabilization_window)
        self.control_mode = state
        # FIX2+5: EMERGENCY dengan rem proporsional + recovery saat gap besar
        if state == 'EMERGENCY':
            if gap < 5.0:
                self.a_desired = self.max_decel   # bahaya nyata: rem penuh
                self._apply_actuation_lag(dt, bypass=True)
            elif gap < self.fsm.EMERGENCY_GAP:
                urgency = max(0.0, 1.0 - (gap - 5.0) / (self.fsm.EMERGENCY_GAP - 5.0))
                self.a_desired = urgency * self.max_decel
                self._apply_actuation_lag(dt)
            else:
                # Gap sudah aman: gunakan ACC control untuk mengejar, bukan rem
                if self.last_leader_data:
                    self.a_desired = self._acc_control(self.last_leader_data['position'], lvel, intruder)
                else:
                    self.a_desired = max(self.A_CMD_MIN, -1.0)  # gentle brake
                self._apply_actuation_lag(dt)
        elif self.last_leader_data:
            self.a_desired = self._acc_control(self.last_leader_data['position'], lvel, intruder)
            self._apply_actuation_lag(dt)
        else:
            self.a_desired = max(self.A_CMD_MIN, -1.0)  # gentle brake tanpa data
            self._apply_actuation_lag(dt)
        self.velocity  = max(0, min(self.max_velocity, self.velocity + self.a_actual*dt))
        self.position += self.velocity * dt
        if self.transfer_cooldown > 0: self.transfer_cooldown -= dt
        if self.stabilization_window > 0: self.stabilization_window -= dt

    def _acc_control(self, leader_pos, leader_vel=None, intruder=None):
        """
        ACC fallback â€” digunakan saat komunikasi terputus.
        Menggunakan gap + kecepatan relatif agar tidak terjadi tumbukan.
        Ref: Rajamani (2012) Vehicle Dynamics and Control, Ch.8
        """
        gap = leader_pos - self.position - self.length
        if intruder and intruder.get('position') is not None:
            ip = intruder['position']
            if self.position < ip < leader_pos:
                gap = min(gap, ip - self.position - self.length)
        hw  = self.time_headway * self.fsm.get_headway_multiplier()
        d_g = self.desired_gap + hw * self.velocity
        e_gap = gap - d_g
        a_gap = 0.5 * e_gap   # FIX-COL-8: naikkan gain dari 0.4->0.5
        if leader_vel is not None:
            e_vel = leader_vel - self.velocity
            a_gap += 0.4 * e_vel  # FIX-COL-8: naikkan dari 0.3->0.4
        # Safety override: FIX-COL-9: zona rem lebih luas dan lebih keras
        if gap < self.fsm.EMERGENCY_GAP * 1.5:   # ~18m
            urgency = max(0.0, 1.0 - gap / (self.fsm.EMERGENCY_GAP * 1.5))
            brake = -urgency * abs(self.max_decel)
            a_gap = min(a_gap, brake)
        if gap < self.fsm.EMERGENCY_GAP:          # <12m
            a_gap = self.max_decel                # rem keras penuh
        return max(self.A_CMD_MIN, min(0.5, a_gap))

    def check_timeout(self, current_time):
        if self.last_packet_time is None: return False
        return (current_time - self.last_packet_time) > self.timeout_threshold

    def update_in_transit(self, dt, target_tail):
        """
        Fase 3 (Â§4.3.3): Kendaraan berpindah lajur menuju platoon tujuan.
        
        FIX-TRANSIT (BUG-2 ROOT CAUSE):
        Dalam simulasi 1D, perpindahan lajur adalah gerakan LATERAL (divisualisasikan).
        Posisi longitudinal tetap berubah sesuai kecepatan â€” kendaraan TIDAK berhenti.
        
        Root cause bug asli: saat target tail ada di BELAKANG kendaraan (posisi lebih
        rendah), actual_gap negatif â†’ a_cmd = max_decel â†’ kendaraan berhenti menunggu â†’
        platoon tujuan "menembus" kendaraan secara visual â†’ join saat tail lewat.
        
        Perbaikan: kendaraan IN_TRANSIT hanya menyamakan kecepatan dengan target platoon.
        Join terjadi ketika selisih posisi longitudinal masuk dalam window yang wajar.
        Kendaraan tidak pernah berhenti total â€” ini sesuai teori perpindahan lajur (Â§4.3.3).
        """
        if target_tail is None or target_tail.collided:
            self.position += self.velocity * dt
            return False

        actual_gap = target_tail.position - self.position - self.length
        d_ref = self.desired_gap + self.time_headway * target_tail.velocity
        e_gap = actual_gap - d_ref
        e_vel = target_tail.velocity - self.velocity

        # FIX-TRANSIT-1: Kontrol hanya pada kecepatan (bukan posisi longitudinal).
        # Kendaraan menyamakan kecepatan dengan tail platoon tujuan.
        # Ini realistis: saat ganti lajur, driver menyamakan kecepatan dengan barisan baru,
        # tidak berhenti total untuk menunggu barisan yang ada di belakang.
        a_cmd = max(-2.0, min(self.max_accel, 1.2 * e_vel))

        # FIX-TRANSIT-2: Jika target ada di depan (actual_gap > d_ref * 2),
        # boleh sedikit akselerasi untuk mengejar â€” tapi batasi agar tidak terlalu agresif.
        if actual_gap > d_ref * 2.0 and actual_gap > 0:
            a_approach = max(-1.0, min(1.5, 0.2 * e_gap + 0.8 * e_vel))
            a_cmd = a_approach

        # Actuation lag
        self.a_actual += (dt / self.tau_act) * (a_cmd - self.a_actual)
        self.a_actual = max(-2.0, min(self.max_accel, self.a_actual))
        self.acceleration = self.a_actual
        self.velocity = max(1.0, min(self.max_velocity, self.velocity + self.a_actual * dt))
        self.position += self.velocity * dt
        if self.transfer_cooldown > 0:
            self.transfer_cooldown -= dt

        # FIX-TRANSIT-3: Kondisi join yang realistis (dua mode).
        #
        # Mode A: V1 masih di belakang atau dekat P1_tail (normal approach)
        #   â†’ join saat gap dalam window [-10, d_ref+20] DAN kecepatan match
        #
        # Mode B: V1 sudah JAUH DI DEPAN P1_tail (overshoot, actual_gap << -10)
        #   â†’ V1 tidak bisa mundur di 1D. Solusi: join segera saat kecepatan match.
        #   â†’ Backend akan reposition V1 di belakang P1_tail saat join.
        #   â†’ Ini fisik yang valid: perpindahan lajur = mencocokan kecepatan,
        #     bukan posisi longitudinal yang harus tepat.
        join_vel_ok = abs(e_vel) < 2.5
        if actual_gap < -10.0:
            # Mode B: overshoot â€” join berdasarkan kecepatan saja
            return join_vel_ok
        join_gap_ok = actual_gap < (d_ref + 20.0)
        return (join_gap_ok and join_vel_ok)

    def to_dict(self):
        # Laporkan 'TRANSFER' saat cooldown aktif (Ref: Â§3.4 FSM Dasar)
        if self.maneuver_phase in ('DEPARTING', 'IN_TRANSIT'):
            reported_fsm = self.maneuver_phase
        elif self.transfer_cooldown > 0 and not self.is_leader:
            reported_fsm = 'TRANSFER'
        else:
            reported_fsm = self.fsm.state
        return {
            'id': self.id, 'position': round(self.position,2),
            'velocity': round(self.velocity,2), 'velocity_kmh': round(self.velocity*3.6,1),
            'acceleration': round(self.acceleration,2), 'a_desired': round(self.a_desired,3),
            'a_actual': round(self.a_actual,3), 'is_leader': self.is_leader,
            'platoon_id': self.platoon_id, 'control_mode': self.control_mode,
            'spacing_error': round(self.spacing_error,2), 'communication_ok': self.communication_ok,
            'collided': self.collided, 'fsm_state': reported_fsm,
            'aoi_ms': round(self.fsm.aoi*1000,1), 'aoi_peak_ms': round(self.aoi_running_peak,1),
            'emergency': self.fsm.emergency_braking, 'integral_e': round(self.integral_e,4),
            'topology': self.topology,
            'length': round(self.length, 2),
            'transfer_cooldown': round(max(0.0, self.transfer_cooldown), 2),
            'maneuver_phase': self.maneuver_phase,   # v7: expose fase manuver ke frontend
            'transit_src_pid':    self.transit_data.get('src_pid')    if self.maneuver_phase else None,
            'transit_target_pid': self.transit_data.get('target_pid') if self.maneuver_phase else None,
            'stabilization_window': round(max(0.0, self.stabilization_window), 3),  # P-03: debug
            'time_headway': round(self.time_headway, 3),
            'time_headway_base': round(self.time_headway_base, 3),
            'repositioning': (
                getattr(self, '_reposition_target', None) is not None
                or float(getattr(self, '_reposition_delay_remaining', 0.0) or 0.0) > 1e-6
            ),
        }

# ============================================================
# SIMULATION ENGINE
# ============================================================
class SimulationEngine:
    def __init__(self, config, mode='cacc'):
        self.config = config; self.mode = mode
        self.dt = 0.01; self.time_elapsed = 0.0
        self.running = False
        self.vehicles = []; self.network = None
        self.data_log = []; self.collision_log = []; self.mode_switch_log = []
        self.collision_occurred = False
        self.experiment_id = datetime.now().strftime('%Y%m%d_%H%M%S') + ('_acc' if mode=='acc_only' else '')
        self.aoi_all_samples = []
        # Platoon management
        self._next_vid = 0
        self._next_pid = 0
        self._lock = threading.Lock()
        # NEW: ManeuverQueue untuk mencegah deadlock (Ref: Â§7.5.1, Â§4.6 Teorema 4.1)
        self._maneuver_queue = ManeuverQueue()
        # Emergency promotion threshold (Ref: Â§5.5 Algorithm 2)
        self._emergency_promo_threshold = 0.5  # 500ms AoI
        # Heterogeneous traffic: non-platoon vehicle in P0 lane (1D cut-in)
        self.cut_in = None
        # Timed disturbance override for leader acceleration
        self._leader_override_accel = None
        self._leader_override_until = 0.0
        self._apply_rng_seed()
        self._initialize_network()
        self._initialize_vehicles()

    def _apply_rng_seed(self):
        """Reproducible stochastic runs (channel, loss, ECDSA jitter)."""
        if self.config.get('random_seed_used') is not None:
            random.seed(int(self.config['random_seed_used']))
            return
        rs = self.config.get('random_seed')
        if rs is not None and str(rs).strip() != '':
            self.config['random_seed_used'] = int(rs)
        else:
            self.config['random_seed_used'] = random.randrange(0, 2**31)
        random.seed(self.config['random_seed_used'])

    def _cut_in_client_dict(self):
        if not self.cut_in:
            return None
        return {
            'position': round(self.cut_in['position'], 2),
            'velocity': round(self.cut_in['velocity'], 2),
            'length':   self.cut_in['length'],
            'platoon_id': 0,
        }

    def _initialize_network(self):
        self.network = Network5G(self.config)

    def _initialize_vehicles(self):
        n_platoons = self.config.get('num_platoons', 1)
        n_vehicles = self.config.get('num_vehicles', 4)
        spacing    = self.config.get('initial_spacing', 15.0)
        speed      = self.config.get('desired_speed', 20.0)
        h          = self.config.get('time_headway', 0.8)
        # BUG-FIX: mulai dari desired equilibrium gap r + h*v agar kontrol langsung stabil
        # Ref: Ploeg et al. (2011) d_i = r_i + h_i * v_i
        eq_gap     = spacing + h * speed + 5.0   # vehicle length 5m
        for pid in range(n_platoons):
            base = 500.0 + pid * 300.0
            for i in range(n_vehicles):
                cfg = dict(self.config)
                if self.mode == 'acc_only': cfg['tau_act'] = 0.35
                v = Vehicle(self._next_vid, base - i*eq_gap, speed, (i==0), pid, cfg)
                self.vehicles.append(v)
                self._next_vid += 1
            self._next_pid = max(self._next_pid, pid + 1)

    # â”€â”€ HELPER: INJECT PAKET INSTAN KE BUFFER PENERIMA â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    def _inject_immediate_packet(self, follower_vehicle, predecessor_vehicle):
        """
        P-01 FIX (FINAL): Inject paket dari predecessor ke follower secara ATOMIS.

        Menggunakan network.inject_immediate_atomic() yang menggabungkan clear() dan
        add_packet() dalam satu _buffer_lock, sehingga Thread-A (step/read) TIDAK bisa
        membaca buffer di antara dua operasi tersebut.

        Ini menggantikan pendekatan lama yang melakukan clear() di transfer_vehicle()
        dan inject() secara terpisah â€” yang membuka jendela race condition 10ms.
        """
        if follower_vehicle is None or predecessor_vehicle is None:
            return
        fid = follower_vehicle.id
        pkt = {
            'position':     predecessor_vehicle.position,
            'velocity':     predecessor_vehicle.velocity,
            'acceleration': predecessor_vehicle.acceleration,
            'timestamp':    self.time_elapsed,
            'network_delay':  0.0,
            'security_delay': 0.0,
            'total_delay':    0.0,
            'sent_time':      self.time_elapsed,
        }
        # ATOMIS: clear lama + inject baru dalam satu lock â€” tidak ada celah race condition
        self.network.inject_immediate_atomic(fid, pkt, self.time_elapsed)

    # ---- NEW-01: TAMBAH PLATOON DINAMIS ----
    # Ref: Bergenhem et al. (2012) platoon formation protocol
    def add_platoon(self, n_vehicles=None, custom_config=None):
        """
        Tambahkan platoon baru ke simulasi yang sedang berjalan.
        Platoon baru diposisikan 100m di belakang kendaraan paling belakang.
        Kecepatan awal 90% dari desired_speed untuk mengejar.
        """
        with self._lock:
            cfg = dict(self.config)
            if custom_config: cfg.update(custom_config)
            if n_vehicles is None: n_vehicles = max(2, cfg.get('num_vehicles', 4))
            spacing   = cfg.get('initial_spacing', 15.0)
            speed     = cfg.get('desired_speed', 20.0)
            new_pid   = self._next_pid

            # Posisi di belakang semua kendaraan yang ada
            if self.vehicles:
                rearmost = min(v.position for v in self.vehicles)
                leader_pos = rearmost - 100.0
            else:
                leader_pos = 500.0

            # B-02 FIX: Gunakan equilibrium gap (r + h*v + L) bukan spacing mentah.
            # spacing mentah=15m sedangkan eq_gap=15+0.8*20+5=36m â†’ spacing_error=-21m â†’ EMERGENCY instan.
            h = cfg.get('time_headway', 0.8)
            eq_gap = spacing + h * (speed * 0.9) + 5.0   # vehicle length 5m
            new_vehicles = []
            for i in range(n_vehicles):
                if self.mode == 'acc_only': cfg['tau_act'] = 0.35
                v = Vehicle(self._next_vid, leader_pos - i*eq_gap,
                            speed * 0.9, (i==0), new_pid, cfg)
                self.vehicles.append(v)
                new_vehicles.append(v)
                self._next_vid += 1

            self._next_pid += 1
            self.config['num_platoons'] = self._next_pid

            return {
                'platoon_id':  new_pid,
                'n_vehicles':  n_vehicles,
                'vehicle_ids': [v.id for v in new_vehicles],
                'leader_id':   new_vehicles[0].id,
                'leader_pos':  round(leader_pos, 1),
            }

    # â”€â”€ VALIDASI PRASYARAT â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    def validate_transfer(self, vehicle_id, target_platoon_id):
        """
        Cek kondisi prasyarat C1-C6 sebelum transfer.
        Ref: Â§4.2 Kondisi Prasyarat Transfer
        """
        vehicle = next((v for v in self.vehicles if v.id == vehicle_id), None)
        if vehicle is None:     return False, 'C0: Vehicle tidak ditemukan'
        if vehicle.is_leader:   return False, 'C1: Tidak bisa transfer platoon leader'
        if vehicle.platoon_id == target_platoon_id: return False, 'C1: Kendaraan sudah di platoon tujuan'
        if vehicle.collided:    return False, 'C2: Kendaraan dalam kondisi tabrakan'
        if vehicle.transfer_cooldown > 0 and vehicle.maneuver_phase not in ('DEPARTING', 'IN_TRANSIT'):
            return False, f'C3: Cooldown aktif ({vehicle.transfer_cooldown:.1f}s tersisa)'
        if vehicle.maneuver_phase in ('DEPARTING', 'IN_TRANSIT'):
            return False, f'C3: Kendaraan sedang dalam proses transfer ({vehicle.maneuver_phase})'
        target_vehicles = [v for v in self.vehicles if v.platoon_id == target_platoon_id]
        if not target_vehicles: return False, f'C4: Platoon {target_platoon_id} tidak ditemukan'
        # C5: cek busy platoon
        if target_platoon_id in self._maneuver_queue._active_platoons:
            return False, 'C5: Platoon tujuan sedang dalam manuver lain'
        return True, 'OK'

    def validate_swap(self, platoon_id_a, platoon_id_b):
        """
        Cek kondisi prasyarat S1-S6 sebelum swap leader.
        Ref: Â§6.3 Kondisi Prasyarat Swap
        """
        if platoon_id_a == platoon_id_b: return False, 'S1: Platoon harus berbeda'
        pvs_a = sorted([v for v in self.vehicles if v.platoon_id == platoon_id_a], key=lambda v: -v.position)
        pvs_b = sorted([v for v in self.vehicles if v.platoon_id == platoon_id_b], key=lambda v: -v.position)
        if not pvs_a: return False, f'S0: Platoon {platoon_id_a} tidak ditemukan'
        if not pvs_b: return False, f'S0: Platoon {platoon_id_b} tidak ditemukan'
        if len(pvs_a) < 2: return False, f'S2: Platoon {platoon_id_a} perlu â‰¥2 kendaraan'
        if len(pvs_b) < 2: return False, f'S2: Platoon {platoon_id_b} perlu â‰¥2 kendaraan'
        if any(v.collided for v in pvs_a + pvs_b): return False, 'S3: Ada kendaraan tabrakan di salah satu platoon'
        la, lb = pvs_a[0], pvs_b[0]
        if la.transfer_cooldown > 0: return False, f'S4: Leader P{platoon_id_a} masih dalam cooldown ({la.transfer_cooldown:.1f}s)'
        if lb.transfer_cooldown > 0: return False, f'S4: Leader P{platoon_id_b} masih dalam cooldown ({lb.transfer_cooldown:.1f}s)'
        # S5: gap antar-platoon (jarak antara kendaraan terdepan platoon belakang vs terbelakang platoon depan)
        front_a = max(pvs_a, key=lambda v: v.position)
        front_b = max(pvs_b, key=lambda v: v.position)
        rear_a  = min(pvs_a, key=lambda v: v.position)
        rear_b  = min(pvs_b, key=lambda v: v.position)
        # Platoon mana yang lebih depan?
        if front_a.position > front_b.position:
            ginter = rear_a.position - front_b.position - front_b.length
        else:
            ginter = rear_b.position - front_a.position - front_a.length
        if ginter < 0: ginter = abs(ginter)  # parallel lanes, gap dihitung dari posisi longitudinal
        # S6: kecepatan kompatibel
        dv = abs(la.velocity - lb.velocity)
        if dv >= 5.0:
            return False, f'S6: Beda kecepatan terlalu besar ({dv:.1f} m/s â‰¥ 5 m/s). Perlu sinkronisasi dulu.'
        # Cek busy
        if platoon_id_a in self._maneuver_queue._active_platoons:
            return False, f'S5: Platoon {platoon_id_a} sedang dalam manuver lain'
        if platoon_id_b in self._maneuver_queue._active_platoons:
            return False, f'S5: Platoon {platoon_id_b} sedang dalam manuver lain'
        return True, 'OK'

    def validate_promote(self, platoon_id):
        """
        Cek kondisi prasyarat Proposisi 5.1 sebelum promosi leader.
        Ref: Â§5.4 Kondisi Keamanan Promosi Leader
        """
        pvs = sorted([v for v in self.vehicles if v.platoon_id == platoon_id], key=lambda v: -v.position)
        if not pvs: return False, 'P0: Platoon tidak ditemukan'
        if len(pvs) < 2: return False, 'P1: Perlu minimal 2 kendaraan (|P| â‰¥ 2)'
        if pvs[0].transfer_cooldown > 0:
            return False, f'P2: Leader masih dalam cooldown ({pvs[0].transfer_cooldown:.1f}s)'
        if platoon_id in self._maneuver_queue._active_platoons:
            return False, 'P3: Platoon sedang dalam manuver lain'
        return True, 'OK'

    def get_maneuver_queue_status(self):
        """Kembalikan status ManeuverQueue untuk API."""
        return self._maneuver_queue.get_status()

    # â”€â”€ SWAP LEADER â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    def swap_leaders(self, platoon_id_a, platoon_id_b):
        """
        Tukar pemimpin (leader) antara dua platoon.
        Fase 1-5 sesuai Â§6.4. Validasi prasyarat S1-S6 (Â§6.3).
        Ref: Bergenhem et al. (2012), Â§6 Protokol Pertukaran Leader Antar-Platoon.
        """
        with self._lock:
            # Fase 1: Validasi prasyarat S1-S6
            ok, reason = self.validate_swap(platoon_id_a, platoon_id_b)
            if not ok:
                return {'success': False, 'error': reason}

            # Cek ManeuverQueue (Teorema 4.1 â€” Isolasi Transfer)
            if not self._maneuver_queue.try_acquire([platoon_id_a, platoon_id_b]):
                return {'success': False, 'error': 'Platoon sedang dalam manuver lain (queue busy)'}

            try:
                pvs_a = sorted([v for v in self.vehicles if v.platoon_id == platoon_id_a], key=lambda v: -v.position)
                pvs_b = sorted([v for v in self.vehicles if v.platoon_id == platoon_id_b], key=lambda v: -v.position)

                leader_a = pvs_a[0]
                leader_b = pvs_b[0]

                pos_a, vel_a = leader_a.position, leader_a.velocity
                pos_b, vel_b = leader_b.position, leader_b.velocity

                def assign_swapped_leader(vehicle, platoon_id, target_pos, target_vel):
                    """Tukar peran + platoon; posisi mengalir menuju slot lawan (terlihat di visualisasi)."""
                    vehicle.platoon_id = platoon_id
                    vehicle.is_leader = True
                    vehicle._reposition_target = target_pos
                    vehicle._reposition_velocity = target_vel
                    self._apply_reposition_profile(vehicle, 'swap')
                    vehicle.control_mode = 'LEADER'
                    vehicle.last_leader_data = None
                    vehicle.last_packet_time = None
                    vehicle.communication_ok = True
                    vehicle.integral_e = 0.0
                    vehicle.spacing_error = 0.0
                    vehicle.a_desired = 0.0
                    vehicle.a_actual = 0.0
                    vehicle.acceleration = 0.0
                    vehicle.aoi_running_peak = 0.0
                    vehicle.transfer_cooldown = 2.0
                    vehicle.fsm.reset()

                assign_swapped_leader(leader_a, platoon_id_b, pos_b, vel_b)
                assign_swapped_leader(leader_b, platoon_id_a, pos_a, vel_a)

                # Fase 4: Sinkron buffer V2V pasca-swap (sama ide dengan FIX4 transfer)
                # Tanpa ini, follower masih pakai last_leader_data dari predecessor lama
                # dan spacing_error bisa meledak -> EMERGENCY / lonjakan a_desired.
                members_a = sorted([v for v in self.vehicles if v.platoon_id == platoon_id_a], key=_platoon_chain_key)
                members_b = sorted([v for v in self.vehicles if v.platoon_id == platoon_id_b], key=_platoon_chain_key)
                for group in (members_a, members_b):
                    for i in range(1, len(group)):
                        vv = group[i]
                        vv.integral_e = 0.0
                        vv.spacing_error = 0.0
                        vv.last_leader_data = None
                        vv.last_packet_time = None
                        vv._post_transfer_inject_frames = 30
                        self._inject_immediate_packet(vv, group[i - 1])

                self.mode_switch_log.append({
                    'time': round(self.time_elapsed, 2),
                    'event': 'leader_swap',
                    'phase': 'COMPLETE',
                    'platoon_a': platoon_id_a, 'platoon_b': platoon_id_b,
                    'new_leader_a': leader_b.id, 'new_leader_b': leader_a.id,
                    'vel_diff_ms': round(abs(vel_a - vel_b), 2),
                    'reason': 'manual_swap'
                })
                result = {
                    'success': True,
                    'platoon_a': platoon_id_a, 'platoon_b': platoon_id_b,
                    'new_leader_a': leader_b.id, 'new_leader_b': leader_a.id,
                    'vel_diff_ms': round(abs(vel_a - vel_b), 2),
                }
            finally:
                # Release setelah eksekusi (cooldown ditangani per-vehicle)
                self._maneuver_queue.release([platoon_id_a, platoon_id_b])
                self._maneuver_queue.log_maneuver('swap', [platoon_id_a, platoon_id_b], result.get('success', False))

            return result

    def promote_next_leader(self, platoon_id):
        """
        Dalam satu platoon, kendaraan ke-2 dipromosikan menjadi leader baru.
        Leader lama turun pangkat menjadi follower di posisi ekor.
        4 Fase sesuai Â§5.2. Validasi Proposisi 5.1 (Â§5.4).
        Ref: Ploeg et al. (2011) CACC leader election, Â§5 Protokol Promosi Leader.
        """
        with self._lock:
            # Validasi prasyarat Proposisi 5.1
            ok, reason = self.validate_promote(platoon_id)
            if not ok:
                return {'success': False, 'error': reason}

            if not self._maneuver_queue.try_acquire([platoon_id]):
                return {'success': False, 'error': 'Platoon sedang dalam manuver lain'}

            try:
                pvs = sorted([v for v in self.vehicles if v.platoon_id == platoon_id], key=lambda v: -v.position)

                old_leader = pvs[0]   # Kendaraan terdepan (leader saat ini)
                new_leader = pvs[1]   # Kendaraan ke-2 (kandidat leader baru â€” deterministik, Â§5.2.2)
                tail = pvs[-1]        # Kendaraan paling belakang

                # FIX-DEMOTE-1: Posisi baru old_leader menggunakan CACC reference gap
                # d_ref = desired_gap + time_headway * velocity agar spacing_error = 0
                cacc_ref_gap_demote = old_leader.desired_gap + old_leader.time_headway * tail.velocity
                demote_pos = tail.position - tail.length - cacc_ref_gap_demote

                # Fase 3a: Naikkan pangkat vehicle[1] (Â§5.2.3)
                new_leader.is_leader     = True
                new_leader.control_mode  = 'LEADER'
                new_leader.spacing_error = 0.0
                new_leader.integral_e    = 0.0
                new_leader.last_leader_data = None
                new_leader.last_packet_time = None
                new_leader.aoi_running_peak = 0.0
                new_leader.transfer_cooldown = 2.0   # Fase 4: Stabilisasi 2s (Â§5.2.4)
                new_leader.fsm.reset()

                # Fase 3b: Turunkan pangkat old_leader ke ekor (Â§5.2.3)
                old_leader.is_leader     = False
                old_leader.control_mode  = 'CACC'
                old_leader._reposition_target = demote_pos
                old_leader._reposition_velocity = tail.velocity
                self._apply_reposition_profile(old_leader, 'promote')
                old_leader.spacing_error = 0.0
                old_leader.integral_e    = 0.0
                old_leader.last_leader_data = None
                old_leader.last_packet_time = None
                old_leader.aoi_running_peak = 0.0
                old_leader.transfer_cooldown = 2.0
                old_leader.fsm.reset()

                # Reset integral semua member agar tidak ada windup (Algorithm 1)
                for v in pvs[2:]:
                    v.integral_e = 0.0

                self.mode_switch_log.append({
                    'time': round(self.time_elapsed, 2),
                    'event': 'leader_promote',
                    'phase': 'COMPLETE',
                    'platoon_id': platoon_id,
                    'old_leader': old_leader.id,
                    'new_leader': new_leader.id,
                    'reason': 'manual_promote'
                })
                result = {
                    'success': True,
                    'platoon_id': platoon_id,
                    'old_leader_id': old_leader.id,
                    'new_leader_id': new_leader.id,
                }
            finally:
                self._maneuver_queue.release([platoon_id])
                self._maneuver_queue.log_maneuver('promote', [platoon_id], result.get('success', False))

            return result

    def transfer_vehicle(self, vehicle_id, target_platoon_id):
        """
        Transfer vehicle_id ke target_platoon_id.
        4 Fase sesuai Â§4.3. Validasi prasyarat C1-C6 (Â§4.2).
        Ref: Â§4 Protokol Transfer Anggota Antar-Platoon.
        """
        with self._lock:
            # Fase 1: Validasi prasyarat C1-C6 (Â§4.2)
            ok, reason = self.validate_transfer(vehicle_id, target_platoon_id)
            if not ok:
                return {'success': False, 'error': reason}

            vehicle = next((v for v in self.vehicles if v.id == vehicle_id), None)
            src_pid = vehicle.platoon_id

            # ManeuverQueue â€” cek isolasi (Teorema 4.1)
            if not self._maneuver_queue.try_acquire([src_pid, target_platoon_id]):
                return {'success': False, 'error': 'Platoon sedang dalam manuver lain (Teorema 4.1)'}

            try:
                target_vehicles = [v for v in self.vehicles if v.platoon_id == target_platoon_id]

                # Fase 2: Pemisahan dari platoon sumber
                # Cari kendaraan di belakang vehicle dalam platoon sumber untuk reset integralnya
                src_pvs = sorted([v for v in self.vehicles if v.platoon_id == src_pid], key=lambda v: -v.position)
                veh_idx = next((i for i, v in enumerate(src_pvs) if v.id == vehicle_id), None)

                # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
                # ROOT CAUSE FIX (FINAL): Bersihkan packet buffer semua followers
                # di belakang vehicle pada step YANG SAMA saat DEPARTING dimulai.
                #
                # Mengapa: dt=0.01s, network delay=10ms â†’ paket dari V1 ke V2 yang
                # dikirim di step t-1 akan SIAP DIBACA di step t (saat DEPARTING dimulai).
                # DEPART-CORE mengeksklusikan V1 dari loop platoon (tidak kirim paket baru),
                # tapi V2's packet_buffer masih berisi paket LAMA dari V1.
                # V2 lalu memproses paket itu: gap(V1â†’V2)â‰ˆ13m, d_ref=31m â†’ e_gap=-18m
                # PID output = 0.5Ã—(-18) = -9 m/sÂ² â†’ EMERGENCY cascade!
                #
                # FIX: Clear buffer + seed last_leader_data dengan predecessor yang BENAR
                # (yaitu kendaraan yang sama â€” V1 masih di platoon saat DEPARTING, jadi
                # V2 tetap tracking V1. Seed mencegah communication_ok=False sementara).
                # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
                if veh_idx is not None:
                    for j in range(veh_idx + 1, len(src_pvs)):
                        sv = src_pvs[j]
                        sv.integral_e    = 0.0   # Reset integral mencegah windup
                        sv.spacing_error = 0.0
                        sv.a_actual      = 0.0   # tidak ada momentum pengereman
                        sv.a_desired     = 0.0
                        sv.acceleration  = 0.0
                        sv.fsm.reset()           # reset FSM ke CACC (AoI=0)
                        # Seed last_leader_data dengan predecessor yang tepat
                        pred = src_pvs[j - 1]
                        sv.last_leader_data = {
                            'position':     pred.position,
                            'velocity':     pred.velocity,
                            'acceleration': pred.acceleration,
                            'timestamp':    self.time_elapsed
                        }
                        sv.last_packet_time = self.time_elapsed
                        sv.fsm.packet_received(self.time_elapsed)
                        sv.communication_ok = True
                        # FIX1: Grace period dan stabilization window JAUH lebih besar
                        sv._no_data_grace = 2.0          # 0.50 -> 2.0s
                        sv.stabilization_window = 3.0     # 0.80 -> 3.0s
                        # FIX4: Multi-frame re-injection selama 30 frame
                        sv._post_transfer_inject_frames = 30
                        # P-01 FIX: ATOMIS clear + inject dalam satu _buffer_lock
                        # Tidak ada celah race condition antara Thread-A dan Thread-B
                        inj_pkt = {
                            'position':     pred.position,
                            'velocity':     pred.velocity,
                            'acceleration': pred.acceleration,
                            'timestamp':    self.time_elapsed,
                            'network_delay':  0.0,
                            'security_delay': 0.0,
                            'total_delay':    0.0,
                            'sent_time':      self.time_elapsed,
                        }
                        self.network.inject_immediate_atomic(sv.id, inj_pkt, self.time_elapsed)

                # Fase 3: Perpindahan dan penggabungan (Â§4.3.3)
                # Cari kendaraan paling belakang di platoon tujuan
                tail = min(target_vehicles, key=lambda v: v.position)

                # FIX-JOIN-1: Posisi dihitung sesuai CACC reference gap agar spacing_error = 0
                # d_ref = desired_gap (r) + time_headway * tail.velocity
                # sehingga kendaraan langsung berada di posisi formasi yang benar
                cacc_ref_gap = vehicle.desired_gap + vehicle.time_headway * tail.velocity
                new_position = tail.position - tail.length - cacc_ref_gap
                new_velocity = tail.velocity

                # â”€â”€ FASE 2: Mulai DEPARTING (Â§4.3.2) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                # v7: Tidak lagi teleport instan. Kendaraan melewati 3 fase:
                #   DEPARTING (1.5s) â†’ IN_TRANSIT (fisika ACC) â†’ STABILIZING (2s)
                # Selama DEPARTING, kendaraan masih di platoon sumber tapi headway
                # meningkat 1.5x (transfer_cooldown > 0 memicu HEADWAY_MULT['TRANSFER'])
                DEPART_DURATION = 1.5   # Â§4.3.2: waktu pemisahan dari platoon sumber

                vehicle.maneuver_phase = 'DEPARTING'
                vehicle.transit_data   = {
                    'target_pid':       target_platoon_id,
                    'target_tail_id':   tail.id,
                    'depart_elapsed':   0.0,
                    'depart_duration':  DEPART_DURATION,
                    'src_pid':          src_pid,
                }
                # Transfer cooldown diberi nilai besar supaya FSM lapor TRANSFER
                # dan headway 1.5x aktif selama proses berlangsung.
                # FIX-DEPART-1: Reset a_actual=0 agar transisi headway tidak menyebabkan
                # spike deceleration yang cascade ke V2/V3.
                vehicle.transfer_cooldown = 999.0
                vehicle.a_actual  = 0.0
                vehicle.a_desired = 0.0
                vehicle.acceleration = 0.0
                # Reset integral agar tidak ada windup saat headway berubah (Â§8.6 Prop 8.1)
                vehicle.integral_e = 0.0

                # FIX-JOIN-2: Hapus paket lama agar tidak dibaca dengan konteks platoon lama
                # P-01 FIX: gunakan clear_buffer_atomic
                self.network.clear_buffer_atomic(vehicle_id)

                self.mode_switch_log.append({
                    'time': round(self.time_elapsed, 2),
                    'vehicle_id': vehicle_id,
                    'event': 'platoon_transfer',
                    'phase': 'DEPARTING',   # v7: fase awal bukan langsung COMPLETE
                    'from': f'P{src_pid}', 'to': f'P{target_platoon_id}',
                    'tail_vehicle': tail.id,
                    'reason': 'manual_transfer'
                })
                result = {
                    'success':         True,
                    'vehicle_id':      vehicle_id,
                    'from_platoon':    src_pid,
                    'to_platoon':      target_platoon_id,
                    'phase':           'DEPARTING',   # v7: proses dimulai, belum selesai
                    'tail_vehicle_id': tail.id,
                }
                # v7: JANGAN release queue di sini â€” akan di-release saat join selesai
                # di dalam step() â†’ update_in_transit() â†’ joined condition
                self._maneuver_queue.log_maneuver('transfer', [src_pid, target_platoon_id], True)
                return result
            except Exception as exc:
                # Kalau ada error sebelum DEPARTING dimulai, release queue
                self._maneuver_queue.release([src_pid, target_platoon_id])
                return {'success': False, 'error': str(exc)}
            # TIDAK ada finally release di sini (finally dihapus untuk kasus sukses DEPARTING)

    def _apply_reposition_profile(self, vehicle, scenario):
        """
        Penundaan awal + batas kecepatan reposisi per skenario (config JSON).
        Kunci opsional:
          reposition_swap_delay_s, reposition_swap_speed_ms
          reposition_promote_delay_s, reposition_promote_speed_ms
          reposition_emergency_delay_s, reposition_emergency_speed_ms
        Fallback kecepatan: maneuver_reposition_speed_ms (global).
        """
        c = self.config
        default_cap = float(c.get('maneuver_reposition_speed_ms', 24.0))
        if scenario == 'swap':
            vehicle._reposition_delay_remaining = float(c.get('reposition_swap_delay_s', 0.2))
            vehicle._reposition_speed_cap = float(c.get('reposition_swap_speed_ms', 20.0))
        elif scenario == 'promote':
            vehicle._reposition_delay_remaining = float(c.get('reposition_promote_delay_s', 0.25))
            vehicle._reposition_speed_cap = float(c.get('reposition_promote_speed_ms', 16.0))
        elif scenario == 'emergency_promote':
            vehicle._reposition_delay_remaining = float(c.get('reposition_emergency_delay_s', 0.0))
            vehicle._reposition_speed_cap = float(c.get('reposition_emergency_speed_ms', 28.0))
        else:
            vehicle._reposition_delay_remaining = 0.0
            vehicle._reposition_speed_cap = default_cap
        if vehicle._reposition_speed_cap <= 0:
            vehicle._reposition_speed_cap = default_cap

    def _step_position_glides(self):
        """Interpolasi posisi menuju slot forma baru (swap/promosi); hormati delay + cap per skenario."""
        default_cap = float(self.config.get('maneuver_reposition_speed_ms', 24.0))
        for v in self.vehicles:
            tgt = getattr(v, '_reposition_target', None)
            if tgt is None:
                continue
            delay = float(getattr(v, '_reposition_delay_remaining', 0.0) or 0.0)
            if delay > 0.0:
                v._reposition_delay_remaining = max(0.0, delay - self.dt)
                v.a_desired = 0.0
                v.a_actual = 0.0
                v.acceleration = 0.0
                continue
            tv = getattr(v, '_reposition_velocity', None)
            if tv is None:
                tv = v.velocity
            cap = getattr(v, '_reposition_speed_cap', None)
            rep_speed = float(cap) if cap is not None and cap > 0 else default_cap
            max_step = rep_speed * self.dt
            d = tgt - v.position
            if abs(d) <= max(0.35, max_step * 1.08):
                v.position = tgt
                v.velocity = tv
                v._reposition_target = None
                v._reposition_velocity = None
                v._reposition_delay_remaining = 0.0
                v._reposition_speed_cap = None
                v.a_desired = 0.0
                v.a_actual = 0.0
                v.acceleration = 0.0
            else:
                v.position += max(-max_step, min(max_step, d))
                v.velocity += (tv - v.velocity) * min(1.0, 6.0 * self.dt)
                v.a_desired = 0.0
                v.a_actual = 0.0
                v.acceleration = 0.0

    def step(self):
        if not self.running: return None
        self.time_elapsed += self.dt
        self.network.update(self.dt)
        self._step_position_glides()
        leader_override = None
        if self._leader_override_accel is not None:
            if self.time_elapsed <= self._leader_override_until:
                leader_override = self._leader_override_accel
            else:
                self._leader_override_accel = None
        if self.cut_in:
            self.cut_in['position'] += self.cut_in['velocity'] * self.dt
            self.cut_in['ttl'] -= self.dt
            if self.cut_in['ttl'] <= 0:
                self.cut_in = None
        platoons = {}
        for v in self.vehicles: platoons.setdefault(v.platoon_id,[]).append(v)

        profile = self.config.get('leader_profile', 'normal')

        # â”€â”€ v7: MULTI-PHASE TRANSFER MANAGEMENT â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # Kelola transisi DEPARTING â†’ IN_TRANSIT â†’ join platoon tujuan (Â§4.3)
        # Dilakukan SEBELUM platoon loop agar kendaraan IN_TRANSIT tidak di-update
        # sebagai leader/follower pada platoon manapun.
        for v in list(self.vehicles):
            if v.maneuver_phase == 'DEPARTING':
                # Fase 2 (Â§4.3.2): Kendaraan masih di platoon sumber.
                # transfer_cooldown=999 â†’ headway 1.5x aktif â†’ gap meningkat alami.
                v.transit_data['depart_elapsed'] = v.transit_data.get('depart_elapsed', 0.0) + self.dt
                if v.transit_data['depart_elapsed'] >= v.transit_data['depart_duration']:
                    # Transisi ke Fase 3: IN_TRANSIT
                    # Hapus dari platoon sumber (update predecessor kendaraan di belakang)
                    src_pid = v.transit_data['src_pid']
                    src_pvs = sorted([x for x in self.vehicles
                                      if x.platoon_id == src_pid and x.id != v.id],
                                     key=lambda x: -x.position)
                    # FIX-PRED-1: Reset state semua kendaraan di belakang v.
                    # ROOT CAUSE yang sebenarnya:
                    #   1. DEPARTING: headway V1 naik 1.5x -> spacing_error spike
                    #   2. V1 brakes hard (-4 m/sÂ²) via ACC control
                    #   3. V2/V3 ikut brake (-4 m/sÂ²) untuk maintain gap ke V1
                    #   4. V1 -> IN_TRANSIT, FIX lama beri transfer_cooldown=0.5 pada V2/V3
                    #   5. FSM TRANSFER branch cek: a_actual < -3.5 -> EMERGENCY!
                    # FIX-PRED-1a: Reset a_actual=0 agar FSM tidak trigger EMERGENCY.
                    # FIX-PRED-1b: Reset FSM ke CACC, JANGAN beri transfer_cooldown
                    #   (cooldown memicu TRANSFER branch yang lebih ketat).
                    # BUG-FIX (DISCONNECT): Seeding last_leader_data dengan predecessor baru
                    # agar communication_ok tidak jatuh ke False saat v meninggalkan barisan.
                    # Tanpa ini, followers memanggil update_without_data & communication_ok=False
                    # selama ~10-20ms (network delay) sebelum paket dari predecessor baru tiba.
                    for j, sv in enumerate(src_pvs):
                        if sv.position < v.position:   # kendaraan di belakang v
                            sv.integral_e    = 0.0     # reset integral (Algorithm 1, Â§4.5)
                            sv.spacing_error = 0.0
                            sv.a_actual  = 0.0
                            sv.a_desired = 0.0
                            sv.acceleration = 0.0
                            sv.fsm.reset()
                            sv.transfer_cooldown = 0.0
                            sv_idx = next((k for k, x in enumerate(src_pvs) if x.id == sv.id), None)
                            if sv_idx is not None and sv_idx > 0:
                                new_pred = src_pvs[sv_idx - 1]
                                sv.last_leader_data = {
                                    'position':     new_pred.position,
                                    'velocity':     new_pred.velocity,
                                    'acceleration': new_pred.acceleration,
                                    'timestamp':    self.time_elapsed
                                }
                                sv.last_packet_time = self.time_elapsed
                                sv.fsm.packet_received(self.time_elapsed)
                                sv.communication_ok = True
                                # FIX1: Grace period dan stabilization window lebih besar
                                sv._no_data_grace = 2.0          # 0.50 -> 2.0s
                                sv.stabilization_window = 3.0     # 0.80 -> 3.0s
                                # FIX4: Multi-frame re-injection selama 30 frame
                                sv._post_transfer_inject_frames = 30
                                # P-01 FIX: ATOMIS clear + inject dari predecessor baru
                                inj_pkt = {
                                    'position':     new_pred.position,
                                    'velocity':     new_pred.velocity,
                                    'acceleration': new_pred.acceleration,
                                    'timestamp':    self.time_elapsed,
                                    'network_delay':  0.0, 'security_delay': 0.0,
                                    'total_delay':    0.0, 'sent_time': self.time_elapsed,
                                }
                                self.network.inject_immediate_atomic(sv.id, inj_pkt, self.time_elapsed)
                            else:
                                sv.last_packet_time  = None
                                sv.last_leader_data  = None
                                # kendaraan jadi leader baru â€” bersihkan buffer saja
                                self.network.clear_buffer_atomic(sv.id)

                    # Pindahkan ke platoon sementara -1 (limbo) supaya tidak
                    # diproses dalam loop platoon normal
                    v.platoon_id    = -1
                    v.maneuver_phase = 'IN_TRANSIT'
                    v.control_mode   = 'ACC'
                    v.integral_e     = 0.0
                    v.spacing_error  = 0.0
                    v.last_leader_data = None
                    v.last_packet_time = None
                    # P-01 FIX: gunakan clear_buffer_atomic
                    self.network.clear_buffer_atomic(v.id)

                    self.mode_switch_log.append({
                        'time': round(self.time_elapsed, 2),
                        'vehicle_id': v.id,
                        'event': 'platoon_transfer',
                        'phase': 'IN_TRANSIT',
                        'from': f'P{src_pid}',
                        'to':   f'P{v.transit_data["target_pid"]}',
                        'reason': 'manual_transfer'
                    })
                    socketio.emit('transfer_phase_update', {
                        'vehicle_id': v.id, 'phase': 'IN_TRANSIT',
                        'from_platoon': src_pid,
                        'to_platoon': v.transit_data['target_pid']
                    })

            elif v.maneuver_phase == 'IN_TRANSIT':
                # Fase 3 (Â§4.3.3): Kendaraan bergerak mandiri menuju ekor platoon tujuan.
                target_pid    = v.transit_data['target_pid']
                target_vehicles = [x for x in self.vehicles
                                   if x.platoon_id == target_pid and not x.collided]

                if not target_vehicles:
                    # Platoon tujuan menghilang â€” abort, kembali ke platoon sumber
                    # BUG-FIX: release ManeuverQueue agar transfer berikutnya tidak terblokir
                    self._maneuver_queue.release([v.transit_data['src_pid'], target_pid])
                    v.platoon_id     = v.transit_data['src_pid']
                    v.maneuver_phase = None
                    v.transfer_cooldown = 1.0
                    v.transit_data   = {}
                    continue

                tail = min(target_vehicles, key=lambda x: x.position)
                joined = v.update_in_transit(self.dt, tail)

                if joined:
                    # â”€â”€ Fase 4: Bergabung ke platoon tujuan (Â§4.3.3 â€“ Â§4.3.4) â”€â”€
                    cacc_ref_gap = v.desired_gap + v.time_headway * tail.velocity
                    # Snap posisi agar spacing_error = 0 tepat saat join
                    v.position       = tail.position - tail.length - cacc_ref_gap
                    v.velocity       = tail.velocity
                    v.platoon_id     = target_pid
                    v.maneuver_phase = None
                    v.is_leader      = False
                    v.control_mode   = 'CACC'
                    v.fsm.reset()
                    v.communication_ok = True
                    v.integral_e       = 0.0
                    v.spacing_error    = 0.0
                    v.a_desired        = 0.0
                    v.aoi_running_peak = 0.0
                    # FIX-JOIN-REPOSITION: Jika V1 ada DI DEPAN tail platoon tujuan
                    # (overshoot case), reposisikan V1 tepat di belakang tail.
                    # Ini diperlukan agar formasi platoon tujuan tetap terurut.
                    actual_gap_join = tail.position - v.position - v.length
                    if actual_gap_join < 0:
                        # V1 melampaui tail: taruh V1 persis di belakang tail
                        desired_pos = tail.position - tail.length - (v.desired_gap + v.time_headway * tail.velocity)
                        v.position = desired_pos
                        v.velocity = tail.velocity   # sinkronkan kecepatan
                    v.transfer_cooldown = 2.0   # Fase 4: Stabilisasi 2s (Â§4.3.4)
                    # FIX1: grace period dan stabilization window lebih besar saat join
                    v._no_data_grace       = 2.0    # 0.50 -> 2.0s
                    v.stabilization_window = 3.0     # 0.80 -> 3.0s
                    # FIX-JOIN-3: seed last_leader_data agar tidak menunggu delay jaringan
                    v.last_leader_data = {
                        'position': tail.position, 'velocity': tail.velocity,
                        'acceleration': tail.acceleration, 'timestamp': self.time_elapsed
                    }
                    v.last_packet_time = self.time_elapsed
                    v.fsm.packet_received(self.time_elapsed)
                    # P-01 FIX: inject paket instan dari tail secara atomis
                    inj_join = {
                        'position': tail.position, 'velocity': tail.velocity,
                        'acceleration': tail.acceleration, 'timestamp': self.time_elapsed,
                        'network_delay': 0.0, 'security_delay': 0.0,
                        'total_delay': 0.0, 'sent_time': self.time_elapsed,
                    }
                    self.network.inject_immediate_atomic(v.id, inj_join, self.time_elapsed)
                    # Release maneuver queue
                    self._maneuver_queue.release([v.transit_data['src_pid'], target_pid])

                    self.mode_switch_log.append({
                        'time': round(self.time_elapsed, 2),
                        'vehicle_id': v.id,
                        'event': 'platoon_transfer',
                        'phase': 'COMPLETE',
                        'from': f'P{v.transit_data["src_pid"]}',
                        'to':   f'P{target_pid}',
                        'tail_vehicle': tail.id,
                        'new_position': round(v.position, 1),
                        'reason': 'manual_transfer'
                    })
                    socketio.emit('vehicle_transferred', {
                        'vehicle_id': v.id,
                        'from_platoon': v.transit_data['src_pid'],
                        'to_platoon': target_pid,
                        'phase': 'COMPLETE'
                    })
                    v.transit_data = {}
        # â”€â”€ akhir multi-phase transfer management â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

        # Rebuild platoons dict SETELAH multi-phase handling (platoon_id bisa berubah)
        # Skip IN_TRANSIT (diupdate mandiri) DAN DEPARTING (diupdate di bawah sendiri).
        # FIX-DEPART-CORE: Kendaraan DEPARTING tidak masuk loop platoon normal.
        # Alasan: saat DEPARTING, transfer_cooldown=999 â†’ headway 1.5x â†’
        # PID menghasilkan a_desired sangat negatif â†’ V1 braking keras â†’
        # V2/V3 di belakang menutup gap â†’ EMERGENCY cascade (root cause sesungguhnya).
        # Solusi: kendaraan DEPARTING cukup mempertahankan kecepatan saat ini (cruise).
        # Gap dengan V0 terbuka alami karena V0 terus maju.
        # FIX-DEPART-FINAL: Kendaraan DEPARTING TETAP di platoon loop sebagai predecessor V2.
        # ROOT CAUSE sebelumnya SALAH: mengekslusikan V1 membuat V2 lompat ke V0 sebagai
        # predecessor (gap GANDA), V2 akselerasi keras, gap ke V0 < 12m â†’ EMERGENCY!
        # SOLUSI BENAR: V1 tetap di loop agar V2 tetap melihat V1 sebagai predecessor
        # (gap normal d_ref). Tapi V1 di-HANDLE KHUSUS dalam loop: a_desired=0 (cruise).
        # Packet dari V1 ke V2 berisi a=0 â†’ tidak ada cascade feedforward.
        platoons = {}
        for v in self.vehicles:
            if v.maneuver_phase == 'IN_TRANSIT':
                continue  # hanya skip IN_TRANSIT, bukan DEPARTING!
            platoons.setdefault(v.platoon_id, []).append(v)

        # FIX4: Multi-frame packet re-injection pasca-transfer
        # Kendaraan dengan _post_transfer_inject_frames > 0 mendapat paket instan
        # dari predecessor baru setiap frame, menjembatani delay jaringan.
        for pid_inj, pvs_inj in platoons.items():
            pvs_inj_sorted = sorted(pvs_inj, key=_platoon_chain_key)
            for ii, vv in enumerate(pvs_inj_sorted):
                if ii > 0 and getattr(vv, '_post_transfer_inject_frames', 0) > 0:
                    pred_inj = pvs_inj_sorted[ii - 1]
                    # Update last_leader_data dengan posisi predecessor TERKINI
                    vv.last_leader_data = {
                        'position':     pred_inj.position,
                        'velocity':     pred_inj.velocity,
                        'acceleration': pred_inj.acceleration,
                        'timestamp':    self.time_elapsed
                    }
                    vv.last_packet_time = self.time_elapsed
                    vv.fsm.packet_received(self.time_elapsed)
                    vv.communication_ok = True
                    # Inject paket instan dari predecessor
                    inj_pkt = {
                        'position':     pred_inj.position,
                        'velocity':     pred_inj.velocity,
                        'acceleration': pred_inj.acceleration,
                        'timestamp':    self.time_elapsed,
                        'network_delay': 0.0, 'security_delay': 0.0,
                        'total_delay':   0.0, 'sent_time': self.time_elapsed,
                    }
                    self.network.inject_immediate_atomic(vv.id, inj_pkt, self.time_elapsed)
                    vv._post_transfer_inject_frames -= 1

        for pid, pvs in platoons.items():
            pvs.sort(key=_platoon_chain_key)
            leader = pvs[0]
            for i, vehicle in enumerate(pvs):
                if vehicle.collided:
                    vehicle.velocity = 0; vehicle.acceleration = 0; continue
                # FIX-DEPART-INLOOP: kendaraan DEPARTING cruise in-place.
                # Tidak dipanggil update_follower/leader â†’ tidak ada PID spike.
                # Ia tetap di loop sebagai predecessor V2, paket dikirim dari posisinya.
                # a=0 berarti packet yang dikirim ke V2 berisi a=0 â†’ tidak ada cascade FF.
                if vehicle.maneuver_phase == 'DEPARTING':
                    vehicle.a_desired    = 0.0
                    vehicle.a_actual     = 0.0
                    vehicle.acceleration = 0.0
                    vehicle.velocity     = max(0.0, min(vehicle.max_velocity, vehicle.velocity))
                    vehicle.position    += vehicle.velocity * self.dt
                    if vehicle.transfer_cooldown > 0:
                        vehicle.transfer_cooldown -= self.dt
                    continue  # skip normal update, TAPI tetap di loop sebagai predecessor
                ci_lane = self.cut_in if pid == 0 else None
                if i == 0:
                    if getattr(vehicle, '_reposition_target', None) is not None:
                        if vehicle.transfer_cooldown > 0:
                            vehicle.transfer_cooldown -= self.dt
                        continue
                    vehicle.update_leader(
                        self.config.get('desired_speed',20.0),
                        self.dt,
                        profile,
                        cut_in=ci_lane,
                        accel_override=leader_override
                    )
                elif i > 0:
                    preceding = pvs[i-1]
                    num_blockers = i - 1
                    # P-04 FIX: Batasi propagasi feedforward saat predecessor dalam EMERGENCY.
                    # Tanpa ini, a=-6.0 m/sÂ² dari predecessor langsung di-propagasi via FF
                    # ke seluruh kendaraan di belakang, memicu cascade EMERGENCY.
                    # Solusi: saat predecessor EMERGENCY, kirim a=0 agar follower tidak ikut rem darurat
                    # kecuali gap fisiknya memang berbahaya (FSM follower akan mendeteksi sendiri).
                    pred_accel_for_packet = preceding.acceleration
                    if preceding.control_mode == 'EMERGENCY' and vehicle.stabilization_window <= 0:
                        pred_accel_for_packet = max(preceding.acceleration, -1.5)
                    packet = {'position': preceding.position, 'velocity': preceding.velocity,
                              'acceleration': pred_accel_for_packet, 'timestamp': self.time_elapsed}
                    leader_packet = {'position': leader.position, 'velocity': leader.velocity,
                                     'acceleration': leader.acceleration}
                    if self.mode == 'acc_only':
                        vehicle.update_without_data(self.dt, self.time_elapsed, intruder=ci_lane)
                    else:
                        self.network.transmit(packet, self.time_elapsed, vehicle.id,
                                              sender_pos=preceding.position,
                                              receiver_pos=vehicle.position,
                                              num_blockers=num_blockers)
                        prev_mode = vehicle.control_mode
                        if vehicle.check_timeout(self.time_elapsed) and prev_mode == 'CACC':
                            self.mode_switch_log.append({'time':self.time_elapsed,'vehicle_id':vehicle.id,'from':'CACC','to':'ACC','reason':'timeout'})
                        ready = self.network.get_packets_for_follower(vehicle.id, self.time_elapsed)
                        lp_direct = leader_packet if vehicle.topology == 'hybrid' else None
                        if ready:
                            pkt = ready[-1]
                            net_delay_s = pkt.get('total_delay', self.network.current_delay) / 1000.0
                            vehicle.update_follower(pkt, self.time_elapsed, self.dt, lp_direct, net_delay_s, intruder=ci_lane)
                        else:
                            vehicle.update_without_data(self.dt, self.time_elapsed, intruder=ci_lane)
                        if vehicle.control_mode != prev_mode:
                            self.mode_switch_log.append({'time':self.time_elapsed,'vehicle_id':vehicle.id,'from':prev_mode,'to':vehicle.control_mode,'reason':'FSM/AoI'})
                    if not vehicle.is_leader:
                        self.aoi_all_samples.append(vehicle.fsm.aoi * 1000)

        self._apply_adaptive_comm_control()

        # Emergency leader promotion check (Algorithm 2, Â§5.5)
        self._check_emergency_promotion()

        self._check_collisions()
        si, ss, si_by_plat = self._calculate_stability()
        ar     = self._amplitude_ratios()
        spacing_errors_raw = {v.id: round(v.spacing_error,3) for v in self.vehicles if not v.is_leader}
        entry = {
            'time': self.time_elapsed,
            'vehicles': [v.to_dict() for v in self.vehicles],
            'network':  self.network.get_metrics(),
            'stability_index': si, 'stability_status': ss,
            'stability_by_platoon': si_by_plat,
            'amplitude_ratios': ar,
            'collision_occurred': self.collision_occurred,
            'max_aoi_ms': self._max_aoi_ms(),
            'spacing_errors': spacing_errors_raw,
            'mode': self.mode,
            'num_platoons': self._next_pid,
            'maneuver_queue': self._maneuver_queue.get_status(),
            'cut_in': self._cut_in_client_dict(),
        }
        self.data_log.append(entry)
        return entry

    def _apply_adaptive_comm_control(self):
        """
        Joint communication–control (Bab 1): perlebar time headway saat AoI tinggi / PDR rendah.
        Menstabilkan barisan tanpa mengganti FSM; melengkapi RSU + zona lingkungan.
        """
        if not self.config.get('adaptive_comm_control', True):
            return
        if self.mode == 'acc_only':
            return
        m = self.network.get_metrics()
        pdr = float(m.get('pdr', 100.0))
        cap = float(self.config.get('adaptive_headway_max_mult', 1.42))
        smooth = float(self.config.get('adaptive_smooth', 0.035))
        for v in self.vehicles:
            if v.is_leader or v.collided:
                continue
            aoi_ms = float(v.fsm.aoi) * 1000.0
            stress = 0.0
            if aoi_ms > 40.0:
                stress += min(1.0, (aoi_ms - 40.0) / 200.0)
            if pdr < 97.0:
                stress += min(1.0, (97.0 - pdr) / 40.0)
            target_hw = v.time_headway_base * (1.0 + (cap - 1.0) * min(1.0, stress))
            v.time_headway += smooth * (target_hw - v.time_headway)
            v.time_headway = max(v.time_headway_base, min(v.time_headway, v.time_headway_base * cap))

    def _check_emergency_promotion(self):
        """
        Algorithm 2 (Â§5.5): Promosi darurat jika V1 tidak terima CAM leader â‰¥ 500ms.
        Hanya aktif saat tidak ada manuver berjalan dan platoon â‰¥ 2 kendaraan.
        """
        platoons = {}
        for v in self.vehicles: platoons.setdefault(v.platoon_id, []).append(v)
        for pid, pvs in platoons.items():
            pvs.sort(key=_platoon_chain_key)
            if len(pvs) < 2: continue
            leader = pvs[0]
            second = pvs[1]
            if leader.collided: continue
            if pid in self._maneuver_queue._active_platoons: continue
            # Cek AoI V1 (kendaraan kedua) terhadap CAM leader
            if second.fsm.aoi >= self._emergency_promo_threshold:
                # Emergency promotion: V1 ambil alih (Â§5.5 Algorithm 2)
                with self._lock:
                    # Re-check setelah lock
                    if pid in self._maneuver_queue._active_platoons: continue
                    if not self._maneuver_queue.try_acquire([pid]): continue
                    try:
                        tail = pvs[-1]
                        # FIX-DEMOTE-2: Gunakan CACC reference gap (sama seperti FIX-DEMOTE-1)
                        cacc_ref_gap_emrg = leader.desired_gap + leader.time_headway * tail.velocity
                        demote_pos = tail.position - tail.length - cacc_ref_gap_emrg
                        # V1 jadi leader darurat
                        second.is_leader = True
                        second.control_mode = 'LEADER'
                        second.spacing_error = 0.0
                        second.integral_e = 0.0
                        second.last_leader_data = None
                        second.last_packet_time = None
                        second.aoi_running_peak = 0.0
                        second.transfer_cooldown = 3.0  # 3s cooldown darurat
                        second.fsm.reset()
                        # Old leader turun ke ekor
                        leader.is_leader = False
                        leader.control_mode = 'CACC'
                        leader._reposition_target = demote_pos
                        leader._reposition_velocity = tail.velocity
                        self._apply_reposition_profile(leader, 'emergency_promote')
                        leader.integral_e = 0.0
                        leader.spacing_error = 0.0
                        leader.last_leader_data = None
                        leader.last_packet_time = None
                        leader.aoi_running_peak = 0.0
                        leader.transfer_cooldown = 3.0
                        leader.fsm.reset()
                        # Reset semua member
                        for v in pvs[2:]:
                            v.integral_e = 0.0
                            v.transfer_cooldown = 3.0
                        self.mode_switch_log.append({
                            'time': round(self.time_elapsed, 2),
                            'event': 'emergency_promote',
                            'phase': 'EMERGENCY',
                            'platoon_id': pid,
                            'old_leader': leader.id,
                            'new_leader': second.id,
                            'trigger_aoi_ms': round(second.fsm.aoi * 1000, 1),
                            'reason': 'emergency_auto'
                        })
                        socketio.emit('emergency_promotion', {
                            'platoon_id': pid, 'new_leader': second.id,
                            'old_leader': leader.id,
                            'trigger_aoi_ms': round(second.fsm.aoi * 1000, 1)
                        })
                    finally:
                        self._maneuver_queue.release([pid])

    def _max_aoi_ms(self):
        vals = [v.fsm.aoi*1000 for v in self.vehicles if not v.is_leader]
        return round(max(vals),1) if vals else 0.0

    def _amplitude_ratios(self):
        """String stability: AR_i = |e_i|/|e_{i-1}| < 1 along each platoon chain (front to rear).
        Ref: Naus et al. (2010). Urutan self.vehicles tidak dijamin longitudinal; gunakan sort per platoon."""
        ratios = {}
        platoons = {}
        for v in self.vehicles:
            if v.platoon_id < 0 or v.collided:
                continue
            platoons.setdefault(v.platoon_id, []).append(v)
        for _pid, pvs in platoons.items():
            pvs.sort(key=lambda x: -x.position)
            if len(pvs) < 2:
                continue
            for i in range(1, len(pvs)):
                v = pvs[i]
                if v.is_leader:
                    continue
                pred = pvs[i - 1]
                e_i = abs(v.spacing_error)
                if i == 1:
                    ratios[v.id] = None
                else:
                    e_im1 = abs(pred.spacing_error)
                    ratios[v.id] = round(e_i / e_im1, 3) if e_im1 > 1e-4 else None
        return ratios

    def _si_from_spacing_errors(self, errors):
        """Satu nilai SI [0–1] + label dari daftar |spacing_error| pengikut dalam satu platoon."""
        if len(errors) < 2:
            return 1.0, 'STABLE'
        if max(errors) == 0:
            return 1.0, 'STABLE'
        amp = max(errors) / max(min(errors), 0.1)
        idx = max(0.0, min(1.0, 1.0 - (amp - 1.0) / 5.0))
        si = round(idx, 3)
        return si, ('STABLE' if idx > 0.8 else 'MARGINAL' if idx > 0.5 else 'UNSTABLE')

    def _calculate_stability(self):
        """
        SI per platoon dari rasio max/min error pengikut; indeks global = min(SI_p) (platoon terburuk).
        Tanpa ini, multi-platoon mencampur error antar rantai dan bisa terlalu optimis.
        """
        platoons = {}
        for v in self.vehicles:
            if v.platoon_id < 0:
                continue
            platoons.setdefault(v.platoon_id, []).append(v)
        by_platoon = {}
        sis = []
        for pid, pvs in platoons.items():
            followers = [v for v in pvs if not v.is_leader and not v.collided]
            errors = [abs(v.spacing_error) for v in followers]
            si_p, ss_p = self._si_from_spacing_errors(errors)
            by_platoon[str(pid)] = {'si': si_p, 'status': ss_p}
            sis.append(si_p)
        if not sis:
            return 1.0, 'STABLE', {}
        worst_si = min(sis)
        worst_ss = 'STABLE' if worst_si > 0.8 else 'MARGINAL' if worst_si > 0.5 else 'UNSTABLE'
        return worst_si, worst_ss, by_platoon

    def _check_collisions(self):
        """
        Deteksi tabrakan fisik saja.
        Kondisi 'comm failure + approach' dihapus â€” itu bukan tabrakan nyata,
        hanya situasi berisiko yang diselesaikan oleh ACC/EMERGENCY mode.
        Ref: GCDC (2016) Collision definition: physical gap <= 0 atau gap < 1m dengan kecepatan relatif tinggi.

        FIX-SWAP-COL: Pasangan harus diurutkan berdasarkan posisi longitudinal per platoon,
        bukan urutan di self.vehicles. Setelah swap leader, urutan array tidak lagi frontâ†’rear,
        sehingga gap = l.pos - f.pos bisa negatif dan memicu tabrakan palsu.
        """
        by_pid = {}
        for v in self.vehicles:
            if v.collided or v.platoon_id < 0:
                continue
            by_pid.setdefault(v.platoon_id, []).append(v)
        for _pid, pvs in by_pid.items():
            pvs.sort(key=lambda x: -x.position)
            for i in range(len(pvs) - 1):
                l, f = pvs[i], pvs[i + 1]
                gap = l.position - f.position - l.length
                rv = f.velocity - l.velocity
                cause = None
                if gap <= 0:
                    cause = f"Physical overlap (gap={gap:.2f}m)"
                elif gap < self.config.get('min_safe_spacing', 1.0) and rv > 1.0:
                    cause = f"Imminent collision (gap={gap:.2f}m, rv={rv:.2f}m/s)"
                if cause:
                    l.collided = f.collided = True
                    self.collision_occurred = True
                    self.collision_log.append({'time': self.time_elapsed, 'vehicles': [l.id, f.id],
                        'position': (l.position + f.position) / 2, 'gap': gap,
                        'relative_velocity': f.velocity - l.velocity, 'cause': cause,
                        'network_delay': self.network.get_current_delay(),
                        'packet_loss': self.network.packet_loss * 100})

        if self.cut_in:
            ci = self.cut_in
            for v in self.vehicles:
                if v.platoon_id != 0 or v.collided:
                    continue
                if ci['position'] <= v.position:
                    continue
                gap = ci['position'] - v.position - v.length
                rv = v.velocity - ci['velocity']
                cause = None
                if gap <= 0:
                    cause = f"Cut-in overlap (gap={gap:.2f}m)"
                elif gap < self.config.get('min_safe_spacing', 1.0) and rv > 1.0:
                    cause = f"Cut-in imminent (gap={gap:.2f}m, rv={rv:.2f}m/s)"
                if cause:
                    v.collided = True
                    self.collision_occurred = True
                    self.collision_log.append({
                        'time': self.time_elapsed, 'vehicles': [v.id, 'CUT_IN'],
                        'position': (v.position + ci['position']) / 2, 'gap': gap,
                        'relative_velocity': rv, 'cause': cause,
                        'network_delay': self.network.get_current_delay(),
                        'packet_loss': self.network.packet_loss * 100,
                    })

    def inject_disturbance(self, t):
        if t == 'leader_brake':
            self._leader_override_accel = -6.0
            self._leader_override_until = self.time_elapsed + 3.0
        elif t == 'acceleration_spike':
            self._leader_override_accel = 3.0
            self._leader_override_until = self.time_elapsed + 3.0
        elif t == 'network_degradation': self.network.inject_degradation(5.0)
        elif t == 'rsu_offline':
            self.network.rsu_enabled = False
            self.network.queuing_delay = 2.2
        elif t == 'cut_in_vehicle':
            pvs0 = sorted([v for v in self.vehicles if v.platoon_id == 0 and not v.collided], key=lambda v: -v.position)
            if len(pvs0) >= 2:
                L, F = pvs0[0], pvs0[1]
                mid = (L.position + F.position) / 2.0
                v_ci = max(1.0, min(L.velocity, F.velocity) * 0.88)
                self.cut_in = {'position': mid, 'velocity': v_ci, 'length': 5.0, 'ttl': 25.0}

    def get_state(self):
        si, ss, si_by_plat = self._calculate_stability()
        return {'time': round(self.time_elapsed,2),
                'vehicles': [v.to_dict() for v in self.vehicles],
                'network':  self.network.get_metrics(),
                'stability_index': round(si,3), 'stability_status': ss,
                'stability_by_platoon': si_by_plat,
                'collision_occurred': self.collision_occurred,
                'amplitude_ratios': self._amplitude_ratios(),
                'max_aoi_ms': self._max_aoi_ms(),
                'mode': self.mode,
                'num_platoons': self._next_pid,
                'cut_in': self._cut_in_client_dict()}

    def get_aoi_distribution(self):
        """G-05: Statistik AoI vs M/D/1 teoritis (Kaul et al. 2012)."""
        if not self.aoi_all_samples: return {}
        s = self.aoi_all_samples
        mean = sum(s)/len(s)
        variance = sum((x-mean)**2 for x in s)/len(s)
        std = variance**0.5
        s_sorted = sorted(s)
        p95 = s_sorted[int(0.95*len(s_sorted))]
        peak = max(s)
        lam = 10.0; S = 1.0/lam; rho = lam * S
        theoretical_peak = (1.0/(2*lam) + S + rho/(lam*(1-rho))) * 1000 if rho < 1 else 999
        return {'mean_ms': round(mean,1), 'std_ms': round(std,1),
                'p95_ms': round(p95,1), 'peak_ms': round(peak,1),
                'theoretical_peak_ms': round(theoretical_peak,1),
                'n_samples': len(s)}

    def get_platoon_info(self):
        """Kembalikan info semua platoon yang aktif dengan detail lebih lengkap."""
        platoons = {}
        for v in self.vehicles:
            pid = v.platoon_id
            if pid not in platoons:
                platoons[pid] = {'platoon_id': pid, 'vehicles': [], 'leader_id': None,
                                 'avg_speed_kmh': 0, 'in_maneuver': pid in self._maneuver_queue._active_platoons}
            platoons[pid]['vehicles'].append({
                'id': v.id, 'is_leader': v.is_leader,
                'position': round(v.position,1),
                'velocity': round(v.velocity, 2),
                'velocity_kmh': round(v.velocity*3.6,1),
                'fsm_state': v.fsm.state,
                'reported_state': 'TRANSFER' if v.transfer_cooldown > 0 and not v.is_leader else v.fsm.state,
                'collided': v.collided,
                'transfer_cooldown': round(max(0, v.transfer_cooldown), 2),
                'aoi_ms': round(v.fsm.aoi * 1000, 1),
                'spacing_error': round(v.spacing_error, 2),
            })
            if v.is_leader:
                platoons[pid]['leader_id'] = v.id
                platoons[pid]['leader_velocity'] = round(v.velocity, 2)
                platoons[pid]['leader_velocity_kmh'] = round(v.velocity * 3.6, 1)
        # Compute average speed per platoon
        for pid, info in platoons.items():
            speeds = [v['velocity_kmh'] for v in info['vehicles']]
            info['avg_speed_kmh'] = round(sum(speeds)/len(speeds), 1) if speeds else 0
        return list(platoons.values())

    def save_data(self):
        with open(os.path.join(LOGS_DIR, f'{self.experiment_id}.json'),'w') as f:
            json.dump({'experiment_id':self.experiment_id,'config':self.config,
                       'collision_occurred':self.collision_occurred,
                       'collision_log':self.collision_log,
                       'mode_switch_log':self.mode_switch_log,
                       'aoi_distribution':self.get_aoi_distribution(),
                       'data':self.data_log}, f, indent=2)
        with open(os.path.join(ANALYSIS_DIR, f'{self.experiment_id}.csv'),'w',newline='') as f:
            w = csv.writer(f)
            w.writerow(['Time','Vehicle_ID','Platoon_ID','Position','Velocity','Acceleration',
                        'A_Desired','A_Actual','Control_Mode','FSM_State',
                        'Spacing_Error','AoI_ms','AoI_Peak_ms','Network_Delay',
                        'Packet_Loss','Collided','Topology'])
            for entry in self.data_log:
                for v in entry['vehicles']:
                    w.writerow([entry['time'],v['id'],v['platoon_id'],v['position'],v['velocity'],
                                v['acceleration'],v.get('a_desired',0),v.get('a_actual',0),
                                v['control_mode'],v.get('fsm_state',''),v.get('spacing_error',0),
                                v.get('aoi_ms',0),v.get('aoi_peak_ms',0),
                                entry['network']['avg_delay'],entry['network']['packet_loss'],
                                v.get('collided',False),v.get('topology','predecessor')])
        return self.experiment_id

# ============================================================
# GLOBALS
# ============================================================
simulation_engine    = None
compare_engines      = {}
engine_lock          = threading.Lock()
compare_lock         = threading.Lock()
last_experiment_data = None

# ============================================================
# SIMULATION LOOPS
# ============================================================
def simulation_loop():
    """Idle: low polling rate so HTTP (login, pages) stays responsive; active: ~50 Hz."""
    global simulation_engine
    _idle_sleep = 0.1
    _active_sleep = 0.02
    while True:
        sleep_sec = _idle_sleep
        with engine_lock:
            eng = simulation_engine
            if eng and eng.running:
                sleep_sec = _active_sleep
                try:
                    state = eng.step()
                    if state: socketio.emit('state_update', state)
                    if eng.collision_occurred and eng.collision_log:
                        socketio.emit('collision_detected',
                            {'collision': eng.collision_log[-1],
                             'time': eng.time_elapsed})
                except Exception as e:
                    print(f"Sim error: {e}"); traceback.print_exc()
        socketio.sleep(sleep_sec)

def compare_loop():
    global compare_engines
    _idle_sleep = 0.1
    _active_sleep = 0.02
    while True:
        sleep_sec = _idle_sleep
        with compare_lock:
            cacc_eng = compare_engines.get('cacc')
            acc_eng = compare_engines.get('acc')
            if cacc_eng and cacc_eng.running and acc_eng and acc_eng.running:
                sleep_sec = _active_sleep
                try:
                    s_cacc = cacc_eng.step()
                    s_acc = acc_eng.step()
                    if s_cacc and s_acc:
                        socketio.emit('compare_update', {'cacc': s_cacc, 'acc': s_acc})
                    if cacc_eng.collision_occurred or acc_eng.collision_occurred:
                        socketio.emit('compare_collision', {
                            'cacc_collision': cacc_eng.collision_occurred,
                            'acc_collision': acc_eng.collision_occurred})
                except Exception as e:
                    print(f"Compare error: {e}"); traceback.print_exc()
        socketio.sleep(sleep_sec)

def _current_nim():
    """Ambil NIM dari JWT (cross-origin) atau Flask session (local dev)."""
    try:
        verify_jwt_in_request()
        return get_jwt_identity()
    except Exception:
        return session.get('nim')


def login_required(f):
    @wraps(f)
    def dec(*a, **kw):
        nim = _current_nim()
        if not nim:
            if request.path.startswith('/api/') or request.is_json:
                return jsonify({'success': False, 'error': 'Authentication required'}), 401
            return redirect(url_for('login_page'))
        return f(*a, **kw)
    return dec

# ============================================================
# ROUTES
# ============================================================
@app.route('/')
def index(): return redirect(url_for('dashboard') if 'nim' in session else url_for('login_page'))

@app.route('/login', methods=['GET','POST'])
def login_page():
    if request.method == 'GET': return render_template('login.html')
    d = request.get_json(silent=True) or {}
    user = verify_user(d.get('username','').strip(), d.get('password','').strip())
    if user:
        session['nim'] = user['nim']; session['name'] = user['name']
        token = create_access_token(identity=user['nim'],
                                    additional_claims={'name': user['name']})
        return jsonify({'success': True, 'redirect': '/dashboard',
                        'token': token, 'name': user['name'], 'nim': user['nim']})
    return jsonify({'success':False,'message':'NIM atau password salah.'}), 401

@app.route('/logout')
def logout():
    session.clear()
    return jsonify({'success': True})

@app.route('/dashboard')
@login_required
def dashboard(): return render_template('dashboard.html', user=session.get('name'))

@app.route('/configure')
@login_required
def configure(): return render_template('configure.html', user=session.get('name'))

@app.route('/simulation')
@login_required
def simulation(): return render_template('simulation.html')

@app.route('/analysis')
@login_required
def analysis(): return render_template('analysis.html')

@app.route('/compare')
@login_required
def compare(): return render_template('compare.html')

@app.route('/history')
@login_required
def history_page():
    nim = _current_nim()
    return render_template('history.html', history=get_user_history(nim), user=session.get('name', ''))

@app.route('/api/history')
@login_required
def get_history():
    history = get_user_history(_current_nim())
    return jsonify({'history':[{'id':h['id'],'date':h['timestamp'],
        'experiment_id':h['experiment_id'],'num_vehicles':h['num_vehicles']*h['num_platoons'],
        'latency':h['latency_ms'],'packet_loss':h['packet_loss'],
        'collision':bool(h['collision_occurred']),'duration':h['duration_seconds']
    } for h in history]})

@app.route('/history_detail/<int:sim_id>')
@login_required
def history_detail(sim_id):
    sim = get_simulation_detail(sim_id, _current_nim())
    if not sim: return "Not found", 404
    sim['config'] = json.loads(sim['config_json'])
    sim['results'] = json.loads(sim['results_json'])
    return render_template('history_detail.html', simulation=sim, detail=sim, user=session.get('name'))

@app.route('/api/current_user')
@login_required
def current_user():
    nim = _current_nim()
    name = session.get('name') or get_user_name(nim)
    return jsonify({'username': nim, 'name': name})

@app.route('/api/validate_config', methods=['POST'])
@login_required
def validate_config():
    c = request.get_json(); risks, rl = [], 'LOW'
    def warn(msg, level):
        nonlocal rl; risks.append(msg)
        if level=='HIGH' or (level=='MEDIUM' and rl=='LOW'): rl=level
    if c.get('initial_spacing',15)<2:   warn('Spacing < 2m: Extremely dangerous','HIGH')
    elif c.get('initial_spacing',15)<5: warn('Spacing < 5m: High collision risk','MEDIUM')
    if c.get('latency_ms',10)>150:      warn('Latency > 150ms: Control instability','HIGH')
    elif c.get('latency_ms',10)>50:     warn('Latency > 50ms: Reduced performance','MEDIUM')
    if c.get('packet_loss',1)>15:       warn('Packet loss > 15%: Comm failure likely','HIGH')
    elif c.get('packet_loss',1)>5:      warn('Packet loss > 5%: Frequent mode switches','MEDIUM')
    if c.get('tau_act',0.25)>0.5:       warn('tau_act > 0.5s: High lag, instability risk','MEDIUM')
    if c.get('ki',0.01)>0.1:            warn('Ki > 0.1: Potential integrator windup','MEDIUM')
    if c.get('curve_enabled'):
        cs, ce = float(c.get('curve_start_m', 0)), float(c.get('curve_end_m', 0))
        if ce <= cs: warn('Zona tikungan: akhir harus lebih besar dari awal (m)','MEDIUM')
    for k in ('reposition_swap_speed_ms', 'reposition_promote_speed_ms', 'reposition_emergency_speed_ms', 'maneuver_reposition_speed_ms'):
        if float(c.get(k, 20)) > 42:
            warn(f'{k} sangat tinggi (>42 m/s): risiko tabrak saat reposisi','MEDIUM')
            break
    return jsonify({'valid':True,'risk_level':rl,'risks':risks})

@app.route('/api/export_csv/<experiment_id>')
@login_required
def export_csv(experiment_id):
    fp = os.path.join(ANALYSIS_DIR, f'{experiment_id}.csv')
    if not os.path.exists(fp): return jsonify({'error':'Not found'}), 404
    return send_file(fp, as_attachment=True)

@app.route('/api/get_last_experiment')
@login_required
def get_last_experiment():
    if last_experiment_data: return jsonify(last_experiment_data)
    return jsonify({'error':'No data'}), 404

@app.route('/api/experiment_json/<experiment_id>')
@login_required
def get_experiment_json(experiment_id):
    """Muat log penuh dari disk (sama bentuk dengan last_experiment_data) untuk halaman Analysis dari History."""
    if not experiment_id or '..' in experiment_id or '/' in experiment_id or '\\' in experiment_id:
        return jsonify({'error': 'Invalid experiment id'}), 400
    nim = _current_nim()
    if not user_owns_experiment(nim, experiment_id):
        return jsonify({'error': 'Not found or access denied'}), 404
    base = os.path.dirname(os.path.abspath(__file__))
    fp = os.path.normpath(os.path.join(base, LOGS_DIR, experiment_id + '.json'))
    if os.path.basename(fp) != experiment_id + '.json':
        return jsonify({'error': 'Invalid path'}), 400
    if not os.path.isfile(fp):
        return jsonify({'error': 'Log file missing on server (JSON was deleted or not saved)'}), 404
    try:
        with open(fp, 'r', encoding='utf-8') as f:
            payload = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        return jsonify({'error': str(e)}), 500
    out = {
        'experiment_id': payload.get('experiment_id', experiment_id),
        'config': payload.get('config') or {},
        'collision_occurred': bool(payload.get('collision_occurred', False)),
        'collision_log': payload.get('collision_log') or [],
        'mode_switch_log': payload.get('mode_switch_log') or [],
        'aoi_distribution': payload.get('aoi_distribution') or {},
        'data': payload.get('data') or [],
    }
    return jsonify(out)

@app.route('/api/aoi_distribution')
@login_required
def get_aoi_distribution():
    if simulation_engine: return jsonify(simulation_engine.get_aoi_distribution())
    return jsonify({'error':'No engine'}), 404

# ---- NEW-01: Tambah platoon via REST ----
@app.route('/api/add_platoon', methods=['POST'])
@login_required
def api_add_platoon():
    """POST /api/add_platoon  body: {n_vehicles, config}"""
    with engine_lock:
        if not simulation_engine or not simulation_engine.running:
            return jsonify({'success':False,'error':'Simulasi tidak berjalan'}), 400
        data   = request.get_json() or {}
        result = simulation_engine.add_platoon(
            n_vehicles=data.get('n_vehicles'), custom_config=data.get('config'))
    socketio.emit('platoon_added', result)
    return jsonify({'success':True, **result})

# ---- NEW-02: Transfer kendaraan via REST ----
@app.route('/api/swap_leaders', methods=['POST'])
@login_required
def api_swap_leaders():
    """POST /api/swap_leaders  body: {platoon_id_a, platoon_id_b}"""
    data = request.json or {}
    with engine_lock:
        if not simulation_engine or not simulation_engine.running:
            return jsonify({'success': False, 'error': 'Simulasi tidak berjalan'})
        result = simulation_engine.swap_leaders(int(data.get('platoon_id_a', -1)), int(data.get('platoon_id_b', -1)))
    if result['success']: socketio.emit('leaders_swapped', result)
    return jsonify(result)

@app.route('/api/promote_leader', methods=['POST'])
@login_required
def api_promote_leader():
    """POST /api/promote_leader  body: {platoon_id}"""
    data = request.json or {}
    with engine_lock:
        if not simulation_engine or not simulation_engine.running:
            return jsonify({'success': False, 'error': 'Simulasi tidak berjalan'})
        result = simulation_engine.promote_next_leader(int(data.get('platoon_id', -1)))
    if result['success']: socketio.emit('leader_promoted', result)
    return jsonify(result)

@app.route('/api/transfer_vehicle', methods=['POST'])
@login_required
def api_transfer_vehicle():
    """POST /api/transfer_vehicle  body: {vehicle_id, target_platoon_id}"""
    with engine_lock:
        if not simulation_engine or not simulation_engine.running:
            return jsonify({'success':False,'error':'Simulasi tidak berjalan'}), 400
        data = request.get_json() or {}
        vid  = data.get('vehicle_id'); tpid = data.get('target_platoon_id')
        if vid is None or tpid is None:
            return jsonify({'success':False,'error':'vehicle_id dan target_platoon_id wajib'}), 400
        result = simulation_engine.transfer_vehicle(int(vid), int(tpid))
    if result['success']: socketio.emit('vehicle_transferred', result)
    return jsonify(result)

@app.route('/api/validate_maneuver', methods=['POST'])
@login_required
def api_validate_maneuver():
    """
    Validasi prasyarat manuver sebelum eksekusi.
    POST body: {type: 'transfer'|'swap'|'promote', params: {...}}
    """
    with engine_lock:
        if not simulation_engine:
            return jsonify({'valid': False, 'reason': 'Simulasi belum dimulai'})
        data = request.get_json() or {}
        mtype = data.get('type')
        params = data.get('params', {})
        if mtype == 'transfer':
            ok, reason = simulation_engine.validate_transfer(
                int(params.get('vehicle_id', -1)),
                int(params.get('target_platoon_id', -1)))
        elif mtype == 'swap':
            ok, reason = simulation_engine.validate_swap(
                int(params.get('platoon_id_a', -1)),
                int(params.get('platoon_id_b', -1)))
        elif mtype == 'promote':
            ok, reason = simulation_engine.validate_promote(
                int(params.get('platoon_id', -1)))
        else:
            return jsonify({'valid': False, 'reason': 'Tipe manuver tidak dikenal'})
        # Untuk swap, sertakan info kecepatan
        extra = {}
        if mtype == 'swap' and ok:
            pvs_a = sorted([v for v in simulation_engine.vehicles if v.platoon_id == int(params.get('platoon_id_a', -1))], key=lambda v: -v.position)
            pvs_b = sorted([v for v in simulation_engine.vehicles if v.platoon_id == int(params.get('platoon_id_b', -1))], key=lambda v: -v.position)
            if pvs_a and pvs_b:
                extra['vel_a_ms'] = round(pvs_a[0].velocity, 2)
                extra['vel_b_ms'] = round(pvs_b[0].velocity, 2)
                extra['vel_diff_ms'] = round(abs(pvs_a[0].velocity - pvs_b[0].velocity), 2)
                extra['vel_a_kmh'] = round(pvs_a[0].velocity * 3.6, 1)
                extra['vel_b_kmh'] = round(pvs_b[0].velocity * 3.6, 1)
    return jsonify({'valid': ok, 'reason': reason, **extra})

@app.route('/api/maneuver_queue_status')
@login_required
def api_maneuver_queue_status():
    """Kembalikan status ManeuverQueue."""
    with engine_lock:
        if not simulation_engine:
            return jsonify({'error': 'Simulasi belum dimulai'})
        return jsonify(simulation_engine.get_maneuver_queue_status())

# ---- Info platoon saat ini ----
@app.route('/api/platoon_info')
@login_required
def api_platoon_info():
    with engine_lock:
        if not simulation_engine:
            return jsonify({'platoons':[],'error':'Simulasi belum dimulai'})
        return jsonify({'platoons': simulation_engine.get_platoon_info(),
                        'num_platoons': simulation_engine._next_pid})

# ============================================================
# SOCKETIO
# ============================================================
@socketio.on('connect')
def handle_connect(auth):
    token = (auth or {}).get('token', '')
    nim = None
    name = None
    if token:
        try:
            data = decode_token(token)
            nim = data.get('sub')
            name = data.get('name') or get_user_name(nim)
        except Exception:
            pass
    if not nim:
        nim = session.get('nim')
        name = session.get('name')
    _socket_users[request.sid] = {'nim': nim, 'name': name}
    emit('connection_status', {'status': 'connected'})

@socketio.on('disconnect')
def handle_disconnect():
    _socket_users.pop(request.sid, None)

@socketio.on('start_simulation')
def handle_start(data):
    global simulation_engine
    with engine_lock:
        cfg = dict(data.get('config') or {})
        simulation_engine = SimulationEngine(cfg, mode='cacc')
        simulation_engine.running = True
        emit('simulation_started', {
            'status': 'running',
            'experiment_id': simulation_engine.experiment_id,
            'random_seed_used': simulation_engine.config.get('random_seed_used'),
        })

@socketio.on('stop_simulation')
def handle_stop():
    global simulation_engine, last_experiment_data
    with engine_lock:
        if simulation_engine:
            simulation_engine.running = False
            eid = simulation_engine.save_data()
            errors = [abs(v.get('spacing_error',0)) for e in simulation_engine.data_log
                      for v in e.get('vehicles',[]) if not v.get('is_leader',False)]
            avg_err = sum(errors)/len(errors) if errors else 0
            final_si = simulation_engine.data_log[-1].get('stability_index',0) if simulation_engine.data_log else 0
            results = {'collision_occurred':simulation_engine.collision_occurred,
                       'duration':simulation_engine.time_elapsed,
                       'avg_spacing_error':avg_err,'final_stability':final_si,
                       'collision_log':simulation_engine.collision_log,
                       'mode_switches':len(simulation_engine.mode_switch_log)}
            try:
                nim = (_socket_users.get(request.sid) or {}).get('nim') or session.get('nim')
                if nim: save_simulation(nim, eid, simulation_engine.config, results)
            except Exception as e: print(f"Save error: {e}")
            last_experiment_data = {'experiment_id':eid,'config':simulation_engine.config,
                'collision_occurred':simulation_engine.collision_occurred,
                'collision_log':simulation_engine.collision_log,
                'mode_switch_log':simulation_engine.mode_switch_log,
                'aoi_distribution':simulation_engine.get_aoi_distribution(),
                'data':simulation_engine.data_log}
            emit('simulation_stopped',{'status':'stopped','experiment_id':eid,'redirect':'/analysis'})

@socketio.on('inject_disturbance')
def handle_disturbance(data):
    with engine_lock:
        if simulation_engine:
            simulation_engine.inject_disturbance(data.get('type'))
            emit('disturbance_injected',{'type':data.get('type')})

# ---- NEW-01: Socket - tambah platoon ----
@socketio.on('add_platoon')
def handle_add_platoon(data):
    """data: {n_vehicles, config}"""
    with engine_lock:
        if not simulation_engine or not simulation_engine.running:
            emit('platoon_error', {'error':'Simulasi tidak berjalan'}); return
        result = simulation_engine.add_platoon(
            n_vehicles=data.get('n_vehicles'), custom_config=data.get('config'))
    emit('platoon_added', result)
    socketio.emit('platoon_added', result)

# ---- NEW-02: Socket - transfer kendaraan ----
@socketio.on('transfer_vehicle')
def handle_transfer_vehicle(data):
    """data: {vehicle_id, target_platoon_id}"""
    with engine_lock:
        if not simulation_engine or not simulation_engine.running:
            emit('transfer_error', {'error':'Simulasi tidak berjalan'}); return
        result = simulation_engine.transfer_vehicle(
            int(data.get('vehicle_id', -1)),
            int(data.get('target_platoon_id', -1)))
    if result['success']:
        emit('vehicle_transferred', result)
        socketio.emit('vehicle_transferred', result)
    else:
        emit('transfer_error', result)

# ---- NEW-04: Socket - tukar leader antar platoon ----
@socketio.on('swap_leaders')
def handle_swap_leaders(data):
    """data: {platoon_id_a, platoon_id_b}"""
    with engine_lock:
        if not simulation_engine or not simulation_engine.running:
            emit('leader_error', {'error': 'Simulasi tidak berjalan'}); return
        result = simulation_engine.swap_leaders(
            int(data.get('platoon_id_a', -1)),
            int(data.get('platoon_id_b', -1)))
    if result['success']:
        emit('leaders_swapped', result)
        socketio.emit('leaders_swapped', result)
    else:
        emit('leader_error', result)

# ---- NEW-05: Socket - promosi leader baru dalam platoon ----
@socketio.on('promote_leader')
def handle_promote_leader(data):
    """data: {platoon_id}"""
    with engine_lock:
        if not simulation_engine or not simulation_engine.running:
            emit('leader_error', {'error': 'Simulasi tidak berjalan'}); return
        result = simulation_engine.promote_next_leader(int(data.get('platoon_id', -1)))
    if result['success']:
        emit('leader_promoted', result)
        socketio.emit('leader_promoted', result)
    else:
        emit('leader_error', result)

@socketio.on('validate_maneuver')
def handle_validate_maneuver(data):
    """Real-time validation over socket: {type, params}"""
    with engine_lock:
        if not simulation_engine:
            emit('maneuver_validation', {'valid': False, 'reason': 'Simulasi belum dimulai'}); return
        mtype = data.get('type')
        params = data.get('params', {})
        extra = {}
        if mtype == 'transfer':
            ok, reason = simulation_engine.validate_transfer(
                int(params.get('vehicle_id', -1)), int(params.get('target_platoon_id', -1)))
        elif mtype == 'swap':
            ok, reason = simulation_engine.validate_swap(
                int(params.get('platoon_id_a', -1)), int(params.get('platoon_id_b', -1)))
            if ok:
                pvs_a = sorted([v for v in simulation_engine.vehicles if v.platoon_id == int(params.get('platoon_id_a', -1))], key=lambda v: -v.position)
                pvs_b = sorted([v for v in simulation_engine.vehicles if v.platoon_id == int(params.get('platoon_id_b', -1))], key=lambda v: -v.position)
                if pvs_a and pvs_b:
                    extra = {'vel_a_kmh': round(pvs_a[0].velocity*3.6,1),
                             'vel_b_kmh': round(pvs_b[0].velocity*3.6,1),
                             'vel_diff_ms': round(abs(pvs_a[0].velocity-pvs_b[0].velocity),2)}
        elif mtype == 'promote':
            ok, reason = simulation_engine.validate_promote(int(params.get('platoon_id', -1)))
        else:
            ok, reason = False, 'Unknown type'
    emit('maneuver_validation', {'valid': ok, 'reason': reason, 'type': mtype, **extra})

# ---- Info platoon via socket ----
@socketio.on('request_platoon_info')
def handle_platoon_info():
    with engine_lock:
        if simulation_engine:
            emit('platoon_info', {'platoons': simulation_engine.get_platoon_info(),
                                  'num_platoons': simulation_engine._next_pid})
        else:
            emit('platoon_info', {'platoons': [], 'num_platoons': 0})

# ---- G-08: Compare mode ----
@socketio.on('start_compare')
def handle_start_compare(data):
    global compare_engines
    with compare_lock:
        cfg = dict(data.get('config') or {})
        if cfg.get('random_seed_used') is None:
            rs = cfg.get('random_seed')
            if rs is not None and str(rs).strip() != '':
                base = int(rs)
            else:
                base = random.randrange(0, 2**31)
            cfg['random_seed_used'] = base
        cfg_acc = dict(cfg)
        cfg_acc['latency_ms'] = 1000
        cfg_acc['random_seed_used'] = int(cfg['random_seed_used']) + 1000003
        compare_engines['cacc'] = SimulationEngine(cfg, mode='cacc')
        compare_engines['acc']  = SimulationEngine(cfg_acc, mode='acc_only')
        compare_engines['cacc'].running = True
        compare_engines['acc'].running  = True
        emit('compare_started', {
            'status': 'running',
            'random_seed_cacc': compare_engines['cacc'].config.get('random_seed_used'),
            'random_seed_acc':  compare_engines['acc'].config.get('random_seed_used'),
        })

@socketio.on('stop_compare')
def handle_stop_compare():
    global compare_engines
    with compare_lock:
        for eng in compare_engines.values():
            if eng: eng.running = False
        compare_engines = {}
        emit('compare_stopped', {'status':'stopped'})

@socketio.on('inject_compare_disturbance')
def handle_compare_disturbance(data):
    with compare_lock:
        for eng in compare_engines.values():
            if eng and eng.running: eng.inject_disturbance(data.get('type'))

# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print("="*70)
    print("V2V 5G PLATOONING SIMULATOR â€” RESEARCH BUILD v5.0")
    print("NEW-01: Dynamic platoon addition via API + Socket")
    print("NEW-02: Vehicle transfer between platoons A->B via API + Socket")
    print("NEW-04: Swap leaders between platoons via API + Socket")
    print("NEW-05: Promote next leader within platoon via API + Socket")
    print("VIS-01: Multi-lane road â€” each platoon in its own lane")
    print("G-02:   3GPP TR38.885 path loss channel model")
    print("G-03:   Stochastic ECDSA delay N(3ms, 0.5ms)")
    print("G-05:   AoI distribution + M/D/1 theoretical comparison")
    print("G-06:   Communication topology selector")
    print("G-08:   ACC vs CACC split-screen comparison mode")
    print("G-09:   FFT string stability data exposed via API")
    print("G-10:   Per-link latency tracking")
    print("="*70)
    socketio.start_background_task(simulation_loop)
    socketio.start_background_task(compare_loop)
    _port = int(os.environ.get('PORT', '5000'))
    socketio.run(app, host='0.0.0.0', port=_port, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)
