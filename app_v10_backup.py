"""
V2V 5G PLATOONING RESEARCH SIMULATOR
Research-Grade Academic Build v5.1 (Bugfix Release)
=====================================================
Semua fitur v5.0 tetap ada. Perbaikan bug kritis v5.1:

  FIX B-01: Race condition auto-disconnect kendaraan pengikut saat transfer platoon
             → Tambah Network5G._buffer_lock untuk sinkronisasi Thread A/B
             → _inject_immediate_packet() kini menggunakan lock yang sama
             → clear_buffer() helper thread-safe menggantikan akses langsung
  FIX B-02: Inisialisasi jarak salah pada add_platoon()
             → Gunakan eq_gap = spacing + h*speed + vehicle_length (bukan spacing mentah)
  FIX B-03: Formula AoI M/D/1 selalu menghasilkan 999ms
             → Gunakan λ dan S aktual dari metrik simulasi (bukan hardcode lam=10)
  FIX B-04: Deteksi tabrakan pada list tidak terurut
             → Urutkan semua kendaraan berdasarkan posisi fisik sebelum cek tabrakan
             → Deteksi lintas platoon (cross-platoon collision) diaktifkan
  FIX B-05: Minimum velocity floor 1.0 m/s di update_in_transit()
             → Hapus floor max(1.0,...) → max(0.0,...) agar bisa berhenti penuh
             → Naikkan batas decel dari -2.0 ke -4.0 m/s² untuk respons darurat
  FIX S-01: Endpoint REST kritis tanpa autentikasi
             → Tambah @login_required pada api_swap_leaders & api_promote_leader
             → CORS origin dapat dikonfigurasi via env var CORS_ORIGINS
  FIX F-04: Input validation — tambah batas atas n_vehicles di add_platoon()

Referensi Teori:
  - Ploeg et al. (2011) CACC PID+FF
  - Naus et al. (2010) String stability: AR_i < 1
  - 3GPP TR 38.885 V2V path loss
  - Brecht et al. (2018) ECDSA-256 delay
  - Kaul et al. (2012) AoI M/D/1
  - Bergenhem et al. (2012) Platoon split/merge/leader election
  - GCDC (2016) Safety envelope
"""

from flask import Flask, render_template, request, jsonify, session, send_file, redirect, url_for
from flask_socketio import SocketIO, emit
import os, sys, json, time, threading, random, math, csv
from datetime import datetime
from collections import deque
from functools import wraps
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import verify_user, get_user_name, save_simulation, get_user_history, get_simulation_detail

app = Flask(__name__, template_folder='../frontend', static_folder='../frontend')
app.config['SECRET_KEY'] = 'v2v-research-platform-2025'
# FIX S-01: cors_allowed_origins="*" sebaiknya diganti daftar domain eksplisit di produksi.
# Untuk development/localhost diizinkan wildcard. Set env var CORS_ORIGINS untuk override.
import os as _os
_cors_origins = _os.environ.get('CORS_ORIGINS', '*')
socketio = SocketIO(app, async_mode='threading', cors_allowed_origins=_cors_origins,
                    ping_timeout=120, ping_interval=30)

LOGS_DIR = '../data/logs'
ANALYSIS_DIR = '../data/analysis'
for d in [LOGS_DIR, ANALYSIS_DIR]:
    os.makedirs(d, exist_ok=True)

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
# MANEUVER QUEUE — Deadlock prevention for N>2 platoons
# Ref: Dokumen §7.5 Theorem 4.1 (Isolation Transfer)
# ============================================================
class ManeuverQueue:
    """
    Serializes concurrent maneuver requests. Prevents concurrent operations
    on overlapping platoons (race condition / deadlock prevention).
    Ref: §7.5.1 Pencegahan Deadlock, §4.6 Teorema 4.1
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
        self.security_delay_mean = config.get('security_delay_ms', 3.0)
        self.security_delay_std  = 0.5
        self.propagation_delay  = 2.0
        self.transmission_delay = 1.0
        self.processing_delay   = 3.0
        self.queuing_delay      = 2.0
        self.packet_buffers = {}
        # FIX B-01/R-01: Lock khusus untuk operasi packet_buffers
        # agar operasi clear/add/read dari thread berbeda tidak race condition.
        # Digunakan di transmit(), get_packets_for_follower(), dan
        # _inject_immediate_packet() secara konsisten.
        self._buffer_lock = threading.Lock()
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

    def update(self, dt):
        if self.degraded:
            self.degradation_timer -= dt
            if self.degradation_timer <= 0:
                self.degraded = False
                self.packet_loss = 0.01
                self.queuing_delay = 2.0

    def transmit(self, packet, current_time, follower_id, sender_pos=0, receiver_pos=0, num_blockers=0):
        self.packets_sent += 1
        dist    = abs(sender_pos - receiver_pos) if abs(sender_pos - receiver_pos) > 1 else 15.0
        is_los  = (num_blockers == 0)
        phys_loss = channel_model.packet_loss_prob(dist, is_los, self.packet_loss)
        if random.random() < phys_loss:
            self.packets_lost += 1
            return None
        net_delay = self._calc_network_delay()
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
        # FIX B-01/R-01: proteksi buffer dengan _buffer_lock
        with self._buffer_lock:
            if follower_id not in self.packet_buffers:
                self.packet_buffers[follower_id] = PacketBuffer()
            self.packet_buffers[follower_id].add_packet(pkt, release_time)
        return total_delay_ms

    def get_packets_for_follower(self, follower_id, current_time):
        # FIX B-01/R-01: proteksi buffer dengan _buffer_lock saat membaca
        with self._buffer_lock:
            if follower_id not in self.packet_buffers: return []
            return self.packet_buffers[follower_id].get_ready_packets(current_time)

    def clear_buffer(self, follower_id):
        """FIX B-01/R-01: Thread-safe buffer clear menggunakan _buffer_lock."""
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
        return {
            'latency_ms': self.latency_ms, 'packet_loss': round(self.packet_loss*100, 2),
            'jitter_ms': self.jitter_ms, 'bandwidth_mbps': self.bandwidth_mbps,
            'network_slicing': self.network_slicing, 'rsu_enabled': self.rsu_enabled,
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
    STATE_TRANSFER  = 'TRANSFER'   # Cooldown setelah manuver (Ref: §4.3.4, §8.1)
    # h_maneuver = 1.5 × h_normal saat cooldown — Ref: §8.6 Proposisi 8.1
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

    def update(self, dt, sim_time, gap, a_actual, transfer_cooldown=0.0):
        if self.last_packet_sim_time is not None:
            self.aoi = sim_time - self.last_packet_sim_time
        else:
            self.aoi += dt
        prev = self.state
        # TRANSFER state overrides normal FSM saat cooldown aktif
        if transfer_cooldown > 0:
            if gap < self.EMERGENCY_GAP or a_actual < self.EMERGENCY_ACCEL:
                self.state = self.STATE_EMERGENCY
                self.emergency_braking = True
            else:
                self.emergency_braking = False
                # FIX-TRANSFER-RECOVERY: Tambahkan recovery dari EMERGENCY di TRANSFER branch.
                # Bug sebelumnya: saat state=EMERGENCY dan gap sudah aman tapi cooldown masih >0,
                # kendaraan tetap di EMERGENCY karena TRANSFER branch tidak punya recovery path.
                # Kendaraan stuck di EMERGENCY sampai cooldown habis (2 detik).
                # Fix: izinkan transisi EMERGENCY→TRANSFER jika gap sudah aman (> 1.8×EMERGENCY_GAP).
                if self.state == self.STATE_EMERGENCY:
                    if gap > self.EMERGENCY_GAP * 1.8:
                        self.state = self.STATE_TRANSFER
                        self.emergency_braking = False
                    # else: tetap di EMERGENCY sampai gap aman
                else:
                    self.state = self.STATE_TRANSFER
            return self.state, prev
        if gap < self.EMERGENCY_GAP or a_actual < self.EMERGENCY_ACCEL:
            self.state = self.STATE_EMERGENCY
            self.emergency_braking = True
        else:
            self.emergency_braking = False
            if self.state == self.STATE_EMERGENCY:
                # FIX-EMRG-RECOVERY: Recovery hanya berdasarkan gap (BUKAN a_actual).
                # Bug sebelumnya: kondisi a_actual > -1.05 tidak pernah terpenuhi
                # karena EMERGENCY bypass lag menetapkan a_actual=-6 secara permanen.
                # Solusi: jika gap sudah aman (> 1.8×EMERGENCY_GAP = 21.6m),
                # langsung kembali ke CACC/DEGRADED tanpa cek a_actual.
                if gap > self.EMERGENCY_GAP * 1.8:
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
        # ── MULTI-PHASE TRANSFER (v7) ─────────────────────────
        # maneuver_phase: None | 'DEPARTING' | 'IN_TRANSIT'
        # Sesuai §4.3 Protokol Transfer 4 Fase
        self.maneuver_phase = None
        self.transit_data   = {}   # {target_pid, depart_elapsed, depart_duration, ...}
        # ── P-02: Grace period sebelum communication_ok = False ───────────────
        # Mencegah false-disconnect selama 1–3 step jeda paket akibat network delay.
        # Ref: Riset B-01 §8.3 Perbaikan P-02
        self._no_data_grace = 0.05   # 50 ms — cukup untuk 5 step (dt=10ms)
        # ── P-03: Stabilization window pasca transfer ──────────────────────────
        # FSM sedikit lebih toleran selama window ini agar recovery lebih mudah.
        self.stabilization_window = 0.0

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

    def _pid_ff_control(self, l_pos, l_vel, l_accel, dt, net_delay_s=0.0):
        """
        CACC PID+FF - Ploeg et al. (2011) Eq.(5)-(7)
        Desired gap: d_i = r + h_i * v_i  (FOLLOWER velocity)
        FIX-COL-6: kompensasi posisi predecessor berdasarkan network delay
        FIX-COL-7: zona rem pre-emergency saat gap menyusut cepat
        Ref §8.6 Prop 8.1: h_maneuver = 1.5 * h_normal saat cooldown
        """
        # Boost headway saat cooldown aktif (Ref: §8.6 Proposisi 8.1)
        h_base = self.time_headway * (1.5 if self.transfer_cooldown > 0 else 1.0)
        hw  = h_base * self.fsm.get_headway_multiplier()
        d_g = self.desired_gap + hw * self.velocity

        # FIX-COL-6: estimasi posisi predecessor saat ini berdasarkan delay
        l_pos_est = l_pos + l_vel * net_delay_s
        a_g = l_pos_est - self.position - self.length
        e_pos = a_g - d_g
        e_vel = l_vel - self.velocity
        self.spacing_error = a_g - (self.desired_gap + self.time_headway * self.velocity)

        u_pid  = self.kp*e_pos + self.kd*e_vel + self.ki*self.integral_e
        a_cmd  = max(self.A_CMD_MIN, min(self.A_CMD_MAX, u_pid + self.alpha_ff*l_accel))

        # FIX-COL-7: pre-emergency braking — paksa rem jika jarak menyusut berbahaya
        PRE_EMRG = self.fsm.EMERGENCY_GAP * 2.0
        if a_g < PRE_EMRG and e_vel < -1.0:
            urgency = max(0.0, min(1.0, (PRE_EMRG - a_g) / PRE_EMRG))
            soft_brake = -urgency * 4.0 * (-e_vel / 5.0)
            a_cmd = min(a_cmd, soft_brake)

        self._update_integral(e_pos, a_cmd, dt)
        return a_cmd

    def update_leader(self, target_speed, dt, profile='normal'):
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
        self._apply_actuation_lag(dt)
        self.velocity  = max(0, min(self.max_velocity, self.velocity + self.a_actual*dt))
        self.position += self.velocity * dt
        if self.transfer_cooldown > 0: self.transfer_cooldown -= dt

    def update_follower(self, leader_packet, current_time, dt, leader_direct_packet=None, net_delay_s=0.0):
        """G-06: hybrid topology menggunakan akselerasi leader langsung."""
        # FIX-PRED-3: Tolak paket stale jika posisi predecessor ternyata di belakang
        # kendaraan ini — bisa terjadi saat predecessor baru saja berpindah platoon.
        # FIXED: jangan reset last_packet_time (itu terlalu agresif — menyebabkan
        # update_without_data langsung menganggap timeout dan comm_ok=False).
        # Cukup skip update paket ini dan gunakan data lama.
        l_pos_check = leader_packet.get('position', self.position)
        if l_pos_check <= self.position + self.length:
            # Paket tidak valid (predecessor di belakang kita) → gunakan data lama saja
            # jangan reset last_packet_time agar grace period P-02 tetap berlaku
            if self.last_leader_data:
                self.update_without_data(dt, current_time)
            else:
                self.communication_ok = False
            return

        self.last_packet_time = current_time
        self.last_leader_data = leader_packet
        self.communication_ok = True
        self.fsm.packet_received(current_time)
        # P-03: decay stabilization_window setiap step
        if self.stabilization_window > 0:
            self.stabilization_window = max(0.0, self.stabilization_window - dt)
        # P-02: Setelah stabilization_window selesai, kembalikan grace period ke default
        if self.stabilization_window <= 0 and self._no_data_grace > 0.05:
            self._no_data_grace = 0.05
        l_pos   = leader_packet['position']
        l_vel   = leader_packet['velocity']
        l_accel = leader_packet.get('acceleration', 0.0)
        # P-04: Batasi propagasi feedforward EMERGENCY ke downstream.
        # Ref: Riset B-01 §8.5 Perbaikan P-04
        # Saat predecessor dalam EMERGENCY (a≈-6), feedforward αff×(-6)=-5.4 langsung
        # memicu FSM EMERGENCY di kendaraan ini (< -3.5 threshold). Cap -1.5 m/s²
        # mempertahankan rem ringan tanpa mempropagasi panic-braking ke seluruh platoon.
        pred_state = leader_packet.get('fsm_state', 'CACC')
        if pred_state == 'EMERGENCY' and self.transfer_cooldown == 0.0:
            l_accel = max(l_accel, -1.5)   # batasi feedforward rem darurat
        if self.topology == 'hybrid' and leader_direct_packet:
            l_accel = leader_direct_packet.get('acceleration', l_accel)
        gap = l_pos - self.position - self.length
        state, _ = self.fsm.update(dt, current_time, gap, self.a_actual, self.transfer_cooldown)
        self.control_mode = state
        self.aoi_running_peak = max(self.aoi_running_peak, self.fsm.aoi * 1000)

        # FIX-COL-5: EMERGENCY bypass actuation lag agar rem langsung penuh
        if state == 'EMERGENCY':
            self.a_desired = self.max_decel   # -6 m/s² penuh, bukan A_CMD_MIN lama (-5)
            self._apply_actuation_lag(dt, bypass=True)
        elif state in ('CACC', 'DEGRADED'):
            ff = l_accel if state == 'CACC' else 0.0
            self.a_desired = self._pid_ff_control(l_pos, l_vel, ff, dt, net_delay_s)
            self._apply_actuation_lag(dt)
        else:
            self.a_desired = self._acc_control(l_pos, l_vel)
            self._apply_actuation_lag(dt)
        self.velocity  = max(0, min(self.max_velocity, self.velocity + self.a_actual*dt))
        self.position += self.velocity * dt
        if self.transfer_cooldown > 0: self.transfer_cooldown -= dt

    def update_without_data(self, dt, current_time):
        # P-02: Grace period — jangan langsung set False jika baru 1–3 step tanpa paket.
        # Mencegah false-disconnect saat network delay normal (11ms > dt=10ms).
        # Ref: Riset B-01 §8.3 Perbaikan P-02
        if self.last_packet_time is not None:
            time_since_last = current_time - self.last_packet_time
            if time_since_last < self._no_data_grace:
                # Masih dalam grace period — pertahankan status komunikasi
                self.communication_ok = True
            else:
                self.communication_ok = False
        else:
            self.communication_ok = False
        if self.last_leader_data:
            gap  = self.last_leader_data['position'] - self.position - self.length
            lvel = self.last_leader_data.get('velocity', None)
        else:
            gap  = 999.0
            lvel = None
        state, _ = self.fsm.update(dt, current_time, gap, self.a_actual, self.transfer_cooldown)
        self.control_mode = state
        if state == 'EMERGENCY':
            self.a_desired = self.max_decel   # FIX-COL-5: full -6 m/s²
            self._apply_actuation_lag(dt, bypass=True)  # bypass lag saat darurat
        elif self.last_leader_data:
            self.a_desired = self._acc_control(self.last_leader_data['position'], lvel)
            self._apply_actuation_lag(dt)
        else:
            self.a_desired = max(self.A_CMD_MIN, -2.0)  # gentle brake tanpa data
            self._apply_actuation_lag(dt)
        self.velocity  = max(0, min(self.max_velocity, self.velocity + self.a_actual*dt))
        self.position += self.velocity * dt
        if self.transfer_cooldown > 0: self.transfer_cooldown -= dt

    def _acc_control(self, leader_pos, leader_vel=None):
        """
        ACC fallback — digunakan saat komunikasi terputus.
        Menggunakan gap + kecepatan relatif agar tidak terjadi tumbukan.
        Ref: Rajamani (2012) Vehicle Dynamics and Control, Ch.8
        """
        gap = leader_pos - self.position - self.length
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
        Fase 3 (§4.3.3): Kendaraan berpindah lajur menuju platoon tujuan.
        
        FIX-TRANSIT (BUG-2 ROOT CAUSE):
        Dalam simulasi 1D, perpindahan lajur adalah gerakan LATERAL (divisualisasikan).
        Posisi longitudinal tetap berubah sesuai kecepatan — kendaraan TIDAK berhenti.
        
        Root cause bug asli: saat target tail ada di BELAKANG kendaraan (posisi lebih
        rendah), actual_gap negatif → a_cmd = max_decel → kendaraan berhenti menunggu →
        platoon tujuan "menembus" kendaraan secara visual → join saat tail lewat.
        
        Perbaikan: kendaraan IN_TRANSIT hanya menyamakan kecepatan dengan target platoon.
        Join terjadi ketika selisih posisi longitudinal masuk dalam window yang wajar.
        Kendaraan tidak pernah berhenti total — ini sesuai teori perpindahan lajur (§4.3.3).
        """
        if target_tail is None or target_tail.collided:
            self.position += self.velocity * dt
            return False

        actual_gap = target_tail.position - self.position - self.length
        d_ref = self.desired_gap + self.time_headway * target_tail.velocity
        e_gap = actual_gap - d_ref
        e_vel = target_tail.velocity - self.velocity

        # FIX B-05: Naikkan batas pengereman dari -2.0 ke -4.0 m/s²
        # agar kendaraan IN_TRANSIT bisa mengikuti pengereman darurat target.
        # Bug sebelumnya: max(-2.0, ...) tidak cukup jika target_tail braking -6 m/s².
        a_cmd = max(-4.0, min(self.max_accel, 1.2 * e_vel))

        # FIX-TRANSIT-2: Jika target ada di depan (actual_gap > d_ref * 2),
        # boleh sedikit akselerasi untuk mengejar — tapi batasi agar tidak terlalu agresif.
        if actual_gap > d_ref * 2.0 and actual_gap > 0:
            a_approach = max(-1.0, min(1.5, 0.2 * e_gap + 0.8 * e_vel))
            a_cmd = a_approach

        # Actuation lag
        self.a_actual += (dt / self.tau_act) * (a_cmd - self.a_actual)
        self.a_actual = max(-4.0, min(self.max_accel, self.a_actual))
        self.acceleration = self.a_actual
        # FIX B-05: Hapus floor 1.0 m/s — kendaraan harus bisa berhenti penuh
        # Bug sebelumnya: max(1.0, ...) mencegah kendaraan berhenti bahkan dalam darurat.
        self.velocity = max(0.0, min(self.max_velocity, self.velocity + self.a_actual * dt))
        self.position += self.velocity * dt
        if self.transfer_cooldown > 0:
            self.transfer_cooldown -= dt

        # FIX-TRANSIT-3: Kondisi join yang realistis (dua mode).
        #
        # Mode A: V1 masih di belakang atau dekat P1_tail (normal approach)
        #   → join saat gap dalam window [-10, d_ref+20] DAN kecepatan match
        #
        # Mode B: V1 sudah JAUH DI DEPAN P1_tail (overshoot, actual_gap << -10)
        #   → V1 tidak bisa mundur di 1D. Solusi: join segera saat kecepatan match.
        #   → Backend akan reposition V1 di belakang P1_tail saat join.
        #   → Ini fisik yang valid: perpindahan lajur = mencocokan kecepatan,
        #     bukan posisi longitudinal yang harus tepat.
        join_vel_ok = abs(e_vel) < 2.5
        if actual_gap < -10.0:
            # Mode B: overshoot — join berdasarkan kecepatan saja
            return join_vel_ok
        join_gap_ok = actual_gap < (d_ref + 20.0)
        return (join_gap_ok and join_vel_ok)

    def to_dict(self):
        # Laporkan 'TRANSFER' saat cooldown aktif (Ref: §3.4 FSM Dasar)
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
            'transfer_cooldown': round(max(0.0, self.transfer_cooldown), 2),
            'maneuver_phase': self.maneuver_phase,   # v7: expose fase manuver ke frontend
            'transit_src_pid':    self.transit_data.get('src_pid')    if self.maneuver_phase else None,
            'transit_target_pid': self.transit_data.get('target_pid') if self.maneuver_phase else None,
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
        # NEW: ManeuverQueue untuk mencegah deadlock (Ref: §7.5.1, §4.6 Teorema 4.1)
        self._maneuver_queue = ManeuverQueue()
        # Emergency promotion threshold (Ref: §5.5 Algorithm 2)
        self._emergency_promo_threshold = 0.5  # 500ms AoI
        self._initialize_network()
        self._initialize_vehicles()

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

    # ── HELPER: INJECT PAKET INSTAN KE BUFFER PENERIMA ─────────────────────
    def _inject_immediate_packet(self, follower_vehicle, predecessor_vehicle):
        """
        Inject satu paket INSTAN (release_time = current_time) dari predecessor_vehicle
        ke packet_buffer follower_vehicle.

        ROOT CAUSE FIX (DISCONNECT):
          Setelah buffer dibersihkan (clear()), step berikutnya tidak ada paket siap.
          ready = [] => update_without_data() => communication_ok = False.
          Seeding last_leader_data saja TIDAK cukup karena update_without_data() SELALU
          set communication_ok = False di baris pertamanya.

        FIX: Inject paket dengan delay=0 sehingga get_packets_for_follower() langsung
          mengembalikan paket => update_follower() dipanggil => communication_ok = True.
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
            'fsm_state':      predecessor_vehicle.fsm.state,  # P-04: diperlukan untuk cap feedforward
        }
        # FIX B-01/R-01: gunakan _buffer_lock yang SAMA dengan transmit()
        # dan get_packets_for_follower() untuk mencegah race condition.
        # release_time = self.time_elapsed → tersedia SEKARANG saat dibaca.
        with self.network._buffer_lock:
            if fid not in self.network.packet_buffers:
                self.network.packet_buffers[fid] = PacketBuffer()
            self.network.packet_buffers[fid].add_packet(pkt, self.time_elapsed)

    # ---- NEW-01: TAMBAH PLATOON DINAMIS ----
    # Ref: Bergenhem et al. (2012) platoon formation protocol
    def add_platoon(self, n_vehicles=None, custom_config=None):
        """
        Tambahkan platoon baru ke simulasi yang sedang berjalan.
        Platoon baru diposisikan 100m di belakang kendaraan paling belakang.
        Kecepatan awal 90% dari desired_speed untuk mengejar.

        FIX B-02: Gunakan eq_gap = spacing + h*speed + vehicle_length
        agar spacing_error = 0 sejak langkah pertama.
        Bug sebelumnya: menggunakan spacing mentah (15m) bukan equilibrium gap (31m)
        sehingga PID menghasilkan perintah -8 m/s² yang melebihi A_CMD_MIN.
        Ref: Ploeg et al. (2011) d_i = r_i + h_i * v_i
        """
        with self._lock:
            cfg = dict(self.config)
            if custom_config: cfg.update(custom_config)
            # Batasi n_vehicles untuk mencegah alokasi berlebihan (FIX F-04)
            if n_vehicles is None: n_vehicles = max(2, cfg.get('num_vehicles', 4))
            n_vehicles = max(1, min(n_vehicles, 20))  # batas atas 20 kendaraan per platoon
            spacing   = cfg.get('initial_spacing', 15.0)
            speed     = cfg.get('desired_speed', 20.0)
            h         = cfg.get('time_headway', 0.8)
            new_pid   = self._next_pid

            # FIX B-02: equilibrium gap = r + h*v + vehicle_length (sama dengan _initialize_vehicles)
            eq_gap = spacing + h * speed + 5.0   # vehicle length 5m
            # Kecepatan awal platoon baru = 90% dari desired agar mengejar dari belakang
            init_speed = speed * 0.9

            # Posisi di belakang semua kendaraan yang ada
            if self.vehicles:
                rearmost = min(v.position for v in self.vehicles)
                leader_pos = rearmost - 100.0
            else:
                leader_pos = 500.0

            new_vehicles = []
            for i in range(n_vehicles):
                if self.mode == 'acc_only': cfg['tau_act'] = 0.35
                # FIX B-02: gunakan eq_gap bukan spacing mentah
                v = Vehicle(self._next_vid, leader_pos - i * eq_gap,
                            init_speed, (i==0), new_pid, cfg)
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

    # ── VALIDASI PRASYARAT ─────────────────────────────────────
    def validate_transfer(self, vehicle_id, target_platoon_id):
        """
        Cek kondisi prasyarat C1-C6 sebelum transfer.
        Ref: §4.2 Kondisi Prasyarat Transfer
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
        Ref: §6.3 Kondisi Prasyarat Swap
        """
        if platoon_id_a == platoon_id_b: return False, 'S1: Platoon harus berbeda'
        pvs_a = sorted([v for v in self.vehicles if v.platoon_id == platoon_id_a], key=lambda v: -v.position)
        pvs_b = sorted([v for v in self.vehicles if v.platoon_id == platoon_id_b], key=lambda v: -v.position)
        if not pvs_a: return False, f'S0: Platoon {platoon_id_a} tidak ditemukan'
        if not pvs_b: return False, f'S0: Platoon {platoon_id_b} tidak ditemukan'
        if len(pvs_a) < 2: return False, f'S2: Platoon {platoon_id_a} perlu ≥2 kendaraan'
        if len(pvs_b) < 2: return False, f'S2: Platoon {platoon_id_b} perlu ≥2 kendaraan'
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
            return False, f'S6: Beda kecepatan terlalu besar ({dv:.1f} m/s ≥ 5 m/s). Perlu sinkronisasi dulu.'
        # Cek busy
        if platoon_id_a in self._maneuver_queue._active_platoons:
            return False, f'S5: Platoon {platoon_id_a} sedang dalam manuver lain'
        if platoon_id_b in self._maneuver_queue._active_platoons:
            return False, f'S5: Platoon {platoon_id_b} sedang dalam manuver lain'
        return True, 'OK'

    def validate_promote(self, platoon_id):
        """
        Cek kondisi prasyarat Proposisi 5.1 sebelum promosi leader.
        Ref: §5.4 Kondisi Keamanan Promosi Leader
        """
        pvs = sorted([v for v in self.vehicles if v.platoon_id == platoon_id], key=lambda v: -v.position)
        if not pvs: return False, 'P0: Platoon tidak ditemukan'
        if len(pvs) < 2: return False, 'P1: Perlu minimal 2 kendaraan (|P| ≥ 2)'
        if pvs[0].transfer_cooldown > 0:
            return False, f'P2: Leader masih dalam cooldown ({pvs[0].transfer_cooldown:.1f}s)'
        if platoon_id in self._maneuver_queue._active_platoons:
            return False, 'P3: Platoon sedang dalam manuver lain'
        return True, 'OK'

    def get_maneuver_queue_status(self):
        """Kembalikan status ManeuverQueue untuk API."""
        return self._maneuver_queue.get_status()

    # ── SWAP LEADER ────────────────────────────────────────────
    def swap_leaders(self, platoon_id_a, platoon_id_b):
        """
        Tukar pemimpin (leader) antara dua platoon.
        Fase 1-5 sesuai §6.4. Validasi prasyarat S1-S6 (§6.3).
        Ref: Bergenhem et al. (2012), §6 Protokol Pertukaran Leader Antar-Platoon.
        """
        with self._lock:
            # Fase 1: Validasi prasyarat S1-S6
            ok, reason = self.validate_swap(platoon_id_a, platoon_id_b)
            if not ok:
                return {'success': False, 'error': reason}

            # Cek ManeuverQueue (Teorema 4.1 — Isolasi Transfer)
            if not self._maneuver_queue.try_acquire([platoon_id_a, platoon_id_b]):
                return {'success': False, 'error': 'Platoon sedang dalam manuver lain (queue busy)'}

            try:
                pvs_a = sorted([v for v in self.vehicles if v.platoon_id == platoon_id_a], key=lambda v: -v.position)
                pvs_b = sorted([v for v in self.vehicles if v.platoon_id == platoon_id_b], key=lambda v: -v.position)

                leader_a = pvs_a[0]
                leader_b = pvs_b[0]

                # Simpan posisi/kecepatan sebelum swap (Fase 3: Eksekusi Atomik §6.4.3)
                pos_a, vel_a = leader_a.position, leader_a.velocity
                pos_b, vel_b = leader_b.position, leader_b.velocity

                def make_leader(vehicle, platoon_id, position, velocity):
                    """Helper untuk set vehicle sebagai leader baru."""
                    vehicle.platoon_id    = platoon_id
                    vehicle.is_leader     = True
                    vehicle.position      = position
                    vehicle.velocity      = velocity
                    vehicle.control_mode  = 'LEADER'
                    vehicle.last_leader_data = None
                    vehicle.last_packet_time = None
                    vehicle.communication_ok = True
                    vehicle.integral_e    = 0.0
                    vehicle.spacing_error = 0.0
                    vehicle.a_desired     = 0.0
                    vehicle.aoi_running_peak = 0.0
                    vehicle.transfer_cooldown = 2.0   # Fase 5: Stabilisasi 2s (§6.4.5)
                    vehicle.fsm.reset()

                # Fase 3: Eksekusi atomik (window < 100ms — Eq. 24)
                make_leader(leader_a, platoon_id_b, pos_b, vel_b)
                make_leader(leader_b, platoon_id_a, pos_a, vel_a)

                # Fase 4: Pembaruan referensi member (§6.4.4)
                # (terjadi secara implisit saat step() meresort kendaraan per platoon)

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
        4 Fase sesuai §5.2. Validasi Proposisi 5.1 (§5.4).
        Ref: Ploeg et al. (2011) CACC leader election, §5 Protokol Promosi Leader.
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
                new_leader = pvs[1]   # Kendaraan ke-2 (kandidat leader baru — deterministik, §5.2.2)
                tail = pvs[-1]        # Kendaraan paling belakang

                # FIX-DEMOTE-1: Posisi baru old_leader menggunakan CACC reference gap
                # d_ref = desired_gap + time_headway * velocity agar spacing_error = 0
                cacc_ref_gap_demote = old_leader.desired_gap + old_leader.time_headway * tail.velocity
                demote_pos = tail.position - tail.length - cacc_ref_gap_demote

                # Fase 3a: Naikkan pangkat vehicle[1] (§5.2.3)
                new_leader.is_leader     = True
                new_leader.control_mode  = 'LEADER'
                new_leader.spacing_error = 0.0
                new_leader.integral_e    = 0.0
                new_leader.last_leader_data = None
                new_leader.last_packet_time = None
                new_leader.aoi_running_peak = 0.0
                new_leader.transfer_cooldown = 2.0   # Fase 4: Stabilisasi 2s (§5.2.4)
                new_leader.fsm.reset()

                # Fase 3b: Turunkan pangkat old_leader ke ekor (§5.2.3)
                old_leader.is_leader     = False
                old_leader.control_mode  = 'CACC'
                old_leader.position      = demote_pos
                old_leader.velocity      = tail.velocity
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
        4 Fase sesuai §4.3. Validasi prasyarat C1-C6 (§4.2).
        Ref: §4 Protokol Transfer Anggota Antar-Platoon.
        """
        with self._lock:
            # Fase 1: Validasi prasyarat C1-C6 (§4.2)
            ok, reason = self.validate_transfer(vehicle_id, target_platoon_id)
            if not ok:
                return {'success': False, 'error': reason}

            vehicle = next((v for v in self.vehicles if v.id == vehicle_id), None)
            src_pid = vehicle.platoon_id

            # ManeuverQueue — cek isolasi (Teorema 4.1)
            if not self._maneuver_queue.try_acquire([src_pid, target_platoon_id]):
                return {'success': False, 'error': 'Platoon sedang dalam manuver lain (Teorema 4.1)'}

            try:
                target_vehicles = [v for v in self.vehicles if v.platoon_id == target_platoon_id]

                # Fase 2: Pemisahan dari platoon sumber
                # Cari kendaraan di belakang vehicle dalam platoon sumber untuk reset integralnya
                src_pvs = sorted([v for v in self.vehicles if v.platoon_id == src_pid], key=lambda v: -v.position)
                veh_idx = next((i for i, v in enumerate(src_pvs) if v.id == vehicle_id), None)

                # ═══════════════════════════════════════════════════════════════
                # ROOT CAUSE FIX (FINAL): Bersihkan packet buffer semua followers
                # di belakang vehicle pada step YANG SAMA saat DEPARTING dimulai.
                #
                # Mengapa: dt=0.01s, network delay=10ms → paket dari V1 ke V2 yang
                # dikirim di step t-1 akan SIAP DIBACA di step t (saat DEPARTING dimulai).
                # DEPART-CORE mengeksklusikan V1 dari loop platoon (tidak kirim paket baru),
                # tapi V2's packet_buffer masih berisi paket LAMA dari V1.
                # V2 lalu memproses paket itu: gap(V1→V2)≈13m, d_ref=31m → e_gap=-18m
                # PID output = 0.5×(-18) = -9 m/s² → EMERGENCY cascade!
                #
                # FIX: Clear buffer + seed last_leader_data dengan predecessor yang BENAR
                # (yaitu kendaraan yang sama — V1 masih di platoon saat DEPARTING, jadi
                # V2 tetap tracking V1. Seed mencegah communication_ok=False sementara).
                # ═══════════════════════════════════════════════════════════════
                if veh_idx is not None:
                    for j in range(veh_idx + 1, len(src_pvs)):
                        sv = src_pvs[j]
                        sv.integral_e    = 0.0   # Reset integral mencegah windup
                        sv.spacing_error = 0.0
                        sv.a_actual      = 0.0   # tidak ada momentum pengereman
                        sv.a_desired     = 0.0
                        sv.acceleration  = 0.0
                        sv.fsm.reset()           # reset FSM ke CACC (AoI=0)
                        # P-02: grace period agar tidak langsung disconnect
                        sv._no_data_grace = 0.10    # 100 ms window saat transfer
                        # P-03: stabilization window untuk toleransi EMERGENCY lebih besar
                        sv.stabilization_window = 0.15  # 150 ms
                        # BUG-FIX DISCONNECT: Seed last_leader_data dengan predecessor
                        # yang TETAP sama (kendaraan di depan sv, yaitu src_pvs[j-1]).
                        # Ini mencegah sv memanggil update_without_data (communication_ok=False).
                        # V1 (vehicle) masih di platoon saat DEPARTING sehingga tetap jadi predecessor sv.
                        pred = src_pvs[j - 1]  # predecessor langsung sv setelah v tetap di posisi
                        sv.last_leader_data = {
                            'position':     pred.position,
                            'velocity':     pred.velocity,
                            'acceleration': pred.acceleration,
                            'timestamp':    self.time_elapsed,
                            'fsm_state':    pred.fsm.state,   # P-04: sertakan FSM state
                        }
                        sv.last_packet_time = self.time_elapsed
                        sv.fsm.packet_received(self.time_elapsed)
                        sv.communication_ok = True
                        # Bersihkan buffer paket stale dari predecessor (akan diisi ulang saat DEPARTING loop)
                        self.network.clear_buffer(sv.id)
                        # ROOT CAUSE FIX DISCONNECT: inject paket instan dari predecessor
                        # sehingga ready != [] dan update_follower (bukan update_without_data)
                        # yang dipanggil -> communication_ok tetap True tanpa jeda 1 step.
                        self._inject_immediate_packet(sv, pred)

                # Fase 3: Perpindahan dan penggabungan (§4.3.3)
                # Cari kendaraan paling belakang di platoon tujuan
                tail = min(target_vehicles, key=lambda v: v.position)

                # FIX-JOIN-1: Posisi dihitung sesuai CACC reference gap agar spacing_error = 0
                # d_ref = desired_gap (r) + time_headway * tail.velocity
                # sehingga kendaraan langsung berada di posisi formasi yang benar
                cacc_ref_gap = vehicle.desired_gap + vehicle.time_headway * tail.velocity
                new_position = tail.position - tail.length - cacc_ref_gap
                new_velocity = tail.velocity

                # ── FASE 2: Mulai DEPARTING (§4.3.2) ───────────────────────────────
                # v7: Tidak lagi teleport instan. Kendaraan melewati 3 fase:
                #   DEPARTING (1.5s) → IN_TRANSIT (fisika ACC) → STABILIZING (2s)
                # Selama DEPARTING, kendaraan masih di platoon sumber tapi headway
                # meningkat 1.5x (transfer_cooldown > 0 memicu HEADWAY_MULT['TRANSFER'])
                DEPART_DURATION = 1.5   # §4.3.2: waktu pemisahan dari platoon sumber

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
                # Reset integral agar tidak ada windup saat headway berubah (§8.6 Prop 8.1)
                vehicle.integral_e = 0.0

                # FIX-JOIN-2: Hapus paket lama agar tidak dibaca dengan konteks platoon lama
                self.network.clear_buffer(vehicle_id)

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
                # v7: JANGAN release queue di sini — akan di-release saat join selesai
                # di dalam step() → update_in_transit() → joined condition
                self._maneuver_queue.log_maneuver('transfer', [src_pid, target_platoon_id], True)
                return result
            except Exception as exc:
                # Kalau ada error sebelum DEPARTING dimulai, release queue
                self._maneuver_queue.release([src_pid, target_platoon_id])
                return {'success': False, 'error': str(exc)}
            # TIDAK ada finally release di sini (finally dihapus untuk kasus sukses DEPARTING)

    def step(self):
        if not self.running: return None
        self.time_elapsed += self.dt
        self.network.update(self.dt)
        platoons = {}
        for v in self.vehicles: platoons.setdefault(v.platoon_id,[]).append(v)

        profile = self.config.get('leader_profile', 'normal')

        # ── v7: MULTI-PHASE TRANSFER MANAGEMENT ──────────────────────────────────
        # Kelola transisi DEPARTING → IN_TRANSIT → join platoon tujuan (§4.3)
        # Dilakukan SEBELUM platoon loop agar kendaraan IN_TRANSIT tidak di-update
        # sebagai leader/follower pada platoon manapun.
        for v in list(self.vehicles):
            if v.maneuver_phase == 'DEPARTING':
                # Fase 2 (§4.3.2): Kendaraan masih di platoon sumber.
                # transfer_cooldown=999 → headway 1.5x aktif → gap meningkat alami.
                v.transit_data['depart_elapsed'] = v.transit_data.get('depart_elapsed', 0.0) + self.dt

                # ── ABORT CHECK: target platoon hilang saat DEPARTING ──────────
                target_pid_check = v.transit_data.get('target_pid')
                target_alive = any(x.platoon_id == target_pid_check and not x.collided
                                   for x in self.vehicles)
                if not target_alive:
                    # Batalkan transfer — kembalikan ke platoon sumber
                    self._maneuver_queue.release([v.transit_data.get('src_pid', -1), target_pid_check])
                    v.maneuver_phase    = None
                    v.transfer_cooldown = 1.0
                    v.transit_data      = {}
                    v.a_actual          = 0.0
                    v.a_desired         = 0.0
                    v.acceleration      = 0.0
                    v.integral_e        = 0.0
                    socketio.emit('transfer_aborted', {'vehicle_id': v.id, 'reason': 'target_platoon_gone'})
                    continue

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
                    #   2. V1 brakes hard (-4 m/s²) via ACC control
                    #   3. V2/V3 ikut brake (-4 m/s²) untuk maintain gap ke V1
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
                            sv.integral_e    = 0.0     # reset integral (Algorithm 1, §4.5)
                            sv.spacing_error = 0.0
                            # FIX-PRED-1a: reset percepatan agar FSM tidak trigger EMERGENCY
                            sv.a_actual  = 0.0
                            sv.a_desired = 0.0
                            sv.acceleration = 0.0
                            # FIX-PRED-1b: reset FSM ke CACC (bersihkan AoI counter)
                            sv.fsm.reset()
                            # FIX-PRED-1c: reset transfer_cooldown agar tidak masuk TRANSFER branch FSM.
                            sv.transfer_cooldown = 0.0
                            # bersihkan buffer paket stale dari predecessor lama
                            self.network.clear_buffer(sv.id)
                            # BUG-FIX DISCONNECT: seed last_leader_data dengan predecessor baru
                            # (kendaraan tepat di depan sv dalam src_pvs setelah v dikeluarkan).
                            # Ini mencegah sv memanggil update_without_data (communication_ok=False)
                            # selama jeda network delay sebelum paket pertama dari predecessor baru tiba.
                            sv_idx = next((k for k, x in enumerate(src_pvs) if x.id == sv.id), None)
                            if sv_idx is not None and sv_idx > 0:
                                new_pred = src_pvs[sv_idx - 1]
                                sv.last_leader_data = {
                                    'position':     new_pred.position,
                                    'velocity':     new_pred.velocity,
                                    'acceleration': new_pred.acceleration,
                                    'timestamp':    self.time_elapsed,
                                    'fsm_state':    new_pred.fsm.state,   # P-04
                                }
                                sv.last_packet_time = self.time_elapsed
                                sv.fsm.packet_received(self.time_elapsed)
                                sv.communication_ok = True
                                sv._no_data_grace = 0.10   # P-02: grace period setelah predecessor berubah
                            else:
                                sv.last_packet_time  = None
                                sv.last_leader_data  = None
                            # TIDAK beri transfer_cooldown: cooldown memicu TRANSFER
                            # branch FSM yang cek a_actual < EMERGENCY_ACCEL lebih ketat
                            # ROOT CAUSE FIX DISCONNECT: inject paket instan dari predecessor baru
                            # sehingga ready != [] -> update_follower -> communication_ok = True.
                            if sv_idx is not None and sv_idx > 0:
                                self._inject_immediate_packet(sv, src_pvs[sv_idx - 1])

                    # Pindahkan ke platoon sementara -1 (limbo) supaya tidak
                    # diproses dalam loop platoon normal
                    v.platoon_id    = -1
                    v.maneuver_phase = 'IN_TRANSIT'
                    v.control_mode   = 'ACC'
                    v.integral_e     = 0.0
                    v.spacing_error  = 0.0
                    v.last_leader_data = None
                    v.last_packet_time = None
                    self.network.clear_buffer(v.id)

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
                # Fase 3 (§4.3.3): Kendaraan bergerak mandiri menuju ekor platoon tujuan.
                target_pid    = v.transit_data['target_pid']
                target_vehicles = [x for x in self.vehicles
                                   if x.platoon_id == target_pid and not x.collided]

                if not target_vehicles:
                    # Platoon tujuan menghilang — abort, kembali ke platoon sumber
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
                    # ── Fase 4: Bergabung ke platoon tujuan (§4.3.3 – §4.3.4) ──
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
                    v.transfer_cooldown = 2.0   # Fase 4: Stabilisasi 2s (§4.3.4)
                    # FIX-JOIN-3: seed last_leader_data agar tidak menunggu delay jaringan
                    v.last_leader_data = {
                        'position': tail.position, 'velocity': tail.velocity,
                        'acceleration': tail.acceleration, 'timestamp': self.time_elapsed,
                        'fsm_state': tail.fsm.state,   # P-04
                    }
                    v.last_packet_time = self.time_elapsed
                    v._no_data_grace   = 0.10   # P-02: grace period setelah join
                    v.fsm.packet_received(self.time_elapsed)
                    self.network.clear_buffer(v.id)
                    # FIX B-01: inject paket instan dari tail setelah join
                    # agar step berikutnya tidak masuk update_without_data
                    self._inject_immediate_packet(v, tail)
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
        # ── akhir multi-phase transfer management ─────────────────────────────────

        # Rebuild platoons dict SETELAH multi-phase handling (platoon_id bisa berubah)
        # Skip IN_TRANSIT (diupdate mandiri) DAN DEPARTING (diupdate di bawah sendiri).
        # FIX-DEPART-CORE: Kendaraan DEPARTING tidak masuk loop platoon normal.
        # Alasan: saat DEPARTING, transfer_cooldown=999 → headway 1.5x →
        # PID menghasilkan a_desired sangat negatif → V1 braking keras →
        # V2/V3 di belakang menutup gap → EMERGENCY cascade (root cause sesungguhnya).
        # Solusi: kendaraan DEPARTING cukup mempertahankan kecepatan saat ini (cruise).
        # Gap dengan V0 terbuka alami karena V0 terus maju.
        # FIX-DEPART-FINAL: Kendaraan DEPARTING TETAP di platoon loop sebagai predecessor V2.
        # ROOT CAUSE sebelumnya SALAH: mengekslusikan V1 membuat V2 lompat ke V0 sebagai
        # predecessor (gap GANDA), V2 akselerasi keras, gap ke V0 < 12m → EMERGENCY!
        # SOLUSI BENAR: V1 tetap di loop agar V2 tetap melihat V1 sebagai predecessor
        # (gap normal d_ref). Tapi V1 di-HANDLE KHUSUS dalam loop: a_desired=0 (cruise).
        # Packet dari V1 ke V2 berisi a=0 → tidak ada cascade feedforward.
        platoons = {}
        for v in self.vehicles:
            if v.maneuver_phase == 'IN_TRANSIT':
                continue  # hanya skip IN_TRANSIT, bukan DEPARTING!
            platoons.setdefault(v.platoon_id, []).append(v)

        for pid, pvs in platoons.items():
            pvs.sort(key=lambda v: -v.position)
            leader = pvs[0]
            for i, vehicle in enumerate(pvs):
                if vehicle.collided:
                    vehicle.velocity = 0; vehicle.acceleration = 0; continue
                # FIX-DEPART-INLOOP: kendaraan DEPARTING cruise in-place.
                # Tidak dipanggil update_follower/leader → tidak ada PID spike.
                # Ia tetap di loop sebagai predecessor V2, paket dikirim dari posisinya.
                # a=0 berarti packet yang dikirim ke V2 berisi a=0 → tidak ada cascade FF.
                if vehicle.maneuver_phase == 'DEPARTING':
                    vehicle.a_desired    = 0.0
                    vehicle.a_actual     = 0.0
                    vehicle.acceleration = 0.0
                    vehicle.velocity     = max(0.0, min(vehicle.max_velocity, vehicle.velocity))
                    vehicle.position    += vehicle.velocity * self.dt
                    if vehicle.transfer_cooldown > 0:
                        vehicle.transfer_cooldown -= self.dt
                    continue  # skip normal update, TAPI tetap di loop sebagai predecessor
                if i == 0:
                    vehicle.update_leader(self.config.get('desired_speed',20.0), self.dt, profile)
                elif i > 0:
                    preceding = pvs[i-1]
                    num_blockers = i - 1
                    packet = {'position': preceding.position, 'velocity': preceding.velocity,
                              'acceleration': preceding.acceleration, 'timestamp': self.time_elapsed,
                              'fsm_state': preceding.fsm.state}   # P-04: expose FSM state untuk cap feedforward
                    leader_packet = {'position': leader.position, 'velocity': leader.velocity,
                                     'acceleration': leader.acceleration}
                    if self.mode == 'acc_only':
                        vehicle.update_without_data(self.dt, self.time_elapsed)
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
                            vehicle.update_follower(pkt, self.time_elapsed, self.dt, lp_direct, net_delay_s)
                        else:
                            vehicle.update_without_data(self.dt, self.time_elapsed)
                        if vehicle.control_mode != prev_mode:
                            self.mode_switch_log.append({'time':self.time_elapsed,'vehicle_id':vehicle.id,'from':prev_mode,'to':vehicle.control_mode,'reason':'FSM/AoI'})
                    if not vehicle.is_leader:
                        self.aoi_all_samples.append(vehicle.fsm.aoi * 1000)

        # Emergency leader promotion check (Algorithm 2, §5.5)
        self._check_emergency_promotion()

        self._check_collisions()
        si, ss = self._calculate_stability()
        ar     = self._amplitude_ratios()
        spacing_errors_raw = {v.id: round(v.spacing_error,3) for v in self.vehicles if not v.is_leader}
        entry = {
            'time': self.time_elapsed,
            'vehicles': [v.to_dict() for v in self.vehicles],
            'network':  self.network.get_metrics(),
            'stability_index': si, 'stability_status': ss,
            'amplitude_ratios': ar,
            'collision_occurred': self.collision_occurred,
            'max_aoi_ms': self._max_aoi_ms(),
            'spacing_errors': spacing_errors_raw,
            'mode': self.mode,
            'num_platoons': self._next_pid,
            'maneuver_queue': self._maneuver_queue.get_status(),
        }
        self.data_log.append(entry)
        return entry

    def _check_emergency_promotion(self):
        """
        Algorithm 2 (§5.5): Promosi darurat jika V1 tidak terima CAM leader ≥ 500ms.
        Hanya aktif saat tidak ada manuver berjalan dan platoon ≥ 2 kendaraan.
        """
        platoons = {}
        for v in self.vehicles: platoons.setdefault(v.platoon_id, []).append(v)
        for pid, pvs in platoons.items():
            pvs.sort(key=lambda v: -v.position)
            if len(pvs) < 2: continue
            leader = pvs[0]
            second = pvs[1]
            if leader.collided: continue
            if pid in self._maneuver_queue._active_platoons: continue
            # Cek AoI V1 (kendaraan kedua) terhadap CAM leader
            if second.fsm.aoi >= self._emergency_promo_threshold:
                # Emergency promotion: V1 ambil alih (§5.5 Algorithm 2)
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
                        leader.position = demote_pos
                        leader.velocity = tail.velocity
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
        """String stability: AR_i = |e_i|/|e_{i-1}| < 1. Ref: Naus et al. (2010)"""
        followers = [v for v in self.vehicles if not v.is_leader and not v.collided]
        if len(followers) < 2: return {}
        errors = [abs(v.spacing_error) for v in followers]
        ratios = {}
        for idx, v in enumerate(followers):
            if idx == 0: ratios[v.id] = None
            else:
                e_i, e_im1 = errors[idx], errors[idx-1]
                ratios[v.id] = round(e_i/e_im1, 3) if e_im1 > 1e-4 else None
        return ratios

    def _calculate_stability(self):
        followers = [v for v in self.vehicles if not v.is_leader and not v.collided]
        if len(followers) < 2: return 1.0, 'STABLE'
        errors = [abs(v.spacing_error) for v in followers]
        if max(errors) == 0: return 1.0, 'STABLE'
        amp = max(errors) / max(min(errors), 0.1)
        idx = max(0.0, min(1.0, 1.0 - (amp-1.0)/5.0))
        return round(idx,3), ('STABLE' if idx>0.8 else 'MARGINAL' if idx>0.5 else 'UNSTABLE')

    def _check_collisions(self):
        """
        Deteksi tabrakan fisik berdasarkan posisi longitudinal semua kendaraan.

        FIX B-04: Urutkan semua kendaraan berdasarkan posisi fisik (descending)
        sebelum memeriksa pasangan berturutan.
        Bug sebelumnya: self.vehicles tidak terurut → kendaraan yang secara fisik
        berdekatan bisa berada di indeks berjauhan → tidak pernah diperiksa.
        Selain itu, kondisi `l.platoon_id != f.platoon_id` melewatkan tabrakan
        lintas platoon (misalnya kendaraan IN_TRANSIT yang menerobos barisan).

        Ref: GCDC (2016) Collision definition: physical gap ≤ 0 atau gap < 1m
        dengan kecepatan relatif mendekat > 1 m/s.
        """
        # Urutkan SEMUA kendaraan berdasarkan posisi descending (terdepan ke terbelakang)
        # Kendaraan IN_TRANSIT (platoon_id=-1) juga disertakan
        sorted_vehicles = sorted(
            [v for v in self.vehicles if not v.collided],
            key=lambda v: -v.position
        )

        for i in range(len(sorted_vehicles) - 1):
            l = sorted_vehicles[i]    # kendaraan lebih depan
            f = sorted_vehicles[i+1]  # kendaraan tepat di belakang
            if l.collided or f.collided:
                continue
            gap = l.position - f.position - l.length
            rv  = f.velocity - l.velocity   # positif = kendaraan belakang mendekati depan
            cause = None
            if gap <= 0:
                cause = f"Physical overlap (gap={gap:.2f}m)"
            elif gap < self.config.get('min_safe_spacing', 1.0) and rv > 1.0:
                cause = f"Imminent collision (gap={gap:.2f}m, rv={rv:.2f}m/s)"
            if cause:
                l.collided = f.collided = True
                self.collision_occurred = True
                self.collision_log.append({
                    'time': self.time_elapsed,
                    'vehicles': [l.id, f.id],
                    'platoon_l': l.platoon_id, 'platoon_f': f.platoon_id,
                    'cross_platoon': l.platoon_id != f.platoon_id,
                    'position': (l.position + f.position) / 2,
                    'gap': gap,
                    'relative_velocity': rv,
                    'cause': cause,
                    'network_delay': self.network.get_current_delay(),
                    'packet_loss': self.network.packet_loss * 100
                })

    def inject_disturbance(self, t):
        if t == 'leader_brake':
            for v in self.vehicles:
                if v.is_leader and not v.collided: v.a_desired = -6.0
        elif t == 'acceleration_spike':
            for v in self.vehicles:
                if v.is_leader and not v.collided: v.a_desired = 3.0
        elif t == 'network_degradation': self.network.inject_degradation(5.0)
        elif t == 'rsu_offline': self.network.rsu_enabled = False

    def get_state(self):
        si, ss = self._calculate_stability()
        return {'time': round(self.time_elapsed,2),
                'vehicles': [v.to_dict() for v in self.vehicles],
                'network':  self.network.get_metrics(),
                'stability_index': round(si,3), 'stability_status': ss,
                'collision_occurred': self.collision_occurred,
                'amplitude_ratios': self._amplitude_ratios(),
                'max_aoi_ms': self._max_aoi_ms(),
                'mode': self.mode,
                'num_platoons': self._next_pid}

    def get_aoi_distribution(self):
        """
        G-05: Statistik AoI vs M/D/1 teoritis (Kaul et al. 2012).

        FIX B-03: Gunakan λ dan S aktual dari metrik simulasi.
        Bug sebelumnya: lam=10.0 dan S=1/lam di-hardcode sehingga
        rho = lam*S = 1.0 SELALU → kondisi antrian tidak stabil →
        theoretical_peak = 999 ms SELALU (tidak bermakna akademis).

        Formula M/D/1 peak AoI — Kaul et al. (2012) Eq.(7):
          Δ_peak = 1/λ + S/2 + ρ / (2λ(1−ρ))
        dengan λ = laju kedatangan aktual dan S = rata-rata waktu layanan.
        """
        if not self.aoi_all_samples: return {}
        s = self.aoi_all_samples
        mean = sum(s)/len(s)
        variance = sum((x-mean)**2 for x in s)/len(s)
        std = variance**0.5
        s_sorted = sorted(s)
        p95 = s_sorted[int(0.95*len(s_sorted))]
        peak = max(s)

        # FIX B-03: Hitung λ dan S dari metrik simulasi yang sebenarnya
        net_metrics = self.network.get_metrics()
        if self.time_elapsed > 0 and self.network.packets_sent > 0:
            lam = self.network.packets_sent / self.time_elapsed  # paket/detik aktual
        else:
            lam = 1.0  # fallback aman

        avg_delay_ms = net_metrics.get('avg_delay', 10.0)
        if avg_delay_ms > 0:
            S = avg_delay_ms / 1000.0   # konversi ms → detik
        else:
            S = 0.01   # fallback: 10ms

        rho = lam * S

        if rho < 1.0:
            # Formula M/D/1 peak AoI — Kaul et al. (2012)
            theoretical_peak = (1.0/lam + S/2.0 + rho / (2.0*lam*(1.0 - rho))) * 1000.0
        else:
            # Antrian jenuh — theoretical peak tidak terdefinisi (tak hingga)
            theoretical_peak = float('inf')

        return {
            'mean_ms': round(mean,1), 'std_ms': round(std,1),
            'p95_ms': round(p95,1), 'peak_ms': round(peak,1),
            'theoretical_peak_ms': round(theoretical_peak,1) if theoretical_peak != float('inf') else None,
            'n_samples': len(s),
            # Expose parameter aktual untuk transparansi akademis
            'lambda_pps': round(lam, 2),
            'service_time_ms': round(S*1000, 2),
            'rho': round(rho, 4),
            'queue_saturated': rho >= 1.0,
        }

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
    global simulation_engine
    while True:
        with engine_lock:
            if simulation_engine and simulation_engine.running:
                try:
                    state = simulation_engine.step()
                    if state: socketio.emit('state_update', state)
                    if simulation_engine.collision_occurred and simulation_engine.collision_log:
                        socketio.emit('collision_detected',
                            {'collision': simulation_engine.collision_log[-1],
                             'time': simulation_engine.time_elapsed})
                except Exception as e:
                    print(f"Sim error: {e}"); traceback.print_exc()
        socketio.sleep(0.01)

def compare_loop():
    global compare_engines
    while True:
        with compare_lock:
            cacc_eng = compare_engines.get('cacc')
            acc_eng  = compare_engines.get('acc')
            if cacc_eng and cacc_eng.running and acc_eng and acc_eng.running:
                try:
                    s_cacc = cacc_eng.step()
                    s_acc  = acc_eng.step()
                    if s_cacc and s_acc:
                        socketio.emit('compare_update', {'cacc': s_cacc, 'acc': s_acc})
                    if cacc_eng.collision_occurred or acc_eng.collision_occurred:
                        socketio.emit('compare_collision', {
                            'cacc_collision': cacc_eng.collision_occurred,
                            'acc_collision':  acc_eng.collision_occurred})
                except Exception as e:
                    print(f"Compare error: {e}"); traceback.print_exc()
        socketio.sleep(0.01)

def login_required(f):
    @wraps(f)
    def dec(*a, **kw):
        if 'nim' not in session: return redirect(url_for('login_page'))
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
    d = request.get_json()
    user = verify_user(d.get('username','').strip(), d.get('password','').strip())
    if user:
        session['nim'] = user['nim']; session['name'] = user['name']
        return jsonify({'success':True,'redirect':'/dashboard'})
    return jsonify({'success':False,'message':'Invalid credentials'}), 401

@app.route('/logout')
def logout(): session.clear(); return redirect(url_for('login_page'))

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
    return render_template('history.html', history=get_user_history(session.get('nim')), user=session.get('name'))

@app.route('/api/history')
@login_required
def get_history():
    history = get_user_history(session.get('nim'))
    return jsonify({'history':[{'id':h['id'],'date':h['timestamp'],
        'experiment_id':h['experiment_id'],'num_vehicles':h['num_vehicles']*h['num_platoons'],
        'latency':h['latency_ms'],'packet_loss':h['packet_loss'],
        'collision':bool(h['collision_occurred']),'duration':h['duration_seconds']
    } for h in history]})

@app.route('/history_detail/<int:sim_id>')
@login_required
def history_detail(sim_id):
    sim = get_simulation_detail(sim_id, session.get('nim'))
    if not sim: return "Not found", 404
    sim['config'] = json.loads(sim['config_json'])
    sim['results'] = json.loads(sim['results_json'])
    return render_template('history_detail.html', simulation=sim, detail=sim, user=session.get('name'))

@app.route('/api/current_user')
@login_required
def current_user(): return jsonify({'username':session.get('nim'),'name':session.get('name')})

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
@login_required  # FIX S-01: endpoint kritis wajib autentikasi
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
@login_required  # FIX S-01: endpoint kritis wajib autentikasi
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
def handle_connect(): emit('connection_status',{'status':'connected'})

@socketio.on('disconnect')
def handle_disconnect(): pass

@socketio.on('start_simulation')
def handle_start(data):
    global simulation_engine
    with engine_lock:
        simulation_engine = SimulationEngine(data.get('config'), mode='cacc')
        simulation_engine.running = True
        emit('simulation_started',{'status':'running','experiment_id':simulation_engine.experiment_id})

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
                nim = session.get('nim')
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
        cfg = data.get('config', {})
        cfg_acc = dict(cfg); cfg_acc['latency_ms'] = 1000
        compare_engines['cacc'] = SimulationEngine(cfg, mode='cacc')
        compare_engines['acc']  = SimulationEngine(cfg_acc, mode='acc_only')
        compare_engines['cacc'].running = True
        compare_engines['acc'].running  = True
        emit('compare_started', {'status':'running'})

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
    print("V2V 5G PLATOONING SIMULATOR — RESEARCH BUILD v5.0")
    print("NEW-01: Dynamic platoon addition via API + Socket")
    print("NEW-02: Vehicle transfer between platoons A->B via API + Socket")
    print("NEW-04: Swap leaders between platoons via API + Socket")
    print("NEW-05: Promote next leader within platoon via API + Socket")
    print("VIS-01: Multi-lane road — each platoon in its own lane")
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
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, use_reloader=False)
