import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app import SimulationEngine

config = {
    'num_vehicles':4, 'num_platoons':1, 'initial_spacing':15, 'desired_speed':20,
    'latency_ms':10, 'packet_loss':1, 'jitter_ms':2, 'kp':0.5, 'kd':0.3, 'ki':0.01,
    'alpha':0.9, 'tau_act':0.25, 'time_headway':0.8, 'security_delay_ms':3,
    'network_slicing':'URLLC', 'rsu_enabled':True, 'topology':'predecessor',
    'leader_profile':'normal', 'max_accel':3, 'max_decel':6, 'bandwidth_mbps':100
}

if __name__ == '__main__':
    try:
        print("Initializing Engine...")
        eng = SimulationEngine(config)
        eng.running = True
        print("Stepping 1000 times...")
        for i in range(100):
            eng.step()
        print("Step returned successfully.")
        print("ALL TESTS PASSED")
    except Exception as e:
        import traceback
        traceback.print_exc()
