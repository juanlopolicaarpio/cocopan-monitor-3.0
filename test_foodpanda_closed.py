#!/usr/bin/env python3
"""
Foodpanda CLOSED-only smoke test for CocoPan monitor

- Uses your real Foodpanda checker: StealthStoreMonitor.check_store_stealth
- No DB, no scheduler; fast mode by default (short sleeps, no long anti-detection delays)
- Prints results and writes CSV: fp_closed_results_YYYYMMDD_HHMMSS.csv

Run:
  python test_foodpanda_closed.py            # fast mode (default)
  python test_foodpanda_closed.py --real     # keep full anti-detection delays/behavior
  python test_foodpanda_closed.py --sleep 1  # tweak pacing in fast mode
"""

import sys, csv, time, argparse, random
from datetime import datetime

# --- Import your monitor module without kicking off its main() ---
try:
    import monitor_service as mon
except Exception as e:
    print("❌ Can't import monitor_service.py. Place this file beside it.")
    print("Error:", e)
    sys.exit(1)

def build_light_monitor(fast_mode: bool = True):
    """Create a StealthStoreMonitor instance WITHOUT running its __init__."""
    monitor = object.__new__(mon.StealthStoreMonitor)  # bypass __init__
    monitor.anti_detection = mon.AntiDetectionManager()
    monitor.store_names = {}
    monitor.stats = {}
    if fast_mode:
        # Cut long anti-detection delays for quick testing
        monitor.anti_detection.get_smart_delay = lambda: random.uniform(0.4, 0.9)
        monitor.anti_detection.should_visit_main_site = lambda: False
    return monitor

# --- Foodpanda pages that exist and commonly show "closed"/"is closed until..." ---
FOODPANDA_CLOSED = [
    {"name": "Master Siomai – España Boulevard (Manila)",
     "url": "https://www.foodpanda.ph/restaurant/u0ak/master-siomai-espana-boulevard-u0ak"},
    {"name": "Goldilocks – Pritil Tondo (Manila)",
     "url": "https://www.foodpanda.ph/restaurant/s5ol/goldilocks-pritil-tondo"},
    {"name": "Max’s Restaurant – Ermita Orosa (Manila)",
     "url": "https://www.foodpanda.ph/restaurant/q1uq/maxs-restaurant-ermita-orosa"},
    {"name": "Baliwag Lechon Manok at Liempo – G. Tuazon Street (Manila)",
     "url": "https://www.foodpanda.ph/restaurant/u0ba/baliwag-lechon-manok-at-liempo-g-tuazon-street"},
    {"name": "Dunkin’ – Zen Towers (Manila)",
     "url": "https://www.foodpanda.ph/restaurant/q4zk/dunkin-zen-towers"},
    {"name": "Goldilocks – G. Tuazon (Manila)",
     "url": "https://www.foodpanda.ph/restaurant/s1ry/goldilocks-g-tuazon"},
    {"name": "Cmoore Takoyaki – Lapu-Lapu City (Cebu)",
     "url": "https://www.foodpanda.ph/restaurant/srf0/cmoore-takoyaki"},
    {"name": "Teriyaki Boy – Sta. Cruz (Laguna)",
     "url": "https://www.foodpanda.ph/restaurant/va1q/teriyaki-boy-sta-cruz-laguna"},
    {"name": "Cafe de Flor – Cauayan (Isabela)",
     "url": "https://www.foodpanda.ph/restaurant/kn3q/cafe-de-flor-cauayan"},
    {"name": "Cindy’s Bakery – Tuguegarao",
     "url": "https://www.foodpanda.ph/restaurant/mz41/cindys-bakery-tuguegarao"},
]

def run_test(fast=True, sleep_between=0.5):
    monitor = build_light_monitor(fast_mode=fast)
    rows = []

    print("=" * 100)
    print(f"🧪 Foodpanda CLOSED-only smoke test | fast={fast} | total={len(FOODPANDA_CLOSED)}")
    print("=" * 100)

    for i, s in enumerate(FOODPANDA_CLOSED, 1):
        name, url = s["name"], s["url"]
        print(f"[{i:02d}/{len(FOODPANDA_CLOSED)}] FOODPANDA  {name}")
        try:
            res = monitor.check_store_stealth(url)
            emoji = {
                mon.StoreStatus.ONLINE: "🟢",
                mon.StoreStatus.OFFLINE: "🔴",
                mon.StoreStatus.BLOCKED: "🚫",
                mon.StoreStatus.ERROR: "⚠️",
                mon.StoreStatus.UNKNOWN: "❓",
            }[res.status]
            print(f"    -> {emoji} {res.status.value.upper():7} {res.response_time}ms  {res.message or ''}")
            rows.append({
                "platform": "foodpanda",
                "name": name,
                "url": url,
                "status": res.status.value,
                "response_time_ms": res.response_time,
                "message": res.message or "",
                "confidence": res.confidence,
            })
        except Exception as e:
            print(f"    -> ⚠️ RUNNER ERROR: {e}")
            rows.append({
                "platform": "foodpanda",
                "name": name,
                "url": url,
                "status": "error",
                "response_time_ms": 0,
                "message": f"runner error: {e}",
                "confidence": 0.0,
            })

        # pacing
        if fast:
            time.sleep(sleep_between)
        else:
            time.sleep(monitor.anti_detection.get_smart_delay())

    # summary
    from collections import Counter
    counts = Counter([r["status"] for r in rows])
    print("\n" + "-" * 100)
    print("Summary:")
    for k in ["online", "offline", "blocked", "error", "unknown"]:
        if k in counts:
            print(f"  {k:7}: {counts[k]}")
    print("-" * 100)

    # CSV
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = f"fp_closed_results_{ts}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"📄 Saved: {out}")

def main():
    ap = argparse.ArgumentParser(description="Run Foodpanda CLOSED-only smoke test using CocoPan monitor logic.")
    ap.add_argument("--real", action="store_true",
                    help="Use full anti-detection delays/behavior (slower, more production-like).")
    ap.add_argument("--sleep", type=float, default=0.5,
                    help="Sleep seconds between requests in fast mode (default 0.5).")
    args = ap.parse_args()

    run_test(fast=not args.real, sleep_between=args.sleep)

if __name__ == "__main__":
    main()
