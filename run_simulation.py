#!/usr/bin/env python3
"""
Phase 3 — Simulation runner for lets-gossip.

Automatically builds the binary and runs the gossip network with configurable
sizes, repeating each experiment with different seeds.

Usage:
  python3 run_simulation.py                          # Basic: N∈{10,20,50}, 5 runs
  python3 run_simulation.py --sweep                  # Full parameter sweep
  python3 run_simulation.py --nodes 10 --runs 2      # Quick test
"""

import subprocess
import os
import sys
import time
import shutil
import json
import argparse
from pathlib import Path

BINARY = "./letsgossip_sim"
RESULTS_DIR = "results-pow"
BASE_PORT = 9000
GOSSIP_MSG = "SIM_TEST_MESSAGE"


def kill_stale_nodes():
    """Kill any leftover node processes from a previous run."""
    subprocess.run(
        ["pkill", "-f", os.path.basename(BINARY)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(0.5)


def build_binary():
    print("[BUILD] Compiling letsgossip_sim ...")
    result = subprocess.run(
        ["go", "build", "-o", BINARY, "."],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"[BUILD] FAILED:\n{result.stderr}")
        sys.exit(1)
    print("[BUILD] OK")


def clear_logs():
    log_dir = Path("logs")
    if log_dir.exists():
        for f in log_dir.iterdir():
            if f.is_file():
                f.unlink()


def run_experiment(n, fanout, ttl, peer_limit, experiment_seed, experiment_dir,
                   hybrid=False, pow_enabled=False, pow_k=4):
    """Start N nodes, wait for stabilisation, inject one gossip, collect logs."""
    mode = "hybrid" if hybrid else "push"
    print(f"  N={n}  fanout={fanout}  ttl={ttl}  peer_limit={peer_limit}  "
          f"seed={experiment_seed}  mode={mode}")

    kill_stale_nodes()
    clear_logs()
    os.makedirs(experiment_dir, exist_ok=True)

    processes = []
    bootstrap_addr = f"127.0.0.1:{BASE_PORT}"

    try:
        # ---- seed node (no bootstrap) ----
        seed_cmd = [
            BINARY,
            "-host", "127.0.0.1",
            "-port", str(BASE_PORT),
            "-bootstrap", "",
            "-fanout", str(fanout),
            "-ttl", str(ttl),
            "-peer-limit", str(peer_limit),
            "-seed", str(experiment_seed * 1000),
        ]
        if hybrid:
            seed_cmd.append("-hybrid")
        if pow_enabled:
            seed_cmd.extend(["-pow", "-pow-k", str(pow_k)])
        seed_proc = subprocess.Popen(
            seed_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        processes.append(seed_proc)
        time.sleep(0.5)

        # Check seed node is still alive
        if seed_proc.poll() is not None:
            err = seed_proc.stderr.read().decode()
            print(f"    ERROR: seed node exited immediately: {err}")
            return

        # ---- remaining N-1 nodes ----
        for i in range(1, n):
            cmd = [
                BINARY,
                "-host", "127.0.0.1",
                "-port", str(BASE_PORT + i),
                "-bootstrap", bootstrap_addr,
                "-fanout", str(fanout),
                "-ttl", str(ttl),
                "-peer-limit", str(peer_limit),
                "-seed", str(experiment_seed * 1000 + i),
            ]
            if hybrid:
                cmd.append("-hybrid")
            if pow_enabled:
                cmd.extend(["-pow", "-pow-k", str(pow_k)])
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            processes.append(proc)
            time.sleep(0.05)

        # ---- stabilisation ----
        # Larger networks need more maintenance-loop cycles (2 s each) for
        # peer discovery to form a connected graph.
        stabilise_s = max(10, 1 + n * 0.5)
        print(f"    waiting {stabilise_s:.0f}s for network to stabilise …")
        time.sleep(stabilise_s)

        # ---- inject gossip from seed node ----
        if seed_proc.poll() is not None:
            err = seed_proc.stderr.read().decode()
            print(f"    ERROR: seed node died during stabilisation: {err}")
            return
        seed_proc.stdin.write(f"{GOSSIP_MSG}\n".encode())
        seed_proc.stdin.flush()

        propagation_s = max(10, 1 + n * 0.5)
        print(f"    gossip injected, waiting for propagation {propagation_s:.0f}s …")
        time.sleep(propagation_s)

    finally:
        for proc in processes:
            try:
                proc.terminate()
            except OSError:
                pass
        for proc in processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    # small grace period for OS file-buffer flush
    time.sleep(0.5)

    # ---- copy logs into experiment directory ----
    log_dir = Path("logs")
    for f in log_dir.iterdir():
        if f.is_file() and f.suffix == ".log":
            shutil.copy2(str(f), str(Path(experiment_dir) / f.name))

    # ---- save metadata ----
    meta = {
        "n": n,
        "fanout": fanout,
        "ttl": ttl,
        "peer_limit": peer_limit,
        "seed": experiment_seed,
        "hybrid": hybrid,
        "pow_enabled": pow_enabled,
        "pow_k": pow_k if pow_enabled else None,
        "base_port": BASE_PORT,
        "gossip_msg": GOSSIP_MSG,
    }
    with open(os.path.join(experiment_dir, "meta.json"), "w") as fh:
        json.dump(meta, fh, indent=2)

    print(f"    ✓ logs → {experiment_dir}")


def schedule_experiments(args):
    """Return a list of (n, fanout, ttl, peer_limit, seed, dir, hybrid, pow_enabled, pow_k) tuples."""
    experiments = []
    modes = [False, True] if args.hybrid else [False]
    pow_configs = [(True, k) for k in args.pow_ks] if args.pow_ks else [(False, None)]

    for n in args.nodes:
        for fanout in args.fanouts:
            for ttl in args.ttls:
                for pl in args.peer_limits:
                    for hybrid in modes:
                        for pow_enabled, pow_k in pow_configs:
                            for run in range(1, args.runs + 1):
                                mode_tag = "hybrid" if hybrid else "push"
                                pow_tag = f"_pow{pow_k}" if pow_enabled else ""
                                tag = f"N{n}_f{fanout}_ttl{ttl}_pl{pl}_{mode_tag}{pow_tag}_run{run}"
                                exp_dir = os.path.join(RESULTS_DIR, tag)
                                experiments.append((n, fanout, ttl, pl, run, exp_dir, hybrid, pow_enabled, pow_k))

    return experiments


def main():
    parser = argparse.ArgumentParser(description="lets-gossip simulation runner (Phase 3)")
    parser.add_argument("--nodes", nargs="+", type=int, default=[10, 20, 50],
                        help="Network sizes to test (default: 10 20 50)")
    parser.add_argument("--runs", type=int, default=5,
                        help="Repetitions per configuration (default: 5)")
    parser.add_argument("--fanouts", nargs="+", type=int, default=[3],
                        help="Fanout values (default: 3)")
    parser.add_argument("--ttls", nargs="+", type=int, default=[8],
                        help="TTL values (default: 8)")
    parser.add_argument("--peer-limits", nargs="+", type=int, default=[20],
                        help="Peer-limit values (default: 20)")
    parser.add_argument("--hybrid", action="store_true",
                        help="Also run Hybrid Push-Pull mode for comparison")
    parser.add_argument("--pow-ks", nargs="+", type=int, default=None,
                        help="Proof-of-Work difficulty values (k) to test (e.g., --pow-ks 2 3 4 5)")
    parser.add_argument("--sweep", action="store_true",
                        help="Run full parameter sweep (fanout, ttl, peer-limit)")
    parser.add_argument("--skip-build", action="store_true",
                        help="Skip go build step")
    args = parser.parse_args()

    if args.sweep:
        args.fanouts = [1, 2, 3, 5]
        args.ttls = [3, 5, 8]
        args.peer_limits = [5, 10, 20]

    if not args.skip_build:
        build_binary()

    experiments = schedule_experiments(args)
    total = len(experiments)
    print(f"\n{total} experiments scheduled\n")

    t_start = time.time()
    for idx, (n, fanout, ttl, pl, seed, exp_dir, hybrid, pow_enabled, pow_k) in enumerate(experiments, 1):
        elapsed = time.time() - t_start
        if idx > 1:
            eta = elapsed / (idx - 1) * (total - idx + 1)
            print(f"\n[{idx}/{total}]  (ETA {eta/60:.1f} min)")
        else:
            print(f"\n[{idx}/{total}]")
        run_experiment(n, fanout, ttl, pl, seed, exp_dir, hybrid=hybrid, pow_enabled=pow_enabled, pow_k=pow_k)

    elapsed = time.time() - t_start
    print(f"\n{'='*60}")
    print(f"All {total} experiments finished in {elapsed/60:.1f} minutes.")
    print(f"Run  python3 analyze.py  to compute metrics and generate plots.")


if __name__ == "__main__":
    main()
