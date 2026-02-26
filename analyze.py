#!/usr/bin/env python3
"""
Phase 3 — Analysis script for lets-gossip simulation results.

Reads JSON logs produced by run_simulation.py, computes:
  • Convergence Time  — time until 95 % of nodes have received the GOSSIP
  • Message Overhead  — total transmitted messages (all types) in that window

Generates comparative plots in the  plots/  directory.

Usage:
  python3 analyze.py                 # analyse results/ and generate plots/
  python3 analyze.py --results DIR   # use a custom results directory
"""

import json
import math
import os
import sys
import argparse
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")  # headless backend — works over SSH too
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError:
    print("matplotlib and numpy are required.\n  pip install matplotlib numpy")
    sys.exit(1)

RESULTS_DIR = "results"
PLOTS_DIR = "plots"
COVERAGE_THRESHOLD = 0.95


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------

def parse_experiment(experiment_dir):
    """Parse one experiment directory and return a metrics dict (or None)."""
    meta_path = os.path.join(experiment_dir, "meta.json")
    if not os.path.exists(meta_path):
        return None
    with open(meta_path) as f:
        meta = json.load(f)

    log_files = sorted(
        p for p in Path(experiment_dir).iterdir()
        if p.suffix == ".log"
    )
    if not log_files:
        return None

    # Collect every JSON event across all log files
    all_events = []
    node_ids = set()
    for lf in log_files:
        with open(lf) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                    all_events.append(ev)
                    if "node" in ev:
                        node_ids.add(ev["node"])
                except json.JSONDecodeError:
                    continue

    n_nodes = len(node_ids)
    if n_nodes == 0:
        return None

    # Find the gossip_originated event (the simulation test message)
    originated = None
    for ev in all_events:
        if ev.get("event") == "gossip_originated":
            originated = ev
            break
    if originated is None:
        print(f"  WARNING: no gossip_originated in {experiment_dir}")
        return None

    msg_id = originated["message_id"]
    t0 = originated["timestamp_ms"]
    origin_node = originated["node"]

    # Gather the earliest gossip_received timestamp per node for this msg_id
    received_at = {origin_node: t0}  # originator already has the message
    for ev in all_events:
        if ev.get("event") == "gossip_received" and ev.get("message_id") == msg_id:
            node = ev["node"]
            ts = ev["timestamp_ms"]
            if node not in received_at or ts < received_at[node]:
                received_at[node] = ts

    # 95 % convergence
    threshold_count = math.ceil(COVERAGE_THRESHOLD * n_nodes)
    sorted_times = sorted(received_at.values())

    if len(sorted_times) >= threshold_count:
        convergence_ms = sorted_times[threshold_count - 1] - t0
        convergence_abs = sorted_times[threshold_count - 1]
    else:
        convergence_ms = None
        convergence_abs = None

    # Message overhead: every transmitted message from t0 until convergence.
    # "message_sent" covers control messages (HELLO, GET_PEERS, PEERS_LIST,
    # PING, PONG, IHAVE, IWANT) while "gossip_forwarded" covers GOSSIP
    # forwards (the engine bypasses sendMessage and logs a separate event).
    counted_events = {"message_sent", "gossip_forwarded"}
    overhead = 0
    for ev in all_events:
        if ev.get("event") in counted_events:
            ts = ev["timestamp_ms"]
            if ts >= t0 and (convergence_abs is None or ts <= convergence_abs):
                overhead += 1

    coverage_pct = len(received_at) / n_nodes * 100.0

    # Extract PoW mining times
    pow_mining_times = []
    pow_k_value = meta.get("pow_k")
    if pow_k_value is not None:
        for ev in all_events:
            if ev.get("event") == "proof_of_work_mined":
                # Collect all mining times for this experiment (all nodes use same pow_k)
                pow_mining_times.append(ev.get("duration_ms", 0))

    return {
        "n": meta["n"],
        "fanout": meta["fanout"],
        "ttl": meta["ttl"],
        "peer_limit": meta["peer_limit"],
        "seed": meta["seed"],
        "hybrid": meta.get("hybrid", False),
        "pow_enabled": meta.get("pow_enabled", False),
        "pow_k": pow_k_value,
        "n_nodes_actual": n_nodes,
        "coverage_pct": coverage_pct,
        "convergence_time_ms": convergence_ms,
        "message_overhead": overhead,
        "pow_mining_times": pow_mining_times,
    }


def parse_all(results_dir):
    rd = Path(results_dir)
    if not rd.exists():
        print(f"Directory {results_dir} not found.  Run run_simulation.py first.")
        sys.exit(1)

    experiments = sorted(d for d in rd.iterdir() if d.is_dir())
    results = []
    for exp in experiments:
        print(f"  parsing {exp.name} …")
        r = parse_experiment(str(exp))
        if r:
            results.append(r)
    return results


# ---------------------------------------------------------------------------
# Plotting helpers
# ---------------------------------------------------------------------------

def _group_by(results, key):
    """Group results by *key* and compute mean/std of metrics."""
    buckets = defaultdict(lambda: {"conv": [], "over": []})
    for r in results:
        v = r[key]
        if r["convergence_time_ms"] is not None:
            buckets[v]["conv"].append(r["convergence_time_ms"])
        buckets[v]["over"].append(r["message_overhead"])
    return buckets


def _bar_plot(ax, xs, means, stds, colour, ylabel):
    """Draw a grouped bar chart with error bars."""
    x_pos = np.arange(len(xs))
    ax.bar(x_pos, means, yerr=stds, capsize=5, color=colour, alpha=0.8, width=0.5)
    ax.set_xticks(x_pos)
    ax.set_xticklabels([str(x) for x in xs])
    ax.set_ylabel(ylabel, fontsize=11)
    ax.grid(axis="y", alpha=0.3)


def plot_vs_n(results, output_dir):
    """Main plot: convergence time & message overhead vs network size N."""
    buckets = _group_by(results, "n")
    if len(buckets) < 2:
        return
    ns = sorted(buckets)
    conv_mean = [np.mean(buckets[n]["conv"]) if buckets[n]["conv"] else 0 for n in ns]
    conv_std  = [np.std(buckets[n]["conv"])  if buckets[n]["conv"] else 0 for n in ns]
    over_mean = [np.mean(buckets[n]["over"]) for n in ns]
    over_std  = [np.std(buckets[n]["over"])  for n in ns]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    _bar_plot(ax1, ns, conv_mean, conv_std, "#4C72B0", "Convergence Time (ms)")
    ax1.set_xlabel("Network Size (N)", fontsize=11)
    ax1.set_title("Convergence Time vs N", fontsize=13)

    _bar_plot(ax2, ns, over_mean, over_std, "#DD8452", "Message Overhead")
    ax2.set_xlabel("Network Size (N)", fontsize=11)
    ax2.set_title("Message Overhead vs N", fontsize=13)

    plt.tight_layout()
    path = os.path.join(output_dir, "convergence_vs_N.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  saved {path}")


def plot_param_effect(results, param_label, param_key, output_dir):
    """Plot convergence time & overhead grouped by a single parameter."""
    buckets = _group_by(results, param_key)
    if len(buckets) < 2:
        return
    vals = sorted(buckets)
    conv_mean = [np.mean(buckets[v]["conv"]) if buckets[v]["conv"] else 0 for v in vals]
    conv_std  = [np.std(buckets[v]["conv"])  if buckets[v]["conv"] else 0 for v in vals]
    over_mean = [np.mean(buckets[v]["over"]) for v in vals]
    over_std  = [np.std(buckets[v]["over"])  for v in vals]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    _bar_plot(ax1, vals, conv_mean, conv_std, "#4C72B0", "Convergence Time (ms)")
    ax1.set_xlabel(param_label, fontsize=11)
    ax1.set_title(f"Convergence Time vs {param_label}", fontsize=13)

    _bar_plot(ax2, vals, over_mean, over_std, "#DD8452", "Message Overhead")
    ax2.set_xlabel(param_label, fontsize=11)
    ax2.set_title(f"Message Overhead vs {param_label}", fontsize=13)

    plt.tight_layout()
    fname = f"effect_{param_key}.png"
    path = os.path.join(output_dir, fname)
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  saved {path}")


def plot_multiline_vs_n(results, param_key, param_label, output_dir):
    """
    For each distinct value of *param_key*, draw a line of
    convergence-time vs N.  Useful when --sweep is used.
    """
    # group by (param_value, N)
    combo = defaultdict(lambda: {"conv": [], "over": []})
    param_vals = set()
    ns_set = set()
    for r in results:
        pv = r[param_key]
        n = r["n"]
        param_vals.add(pv)
        ns_set.add(n)
        if r["convergence_time_ms"] is not None:
            combo[(pv, n)]["conv"].append(r["convergence_time_ms"])
        combo[(pv, n)]["over"].append(r["message_overhead"])

    if len(param_vals) < 2 or len(ns_set) < 2:
        return

    param_vals = sorted(param_vals)
    ns = sorted(ns_set)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    for pv in param_vals:
        means_c = [np.mean(combo[(pv, n)]["conv"]) if combo[(pv, n)]["conv"] else None for n in ns]
        means_o = [np.mean(combo[(pv, n)]["over"]) if combo[(pv, n)]["over"] else 0 for n in ns]
        label = f"{param_label}={pv}"
        ax1.plot(ns, means_c, marker="o", label=label)
        ax2.plot(ns, means_o, marker="s", label=label)

    ax1.set_xlabel("N"); ax1.set_ylabel("Convergence Time (ms)")
    ax1.set_title(f"Convergence Time by {param_label}"); ax1.legend(); ax1.grid(alpha=0.3)
    ax2.set_xlabel("N"); ax2.set_ylabel("Message Overhead")
    ax2.set_title(f"Message Overhead by {param_label}"); ax2.legend(); ax2.grid(alpha=0.3)
    plt.tight_layout()
    path = os.path.join(output_dir, f"multiline_{param_key}.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  saved {path}")


def plot_pow_analysis(results, output_dir):
    """Plot PoW mining time vs pow_k difficulty."""
    # Filter results with PoW enabled
    pow_results = [r for r in results if r.get("pow_enabled") and r.get("pow_k") is not None]
    if not pow_results:
        return

    # Group by pow_k and collect all mining times
    pow_by_k = defaultdict(list)
    for r in pow_results:
        k = r["pow_k"]
        if r.get("pow_mining_times"):
            pow_by_k[k].extend(r["pow_mining_times"])

    if not pow_by_k:
        return

    # Calculate statistics per pow_k
    ks = sorted(pow_by_k.keys())
    means = [np.mean(pow_by_k[k]) for k in ks]
    stds = [np.std(pow_by_k[k]) for k in ks]
    medians = [np.median(pow_by_k[k]) for k in ks]

    # Create plots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Plot 1: Mean mining time with error bars
    x_pos = np.arange(len(ks))
    ax1.bar(x_pos, means, yerr=stds, capsize=5, color="#4C72B0", alpha=0.8, width=0.5)
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels([f"k={k}" for k in ks])
    ax1.set_xlabel("Proof-of-Work Difficulty (k)", fontsize=11)
    ax1.set_ylabel("Mean Mining Time (ms)", fontsize=11)
    ax1.set_title("PoW Mining Time vs Difficulty (Mean ± Std)", fontsize=13)
    ax1.grid(axis="y", alpha=0.3)

    # Plot 2: Box plot for distribution
    data_to_plot = [pow_by_k[k] for k in ks]
    bp = ax2.boxplot(data_to_plot, labels=[f"k={k}" for k in ks], patch_artist=True)
    for patch in bp['boxes']:
        patch.set_facecolor("#55A868")
        patch.set_alpha(0.7)
    ax2.set_xlabel("Proof-of-Work Difficulty (k)", fontsize=11)
    ax2.set_ylabel("Mining Time (ms)", fontsize=11)
    ax2.set_title("PoW Mining Time Distribution", fontsize=13)
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    path = os.path.join(output_dir, "pow_analysis.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  saved {path}")

    # Print summary table
    print("\n  PoW Mining Time Summary (mean ± std, median):")
    print(f"  {'k':>4}  {'Mean (ms)':>14}  {'Std (ms)':>14}  {'Median (ms)':>14}  {'Samples':>10}")
    print(f"  {'-'*70}")
    for k in ks:
        times = pow_by_k[k]
        mean_str = f"{np.mean(times):.1f} ± {np.std(times):.1f}"
        median_str = f"{np.median(times):.1f}"
        print(f"  {k:>4}  {mean_str:>14}  {np.std(times):>14.1f}  {median_str:>14}  {len(times):>10}")


def plot_push_vs_hybrid(results, output_dir):
    """Side-by-side bar chart comparing Push-only vs Hybrid for each N."""
    push   = [r for r in results if not r["hybrid"]]
    hybrid = [r for r in results if r["hybrid"]]
    if not push or not hybrid:
        return

    push_by_n   = _group_by(push,   "n")
    hybrid_by_n = _group_by(hybrid, "n")
    ns = sorted(set(push_by_n) & set(hybrid_by_n))
    if not ns:
        return

    x = np.arange(len(ns))
    w = 0.35

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    for idx, (metric, ylabel) in enumerate([("conv", "Convergence Time (ms)"),
                                             ("over", "Message Overhead")]):
        ax = axes[idx]
        p_mean = [np.mean(push_by_n[n][metric])   if push_by_n[n][metric]   else 0 for n in ns]
        p_std  = [np.std(push_by_n[n][metric])    if push_by_n[n][metric]   else 0 for n in ns]
        h_mean = [np.mean(hybrid_by_n[n][metric]) if hybrid_by_n[n][metric] else 0 for n in ns]
        h_std  = [np.std(hybrid_by_n[n][metric])  if hybrid_by_n[n][metric] else 0 for n in ns]

        ax.bar(x - w/2, p_mean, w, yerr=p_std, capsize=4, label="Push-only",  color="#4C72B0")
        ax.bar(x + w/2, h_mean, w, yerr=h_std, capsize=4, label="Hybrid",     color="#55A868")
        ax.set_xticks(x)
        ax.set_xticklabels([str(n) for n in ns])
        ax.set_xlabel("Network Size (N)", fontsize=11)
        ax.set_ylabel(ylabel, fontsize=11)
        ax.set_title(f"{ylabel.split('(')[0].strip()} — Push vs Hybrid", fontsize=13)
        ax.legend()
        ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    path = os.path.join(output_dir, "push_vs_hybrid.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  saved {path}")

    # Print mean ± std summary
    print("\n  Push-only vs Hybrid summary (mean ± std):")
    print(f"  {'N':>4}  {'Mode':<8}  {'Conv (ms)':>14}  {'Overhead':>14}")
    print(f"  {'-'*48}")
    for n in ns:
        for label, by_n in [("Push", push_by_n), ("Hybrid", hybrid_by_n)]:
            c = by_n[n]["conv"]
            o = by_n[n]["over"]
            c_str = f"{np.mean(c):.1f} ± {np.std(c):.1f}" if c else "N/A"
            o_str = f"{np.mean(o):.1f} ± {np.std(o):.1f}"
            print(f"  {n:>4}  {label:<8}  {c_str:>14}  {o_str:>14}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="lets-gossip Phase 3 analysis")
    parser.add_argument("--results", default=RESULTS_DIR, help="Results directory")
    parser.add_argument("--plots", default=PLOTS_DIR, help="Output directory for plots")
    args = parser.parse_args()

    print("Parsing experiment logs …")
    results = parse_all(args.results)
    if not results:
        print("No valid experiment data found.")
        sys.exit(1)

    # ---- summary table ----
    hdr = f"{'N':>4} {'Fan':>4} {'TTL':>4} {'PL':>4} {'Mode':<7} {'Pow':>4} {'Seed':>4} {'Nodes':>5} {'Cov%':>6} {'Conv(ms)':>9} {'Overhead':>9}"
    print(f"\n{'='*len(hdr)}\n{hdr}\n{'-'*len(hdr)}")
    for r in sorted(results, key=lambda x: (x["n"], x["hybrid"], x.get("pow_k") or 0, x["fanout"], x["ttl"], x["peer_limit"], x["seed"])):
        conv = str(r["convergence_time_ms"]) if r["convergence_time_ms"] is not None else "N/A"
        mode = "hybrid" if r["hybrid"] else "push"
        pow_str = f"k={r['pow_k']}" if r.get("pow_enabled") and r.get("pow_k") is not None else "no"
        print(f"{r['n']:>4} {r['fanout']:>4} {r['ttl']:>4} {r['peer_limit']:>4} "
              f"{mode:<7} {pow_str:>4} {r['seed']:>4} {r['n_nodes_actual']:>5} {r['coverage_pct']:>5.1f}% "
              f"{conv:>9} {r['message_overhead']:>9}")
    print("=" * len(hdr))

    # ---- generate plots ----
    os.makedirs(args.plots, exist_ok=True)
    print("\nGenerating plots …")

    # 1. Main plot — metrics vs N (push-only with default fanout/ttl/pl)
    default_push = [
        r for r in results
        if r["fanout"] == 3 and r["ttl"] == 8 and r["peer_limit"] == 20
        and not r["hybrid"]
    ]
    plot_vs_n(default_push or [r for r in results if not r["hybrid"]] or results,
              args.plots)

    # 2. Push-only vs Hybrid comparison
    plot_push_vs_hybrid(results, args.plots)

    # 3. Parameter-effect plots (only if more than one value exists)
    plot_param_effect(results, "Fanout", "fanout", args.plots)
    plot_param_effect(results, "TTL", "ttl", args.plots)
    plot_param_effect(results, "Peer Limit", "peer_limit", args.plots)

    # 4. Multi-line plots for sweep mode
    plot_multiline_vs_n(results, "fanout", "Fanout", args.plots)
    plot_multiline_vs_n(results, "ttl", "TTL", args.plots)
    plot_multiline_vs_n(results, "peer_limit", "PeerLimit", args.plots)

    # 5. PoW analysis plots
    plot_pow_analysis(results, args.plots)

    # ---- CSV export ----
    csv_path = os.path.join(args.plots, "results.csv")
    with open(csv_path, "w") as f:
        f.write("n,fanout,ttl,peer_limit,hybrid,pow_enabled,pow_k,seed,n_nodes_actual,"
                "coverage_pct,convergence_time_ms,message_overhead,pow_mean_ms,pow_std_ms\n")
        for r in results:
            conv = r["convergence_time_ms"] if r["convergence_time_ms"] is not None else ""
            pow_k = r.get("pow_k") if r.get("pow_k") is not None else ""
            pow_enabled = r.get("pow_enabled", False)
            pow_times = r.get("pow_mining_times", [])
            pow_mean = np.mean(pow_times) if pow_times else ""
            pow_std = np.std(pow_times) if pow_times else ""
            f.write(f"{r['n']},{r['fanout']},{r['ttl']},{r['peer_limit']},"
                    f"{r['hybrid']},{pow_enabled},{pow_k},{r['seed']},{r['n_nodes_actual']},"
                    f"{r['coverage_pct']:.1f},{conv},{r['message_overhead']},"
                    f"{pow_mean},{pow_std}\n")
    print(f"  saved {csv_path}")

    print("\nDone!  Check the plots/ directory.")


if __name__ == "__main__":
    main()
