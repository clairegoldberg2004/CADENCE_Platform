"""
Interdependency Model for Energy Project Deployment

This module implements a DAG-based model for sequencing energy infrastructure projects
with cross-project dependencies. Projects move through stages (Definition → Approvals → 
Construction → Commissioning), and additional edges enforce coordinated build-out 
between different technology types.

Key Interdependencies:
- Transmission gating for Solar (5 MW Solar : 1 MW Transmission)
  - 20% of Solar can start Construction when Transmission reaches FID (Approvals)
  - 80% waits until Transmission Construction completes
- Gas turbine support for Solar (5:1)
  - Solar Construction depends on matched Gas Construction completing
- Battery co-commissioning with Solar (1:1)
  - Solar cannot complete until matched Battery projects complete Construction
  - Enforced via zero-duration joint commissioning nodes

Example Usage:
    from pathlib import Path
    from base_interdependency_model import (
        load_solar_targets,
        load_gas_targets,
        load_battery_targets,
        load_transmission_targets,
        generate_projects_multi_tech,
        build_durations_with_commissioning,
        build_system_dag_with_interdeps,
        build_system_dag_sequencing,
        cpm_forward_with_seeds,
        relabel_joint_commissioning_nodes,
        export_results_to_dataframes,
        INTERDEPENDENCY_PARAMS,
    )
    
    # Load capacity targets from CSV
    # This model expects an **annual additions (delta)** CSV in the schema of
    # `capacity_delta_subset(in).csv`.
    csv_path = Path("capacity_delta_subset(in).csv")
    solar_targets = load_solar_targets(csv_path, zones=["qld-north"])
    gas_targets = load_gas_targets(csv_path, zones=["qld-north"])
    battery_targets = load_battery_targets(csv_path, zones=["qld-north"])
    transmission_targets = load_transmission_targets(csv_path, zones=["qld-north"])
    
    # Generate projects and run model...
"""

import random
from pathlib import Path
from typing import Dict, List, Tuple

import hashlib
import networkx as nx
import numpy as np
import pandas as pd


# ============================================================================
# CONSTANTS
# ============================================================================

# Project stages (without commissioning)
STAGES = ["Definition", "Approvals", "Construction"]

# Project stages (with commissioning for joint commissioning nodes)
# Note: "Commissioning" is 0-duration in this model. We mostly use it as a clean
# “finish marker” (and to implement joint commissioning for Solar+Batteries).
STAGES4 = ["Definition", "Approvals", "Construction", "Commissioning"]

# Interdependency parameters
# These are the knobs you tweak when you want different “rules of the road”
# (how much transmission unlocks solar, ratios between techs, etc.).
INTERDEPENDENCY_PARAMS = {
    # Matching policy: by default, if we run out of supporting capacity (TX/Gas/Battery),
    # the dependent Solar project proceeds unconstrained (legacy behavior).
    # If you want a "fail-closed" regime where unmatched Solar projects do not deploy,
    # set matching.allow_unmatched_solar = False in your params dict.
    "matching": {
        "allow_unmatched_solar": True,
    },
    "transmission": {
        "solar_fraction_proceed_at_FID": 0.20,
        "solar_fraction_proceed_at_completion": 0.80,
        "solar_to_transmission_ratio": (5.0, 1.0),
        "rep_project_size_MW": 500.0,
        "lead_time_months": 120,
    },
    "gas": {
        "solar_to_gas_ratio": (5.0, 1.0),
    },
    "battery": {
        "solar_to_battery_ratio": (1.0, 1.0),
    },
    "attrition": {
        "enabled": False,  # Set to True to enable attrition
        "base_rate": 0.05,  # Base attrition rate (5%)
        "delay_threshold_years": 1.0,  # Years of delay where attrition becomes significant
        "max_rate": 0.50,  # Maximum attrition rate (50%)
    },
}

# Default stage durations (in months) for each technology
DEFAULT_DURATIONS = {
    "Solar": {
        "Definition_months": 12,
        "Approvals_months": 12,
        "Construction_months": 18,
    },
    "Transmission": {
        "Definition_months": 24,
        "Approvals_months": 36,
        "Construction_months": 120,  # Uses INTERDEPENDENCY_PARAMS["transmission"]["lead_time_months"]
    },
    "Gas Turbine": {
        "Definition_months": 12,
        "Approvals_months": 12,
        "Construction_months": 18,
    },
    "Battery": {
        "Definition_months": 6,
        "Approvals_months": 6,
        "Construction_months": 12,
    },
}

# Duration distribution parameters (optional - if None or enabled=False, uses fixed durations)
# When enabled, each project samples its own durations from the specified distribution
# This is meant to represent “natural variation” (not a modelled delay/shock).
# If you turn this on, two solar projects can have different construction times, etc.
DURATION_DISTRIBUTIONS = {
    "Solar": {
        "enabled": False,  # Set to True to enable per-project duration sampling
        "Definition_months": {
            "distribution": "normal",  # "normal", "lognormal", or "uniform"
            "mean": 12,
            "std": 2,  # Standard deviation for normal/lognormal distributions
            "min": 6,   # Optional minimum bound (clips samples)
            "max": 18,  # Optional maximum bound (clips samples)
        },
        "Approvals_months": {
            "distribution": "normal",
            "mean": 12,
            "std": 2,
            "min": 6,
            "max": 18,
        },
        "Construction_months": {
            "distribution": "normal",
            "mean": 18,
            "std": 3,
            "min": 9,
            "max": 27,
        },
    },
    "Transmission": {
        "enabled": False,
        "Definition_months": {
            "distribution": "normal",
            "mean": 24,
            "std": 4,
            "min": 12,
            "max": 36,
        },
        "Approvals_months": {
            "distribution": "normal",
            "mean": 36,
            "std": 6,
            "min": 18,
            "max": 54,
        },
        "Construction_months": {
            "distribution": "normal",
            "mean": 120,  # Will be overridden by transmission lead_time_months if using params
            "std": 20,
            "min": 60,
            "max": 180,
        },
    },
    "Gas Turbine": {
        "enabled": False,
        "Definition_months": {
            "distribution": "normal",
            "mean": 12,
            "std": 2,
            "min": 6,
            "max": 18,
        },
        "Approvals_months": {
            "distribution": "normal",
            "mean": 12,
            "std": 2,
            "min": 6,
            "max": 18,
        },
        "Construction_months": {
            "distribution": "normal",
            "mean": 18,
            "std": 3,
            "min": 9,
            "max": 27,
        },
    },
    "Battery": {
        "enabled": False,
        "Definition_months": {
            "distribution": "normal",
            "mean": 6,
            "std": 1,
            "min": 3,
            "max": 9,
        },
        "Approvals_months": {
            "distribution": "normal",
            "mean": 6,
            "std": 1,
            "min": 3,
            "max": 9,
        },
        "Construction_months": {
            "distribution": "normal",
            "mean": 12,
            "std": 2,
            "min": 6,
            "max": 18,
        },
    },
}

# Default zones to analyze
DEFAULT_ZONES = ["qld-north"]

def summarize_solar_matching_and_kills(G: nx.DiGraph) -> Dict[str, float]:
    """
    Summarize Solar matching coverage (TX/Gas/Battery) and kill-switch outcomes.

    This inspects the *constructed interdependency DAG* (typically right after
    `build_system_dag_with_interdeps(...)` and before CPM/attrition).

    Outputs are MW-weighted and project-weighted where possible, as flat numeric fields.
    """
    # Gather unique Solar project IDs from Definition-stage nodes (one row per project).
    solar_pids: List[str] = []
    solar_cap_by_pid: Dict[str, float] = {}
    solar_zone_by_pid: Dict[str, str] = {}
    solar_year_by_pid: Dict[str, int] = {}

    for n in G.nodes:
        if not isinstance(n, tuple) or len(n) != 2:
            continue
        pid, stage = n
        if stage != "Definition":
            continue
        if G.nodes[n].get("TECH") != "Solar":
            continue
        pid_s = str(pid)
        solar_pids.append(pid_s)
        solar_cap_by_pid[pid_s] = float(G.nodes[n].get("Capacity", 0.0))
        solar_zone_by_pid[pid_s] = str(G.nodes[n].get("ZONE", ""))
        try:
            solar_year_by_pid[pid_s] = int(G.nodes[n].get("YEAR", 0))
        except Exception:
            solar_year_by_pid[pid_s] = 0

    total_projects = len(solar_pids)
    total_mw = float(sum(solar_cap_by_pid.values()))

    # Match coverage: detect edges into Solar Construction / Commissioning.
    has_tx: set[str] = set()
    has_gas: set[str] = set()
    has_battery_joint: set[str] = set()

    for pid in solar_pids:
        cons = (pid, "Construction")
        comm = (pid, "Commissioning")

        if cons in G.nodes:
            for p in G.predecessors(cons):
                tech = G.nodes[p].get("TECH")
                if tech == "Transmission":
                    has_tx.add(pid)
                elif tech == "Gas Turbine":
                    has_gas.add(pid)

        if comm in G.nodes:
            for p in G.predecessors(comm):
                if G.nodes[p].get("TECH") == "Joint":
                    has_battery_joint.add(pid)
                    break

    def mw_for(pids: set[str]) -> float:
        return float(sum(solar_cap_by_pid.get(pid, 0.0) for pid in pids))

    # Kill switch: infer killed Solar projects by looking for unmatched_kill on Definition node.
    killed: set[str] = set()
    killed_reason: Dict[str, str] = {}
    for pid in solar_pids:
        dn = (pid, "Definition")
        if dn in G.nodes and bool(G.nodes[dn].get("unmatched_kill", False)):
            killed.add(pid)
            killed_reason[pid] = str(G.nodes[dn].get("unmatched_kill_reason", "") or "")

    killed_mw = mw_for(killed)

    # By-reason breakdowns (counts + MW)
    by_reason_count: Dict[str, int] = {}
    by_reason_mw: Dict[str, float] = {}
    for pid in killed:
        r = killed_reason.get(pid, "")
        by_reason_count[r] = by_reason_count.get(r, 0) + 1
        by_reason_mw[r] = by_reason_mw.get(r, 0.0) + solar_cap_by_pid.get(pid, 0.0)

    def emit_reason(prefix: str, reason: str) -> Dict[str, float]:
        return {
            f"{prefix}_killed_projects": float(by_reason_count.get(reason, 0)),
            f"{prefix}_killed_MW": float(by_reason_mw.get(reason, 0.0)),
        }

    out: Dict[str, float] = {
        "solar_projects_total": float(total_projects),
        "solar_total_MW": float(total_mw),
        "solar_has_tx_dep_projects": float(len(has_tx)),
        "solar_has_tx_dep_MW": float(mw_for(has_tx)),
        "solar_has_gas_dep_projects": float(len(has_gas)),
        "solar_has_gas_dep_MW": float(mw_for(has_gas)),
        "solar_has_battery_joint_projects": float(len(has_battery_joint)),
        "solar_has_battery_joint_MW": float(mw_for(has_battery_joint)),
        "solar_killed_projects": float(len(killed)),
        "solar_killed_MW": float(killed_mw),
    }

    # Shares (guarded)
    if total_projects:
        out["solar_has_tx_dep_share_projects"] = float(len(has_tx)) / float(total_projects)
        out["solar_has_gas_dep_share_projects"] = float(len(has_gas)) / float(total_projects)
        out["solar_has_battery_joint_share_projects"] = float(len(has_battery_joint)) / float(total_projects)
        out["solar_survival_share_projects"] = float(total_projects - len(killed)) / float(total_projects)
    else:
        out["solar_has_tx_dep_share_projects"] = 0.0
        out["solar_has_gas_dep_share_projects"] = 0.0
        out["solar_has_battery_joint_share_projects"] = 0.0
        out["solar_survival_share_projects"] = 0.0

    if total_mw > 1e-9:
        out["solar_has_tx_dep_share_MW"] = float(mw_for(has_tx)) / float(total_mw)
        out["solar_has_gas_dep_share_MW"] = float(mw_for(has_gas)) / float(total_mw)
        out["solar_has_battery_joint_share_MW"] = float(mw_for(has_battery_joint)) / float(total_mw)
        out["solar_survival_share_MW"] = float(total_mw - killed_mw) / float(total_mw)
    else:
        out["solar_has_tx_dep_share_MW"] = 0.0
        out["solar_has_gas_dep_share_MW"] = 0.0
        out["solar_has_battery_joint_share_MW"] = 0.0
        out["solar_survival_share_MW"] = 0.0

    # Common kill reasons (explicit fields for easy CSV work)
    out.update(emit_reason("kill_tx_no_tx_in_zone", "unmatched_transmission_no_tx_in_zone"))
    out.update(emit_reason("kill_tx_insufficient", "unmatched_transmission_insufficient_capacity"))
    out.update(emit_reason("kill_gas_no_gas_in_zone", "unmatched_gas_no_gas_in_zone"))
    out.update(emit_reason("kill_gas_insufficient", "unmatched_gas_insufficient_capacity"))
    out.update(emit_reason("kill_battery_no_battery_in_zone", "unmatched_battery_no_battery_in_zone"))
    out.update(emit_reason("kill_battery_insufficient", "unmatched_battery_insufficient_capacity"))

    return out

def _stable_int_seed(*parts: object, mod: int = 2**31) -> int:
    """
    Deterministic integer seed derived from arbitrary parts.

    Why: Python's built-in hash() is randomized per process (PYTHONHASHSEED),
    which makes results *non-reproducible across runs* when used for RNG seeding.
    We use a stable cryptographic hash instead.
    """
    s = "||".join(str(p) for p in parts)
    digest = hashlib.blake2b(s.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % int(mod)


# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================


def load_targets_from_capacity_subset(
    csv_path: Path,
    detailed_filters: List[str] | str,
    type_filter: str | None = None,
    tech_label: str = "Solar",
    unit_expected: str | List[str] = "gigawatt",
    zones: List[str] | None = None,
    run_name: str | None = None,
    sequencing_sector: str | None = None,
) -> pd.DataFrame:
    """
    Load capacity targets from a CSV file and filter by technology type and zone.
    
    Note: This function handles both "new" and "retrofit" types (includes both).
    The data represents annual additions (delta) per year.
    
    Args:
        csv_path: Path to the CSV file containing capacity data
        detailed_filters: Single string or list of strings for detailed technology types
                         (e.g., "large-scale solar pv" or ["gas combustion turbine", "gas combined cycle"])
        type_filter: Optional filter for technology type (e.g., "fixed", "thermal", "electric storage")
                    If None, no type filter is applied
        tech_label: Label to assign to the technology in the output (e.g., "Solar", "Gas Turbine", "Battery")
        unit_expected: Expected unit for capacity values (e.g., "gigawatt")
        zones: Optional list of zones to filter by. If None, includes all zones.
    
    Returns:
        DataFrame with columns: TECH, YEAR, ZONE, add_capacity_MW
    
    Raises:
        ValueError: If no rows match the filters or if zones filter results in empty data
    """
    print(f"[INFO] Loading {tech_label} targets from {csv_path.name}...")
    df = pd.read_csv(csv_path)
    print(f"       Loaded {len(df):,} total rows from CSV")

    # Optional: filter by run name (capacity_delta_subset(in).csv can include multiple scenarios)
    if run_name is not None:
        if "run name" in df.columns:
            before = len(df)
            df = df[df["run name"].fillna("").astype(str).str.lower() == str(run_name).lower()].copy()
            print(f"       Filtered by run name='{run_name}': {before:,} -> {len(df):,} rows")
            if df.empty:
                raise ValueError(f"No rows matched run name='{run_name}' in {csv_path}")
        else:
            print(f"[WARN] run_name filter requested but CSV has no 'run name' column. Ignoring run_name='{run_name}'.")

    # Optional: filter by sequencing sector (present in capacity_delta_subset(in).csv)
    if sequencing_sector is not None:
        if "sequencing_sector" in df.columns:
            before = len(df)
            df = df[df["sequencing_sector"].fillna("").astype(str).str.lower() == str(sequencing_sector).lower()].copy()
            print(f"       Filtered by sequencing_sector='{sequencing_sector}': {before:,} -> {len(df):,} rows")
            if df.empty:
                raise ValueError(f"No rows matched sequencing_sector='{sequencing_sector}' in {csv_path}")
        else:
            print(f"[WARN] sequencing_sector filter requested but CSV has no 'sequencing_sector' column. Ignoring sequencing_sector='{sequencing_sector}'.")
    
    # Convert single filter to list for consistent processing
    if isinstance(detailed_filters, str):
        detailed_filters = [detailed_filters]
    
    print(f"       Filtering for technologies: {', '.join(detailed_filters)}")
    if type_filter:
        print(f"       Type filter: {type_filter}")
    
    # Filter by detailed technology type(s) - match any of the filters
    # Handle empty strings in tech||outputs_group_detailed (some rows may have blank values)
    detailed_mask = df["tech||outputs_group_detailed"].fillna("").str.lower().isin(
        [f.lower() for f in detailed_filters]
    )
    
    # Apply type filter if specified
    if type_filter is not None:
        type_mask = df["tech||type"].str.lower() == type_filter.lower()
        q = detailed_mask & type_mask
    else:
        q = detailed_mask
    
    # Note: Include both "new" and "retrofit" types (the "type" column in CSV)
    # We don't filter by the CSV's "type" column here - we include all matching technologies
    tech_df = df.loc[q].copy()
    print(f"       Found {len(tech_df):,} rows matching technology filters")
    
    if tech_df.empty:
        filter_str = ", ".join(detailed_filters)
        type_str = f" and type='{type_filter}'" if type_filter else ""
        raise ValueError(
            f"No rows matched detailed='{filter_str}'{type_str} in {csv_path}"
        )

    # Handle missing zones
    tech_df["zone"] = tech_df["zone"].fillna("unknown")
    
    # Filter by zones if specified
    if zones:
        zones_lower = {z.lower() for z in zones}
        tech_df = tech_df[tech_df["zone"].str.lower().isin(zones_lower)]
        print(f"       Filtered to zones: {zones} -> {len(tech_df):,} rows")
        if tech_df.empty:
            raise ValueError(f"No rows left after filtering zones={zones}")
    else:
        print(f"       No zone filter applied")

    # Check units
    units = tech_df["unit"].dropna().astype(str).str.lower().unique().tolist()
    expected_units = [unit_expected] if isinstance(unit_expected, str) else list(unit_expected)
    expected_units_lower = {u.lower() for u in expected_units}
    if not (set(units) & expected_units_lower):
        print(f"[WARN] Expected unit in {sorted(expected_units_lower)}, found {units}. Proceeding anyway.")

    # Aggregate by year and zone (summing across multiple technologies if multiple filters)
    # This is the key “targets” output: how many MW get added each year in each zone.
    agg = (
        tech_df.groupby(["year", "zone"], as_index=False)["value"].sum()
        .rename(columns={"value": "add_capacity_GW"})
    )
    agg["TECH"] = tech_label
    agg["add_capacity_MW"] = agg["add_capacity_GW"] * 1000.0
    agg = agg.rename(columns={"year": "YEAR", "zone": "ZONE"})
    
    total_capacity = agg["add_capacity_MW"].sum()
    print(f"       Total {tech_label} capacity loaded: {total_capacity:,.2f} MW across {len(agg)} year-zone combinations")
    print(f"       Years: {sorted(agg['YEAR'].unique())}")
    print(f"       Zones: {sorted(agg['ZONE'].unique())}")
    
    return agg[["TECH", "YEAR", "ZONE", "add_capacity_MW"]]


def subtract_initial_queue_from_targets(
    targets_df: pd.DataFrame,
    tech_name: str,
    initial_queue_state: Dict[str, Dict[str, float]],
    subtract_initial_queue: bool,
    zones: List[str] | None = None,
) -> pd.DataFrame:
    """
    Subtract initial queue capacity from targets if subtractive mode is enabled.
    
    This function adjusts capacity targets by subtracting the initial queue capacity
    that's already in the pipeline. In subtractive mode, this ensures that total
    deployment (initial queue + new projects) matches the original targets exactly.
    
    Args:
        targets_df: DataFrame with columns TECH, YEAR, ZONE, add_capacity_MW
        tech_name: Technology name (e.g., "Solar", "Transmission")
        initial_queue_state: Dictionary mapping tech names to stage capacities
                           Format: {tech: {"Definition_MW": X, "Approvals_MW": Y, "Construction_MW": Z}}
        subtract_initial_queue: If True, subtract initial queue from targets. If False, return targets unchanged.
        zones: Optional list of zones. If provided, only subtract for these zones.
    
    Returns:
        DataFrame with adjusted targets (columns: TECH, YEAR, ZONE, add_capacity_MW)
    """
    # If not subtractive mode, return targets unchanged
    if not subtract_initial_queue:
        return targets_df.copy()
    
    # Get initial queue capacity for this technology
    if tech_name not in initial_queue_state:
        return targets_df.copy()
    
    queue_state = initial_queue_state[tech_name]
    # Sum all stages to get total initial queue capacity
    total_initial_queue_MW = (
        queue_state.get("Definition_MW", 0.0) +
        queue_state.get("Approvals_MW", 0.0) +
        queue_state.get("Construction_MW", 0.0)
    )
    
    # If no initial queue, return unchanged
    if total_initial_queue_MW <= 1e-6:
        return targets_df.copy()
    
    # Create a copy to avoid modifying original
    adjusted_df = targets_df.copy()
    
    # Calculate total target capacity
    total_target_MW = adjusted_df["add_capacity_MW"].sum()
    
    # Subtract initial queue proportionally across all years/zones
    # This distributes the subtraction across all target entries
    # Quick note: this is a “scale down” approach (preserves the year/zone shape).
    # It’s simple and stable, but it’s not saying the queue belongs to any specific year.
    if total_target_MW > 1e-6:
        # Calculate proportion to subtract from each row
        scale_factor = max(0.0, 1.0 - (total_initial_queue_MW / total_target_MW))
        adjusted_df["add_capacity_MW"] = adjusted_df["add_capacity_MW"] * scale_factor
        
        # Ensure no negative values
        adjusted_df["add_capacity_MW"] = adjusted_df["add_capacity_MW"].clip(lower=0.0)
        
        print(f"       Subtracted {total_initial_queue_MW:,.2f} MW initial queue from {total_target_MW:,.2f} MW targets")
        print(f"       Adjusted targets: {adjusted_df['add_capacity_MW'].sum():,.2f} MW")
    else:
        print(f"       Warning: Total targets ({total_target_MW:,.2f} MW) <= initial queue ({total_initial_queue_MW:,.2f} MW)")
        print(f"       Setting all targets to zero")
        adjusted_df["add_capacity_MW"] = 0.0
    
    return adjusted_df


def load_solar_targets(
    csv_path: Path,
    zones: List[str] | None = None,
    initial_queue_state: Dict[str, Dict[str, float]] | None = None,
    subtract_initial_queue: bool = True,
    run_name: str | None = None,
) -> pd.DataFrame:
    """
    Load solar capacity targets from CSV, optionally subtracting initial queue capacity.
    
    Args:
        csv_path: Path to the CSV file containing capacity data
        zones: Optional list of zones to filter by. If None, includes all zones.
        initial_queue_state: Optional dictionary mapping tech names to initial queue capacities.
                            Format: {tech: {"Definition_MW": X, "Approvals_MW": Y, "Construction_MW": Z}}
        subtract_initial_queue: If True and initial_queue_state provided, subtract initial queue from targets.
    
    Returns:
        DataFrame with columns: TECH, YEAR, ZONE, add_capacity_MW
    """
    print("\n" + "=" * 80)
    print("LOADING SOLAR TARGETS")
    print("=" * 80)
    targets_df = load_targets_from_capacity_subset(
        csv_path=csv_path,
        detailed_filters="large-scale solar pv",
        type_filter="fixed",
        tech_label="Solar",
        zones=zones,
        run_name=run_name,
        sequencing_sector="electricity",
    )
    
    # Subtract initial queue if provided and subtractive mode
    if initial_queue_state is not None:
        targets_df = subtract_initial_queue_from_targets(
            targets_df=targets_df,
            tech_name="Solar",
            initial_queue_state=initial_queue_state,
            subtract_initial_queue=subtract_initial_queue,
            zones=zones,
        )
    
    return targets_df


def load_gas_targets(
    csv_path: Path,
    zones: List[str] | None = None,
    initial_queue_state: Dict[str, Dict[str, float]] | None = None,
    subtract_initial_queue: bool = True,
    run_name: str | None = None,
) -> pd.DataFrame:
    """
    Load gas capacity targets from CSV (combines gas combustion turbine and gas combined cycle),
    optionally subtracting initial queue capacity.
    
    Args:
        csv_path: Path to the CSV file containing capacity data
        zones: Optional list of zones to filter by. If None, includes all zones.
        initial_queue_state: Optional dictionary mapping tech names to initial queue capacities.
        subtract_initial_queue: If True and initial_queue_state provided, subtract initial queue from targets.
    
    Returns:
        DataFrame with columns: TECH, YEAR, ZONE, add_capacity_MW
    """
    print("\n" + "=" * 80)
    print("LOADING GAS TARGETS")
    print("=" * 80)
    targets_df = load_targets_from_capacity_subset(
        csv_path=csv_path,
        detailed_filters=["gas combustion turbine", "gas combined cycle"],
        type_filter=None,  # Both are "thermal" but we want to include both regardless
        tech_label="Gas Turbine",
        zones=zones,
        run_name=run_name,
        sequencing_sector="electricity",
    )
    
    # Subtract initial queue if provided and subtractive mode
    if initial_queue_state is not None:
        targets_df = subtract_initial_queue_from_targets(
            targets_df=targets_df,
            tech_name="Gas Turbine",
            initial_queue_state=initial_queue_state,
            subtract_initial_queue=subtract_initial_queue,
            zones=zones,
        )
    
    return targets_df


def load_battery_targets(
    csv_path: Path,
    zones: List[str] | None = None,
    initial_queue_state: Dict[str, Dict[str, float]] | None = None,
    subtract_initial_queue: bool = True,
    run_name: str | None = None,
) -> pd.DataFrame:
    """
    Load battery storage capacity targets from CSV, optionally subtracting initial queue capacity.
    
    Args:
        csv_path: Path to the CSV file containing capacity data
        zones: Optional list of zones to filter by. If None, includes all zones.
        initial_queue_state: Optional dictionary mapping tech names to initial queue capacities.
        subtract_initial_queue: If True and initial_queue_state provided, subtract initial queue from targets.
    
    Returns:
        DataFrame with columns: TECH, YEAR, ZONE, add_capacity_MW
    """
    print("\n" + "=" * 80)
    print("LOADING BATTERY TARGETS")
    print("=" * 80)
    targets_df = load_targets_from_capacity_subset(
        csv_path=csv_path,
        detailed_filters="battery storage",
        type_filter="electric storage",
        tech_label="Battery",
        zones=zones,
        run_name=run_name,
        sequencing_sector="electricity",
    )
    
    # Subtract initial queue if provided and subtractive mode
    if initial_queue_state is not None:
        targets_df = subtract_initial_queue_from_targets(
            targets_df=targets_df,
            tech_name="Battery",
            initial_queue_state=initial_queue_state,
            subtract_initial_queue=subtract_initial_queue,
            zones=zones,
        )
    
    return targets_df


def load_transmission_targets(
    csv_path: Path,
    zones: List[str] | None = None,
    initial_queue_state: Dict[str, Dict[str, float]] | None = None,
    subtract_initial_queue: bool = True,
    run_name: str | None = None,
) -> pd.DataFrame:
    """
    Load transmission capacity targets directly from CSV.
    
    Note: Transmission entries have zones embedded in the "tech" column (format: "zone1||zone2||electricity||1").
    This function extracts zones from the tech column and includes transmission lines if either zone matches.
    Includes both "new" and "retrofit" types.
    
    Args:
        csv_path: Path to the CSV file containing capacity data
        zones: Optional list of zones to filter by. If None, includes all zones.
    
    Returns:
        DataFrame with columns: TECH, YEAR, ZONE, add_capacity_MW
    """
    print("\n" + "=" * 80)
    print("LOADING TRANSMISSION TARGETS")
    print("=" * 80)
    df = pd.read_csv(csv_path)
    print(f"       Loaded {len(df):,} total rows from CSV")

    # Optional: filter by run name
    if run_name is not None:
        if "run name" in df.columns:
            before = len(df)
            df = df[df["run name"].fillna("").astype(str).str.lower() == str(run_name).lower()].copy()
            print(f"       Filtered by run name='{run_name}': {before:,} -> {len(df):,} rows")
            if df.empty:
                raise ValueError(f"No rows matched run name='{run_name}' in {csv_path}")
        else:
            print(f"[WARN] run_name filter requested but CSV has no 'run name' column. Ignoring run_name='{run_name}'.")
    
    # Filter for electricity transmission using sequencing_sector column
    # Note: This uses an extra column, but it's necessary to identify transmission correctly
    if "sequencing_sector" not in df.columns:
        raise ValueError(
            "CSV is missing required column 'sequencing_sector'. "
            "To load Transmission directly from data (no deriving), use the schema of "
            "`capacity_delta_subset(in).csv`."
        )

    # Handle NaN values in sequencing_sector
    transmission_df = df[df["sequencing_sector"].fillna("").astype(str).str.lower() == "electricity transmission"].copy()
    print(f"       Found {len(transmission_df):,} rows with sequencing_sector='electricity transmission'")
    
    if transmission_df.empty:
        raise ValueError(f"No transmission rows found in {csv_path}")
    
    # Extract zones from the "tech" column (format: "zone1||zone2||electricity||1")
    # Split by "||" and take the first two parts as zones
    def extract_zones(tech_str):
        """Extract zones from tech column format: 'zone1||zone2||electricity||1'"""
        if pd.isna(tech_str) or not tech_str:
            return []
        parts = str(tech_str).split("||")
        if len(parts) >= 2:
            return [parts[0].strip(), parts[1].strip()]
        return []
    
    # Units note:
    # In capacity_delta_subset(in).csv, electricity transmission often uses unit='gigawatt_hour/hour',
    # which is dimensionally equivalent to GW. We treat these as "GW-equivalent" and convert to MW.
    tx_units = transmission_df.get("unit", pd.Series(dtype=str)).dropna().astype(str).str.lower().unique().tolist()
    allowed_units = {"gigawatt", "gigawatt_hour/hour"}
    if tx_units and not (set(tx_units) & allowed_units):
        print(f"[WARN] Unexpected Transmission units {tx_units}; expected one of {sorted(allowed_units)}. Proceeding anyway.")

    # Create a list of rows with extracted zones.
    # Allocation rule:
    # - If a line connects zoneA||zoneB, we allocate half the capacity to each zone (to avoid
    #   starving one endpoint and to keep totals consistent when summing across zones).
    # - If zones filter is provided and only one endpoint is in-scope, allocate 100% to that endpoint.
    rows_with_zones = []
    for _, row in transmission_df.iterrows():
        zones_in_tech = extract_zones(row["tech"])
        if zones_in_tech:
            v = float(row["value"]) if row.get("value") is not None else 0.0

            # If zones filter is specified, include if either zone matches
            if zones:
                zones_lower = {z.lower() for z in zones}
                in_scope = [z for z in zones_in_tech if z.lower() in zones_lower]
                if not in_scope:
                    continue

                if len(in_scope) == 1:
                    # Only one endpoint in-scope: allocate full capacity to that endpoint
                    rows_with_zones.append({"year": row["year"], "zone": in_scope[0], "value": v})
                else:
                    # Both endpoints in-scope: split evenly
                    rows_with_zones.append({"year": row["year"], "zone": in_scope[0], "value": v / 2.0})
                    rows_with_zones.append({"year": row["year"], "zone": in_scope[1], "value": v / 2.0})
            else:
                # No zone filter: split evenly across both endpoints
                rows_with_zones.append({"year": row["year"], "zone": zones_in_tech[0], "value": v / 2.0})
                rows_with_zones.append({"year": row["year"], "zone": zones_in_tech[1], "value": v / 2.0})
    
    if not rows_with_zones:
        raise ValueError(f"No transmission rows matched zones={zones}")
    
    # create a dataframe from the rows with zones, this will hold the capacity for each year and zone for transmission
    tech_df = pd.DataFrame(rows_with_zones)
    print(f"       Extracted zones from tech column -> {len(tech_df):,} rows")
    if zones:
        print(f"       Filtered to zones: {zones} -> {len(tech_df):,} rows")
    
    # Aggregate by year and zone (summing transmission capacity for each zone)
    agg = (
        tech_df.groupby(["year", "zone"], as_index=False)["value"].sum()
        .rename(columns={"value": "add_capacity_GW"})
    )
    agg["TECH"] = "Transmission"
    # Same unit conversion as the other techs: GW -> MW.
    agg["add_capacity_MW"] = agg["add_capacity_GW"] * 1000.0
    agg = agg.rename(columns={"year": "YEAR", "zone": "ZONE"})
    # sum the capacity for each year and zone
    total_capacity = agg["add_capacity_MW"].sum()
    print(f"       Total Transmission capacity loaded: {total_capacity:,.2f} MW across {len(agg)} year-zone combinations")
    print(f"       Years: {sorted(agg['YEAR'].unique())}")
    print(f"       Zones: {sorted(agg['ZONE'].unique())}")
    
    # Create targets DataFrame
    targets_df = agg[["TECH", "YEAR", "ZONE", "add_capacity_MW"]].copy()
    
    # Subtract initial queue if provided and subtractive mode
    if initial_queue_state is not None:
        targets_df = subtract_initial_queue_from_targets(
            targets_df=targets_df,
            tech_name="Transmission",
            initial_queue_state=initial_queue_state,
            subtract_initial_queue=subtract_initial_queue,
            zones=zones,
        )
    
    return targets_df


def derive_transmission_targets_from_solar(
    solar_targets: pd.DataFrame,
    params: dict,
) -> pd.DataFrame:
    """
    DEPRECATED: This function is kept for backward compatibility but should not be used... was when we didn't have the transmission 
    targets in the CSV
    Use load_transmission_targets() instead to load transmission directly from CSV.
    
    Derive transmission capacity targets from solar targets using the ratio.
    
    Args:
        solar_targets: DataFrame with solar capacity targets
        params: Dictionary containing interdependency parameters
    
    Returns:
        DataFrame with columns: TECH, YEAR, ZONE, add_capacity_MW
    """
    print("\n" + "=" * 80)
    print("DERIVING TRANSMISSION TARGETS FROM SOLAR (DEPRECATED)")
    print("=" * 80)
    print("[WARN] This function is deprecated. Use load_transmission_targets() instead.")
    transmission_targets = solar_targets.copy()
    ratio_s, ratio_tx = params["transmission"]["solar_to_transmission_ratio"]
    transmission_targets["add_capacity_MW"] *= ratio_tx / ratio_s
    transmission_targets["TECH"] = "Transmission"
    
    total_solar = solar_targets["add_capacity_MW"].sum()
    total_transmission = transmission_targets["add_capacity_MW"].sum()
    print(f"[INFO] Derived transmission from solar using ratio {ratio_s}:{ratio_tx}")
    print(f"       Solar capacity: {total_solar:,.2f} MW")
    print(f"       Transmission capacity: {total_transmission:,.2f} MW")
    
    return transmission_targets


# ============================================================================
# PROJECT GENERATION FUNCTIONS
# ============================================================================

def chunk_capacity_by_year(
    year_to_mw: Dict[int, float],
    chunk_size_mw: float,
    tech_label: str,
    zone: str = "all",
    durations_config: Dict[str, Dict[str, int]] | None = None,
    distributions_config: Dict[str, Dict] | None = None,
) -> pd.DataFrame:
    """
    Split annual capacity targets into representative projects of a given size.
    
    In plain terms: if the target says “600 MW of solar in 2032”, and the rep size
    is 200 MW, we create 3 solar “projects” for that year.
    
    Args:
        year_to_mw: Dictionary mapping year to capacity in MW
        chunk_size_mw: Size of each representative project in MW
        tech_label: Technology label (e.g., "Solar", "Transmission") to know the representative project size
        zone: Zone identifier for the projects
        durations_config: Fixed durations dictionary (for fallback if distributions disabled)
        distributions_config: Duration distribution parameters (for per-project sampling)
    
    Returns:
        DataFrame with columns: ID, TECH, YEAR, ZONE, Capacity, Definition_months, Approvals_months, Construction_months
        (Duration columns are added if distributions are enabled, otherwise omitted)
    """
    rows: List[Dict] = []
    total_capacity = sum(year_to_mw.values())
    
    # Track if we're using distributions (to determine if we need duration columns)
    use_distributions = (
        distributions_config is not None 
        and tech_label in distributions_config 
        and distributions_config[tech_label].get("enabled", False)
    )
    
    for year in sorted(year_to_mw.keys()):
        # Get the capacity for the current year
        remaining = float(year_to_mw[year])
        
        # Skip years with no capacity
        if remaining <= 0:
            continue
        
        k = 1
        while remaining > 1e-6:
            # Create a project with capacity up to chunk_size_mw
            cap = min(chunk_size_mw, remaining)
            
            # Create project ID: tech_zone_year_index
            pid = f"{tech_label.lower().replace(' ', '_')}_{zone}_{year}_{k:03d}"
            
            # Sample durations for this project if distributions are enabled
            project_row = {
                "ID": pid,
                "TECH": tech_label,
                "YEAR": int(year),
                "ZONE": zone,
                "Capacity": float(cap),
            }
            
            if use_distributions:
                # Sample durations using project ID as seed for reproducibility
                # This ensures the same project always gets the same sampled durations.
                project_seed = _stable_int_seed("project", pid)
                sampled_durations = sample_project_durations(
                    tech=tech_label,
                    durations_config=durations_config,
                    distributions_config=distributions_config,
                    random_seed=project_seed,
                )
                project_row.update(sampled_durations)
            else:
                # When distributions are disabled, attach fixed durations from config so the DAG
                # uses them (avoids relying on tech-level lookup, which must match the scenario).
                if durations_config and tech_label in durations_config:
                    td = durations_config[tech_label]
                    project_row["Definition_months"] = int(td.get("Definition_months", 0))
                    project_row["Approvals_months"] = int(td.get("Approvals_months", 0))
                    project_row["Construction_months"] = int(td.get("Construction_months", 0))
            
            # Add project to list
            rows.append(project_row)
            remaining -= cap
            k += 1
    
    num_projects = len(rows)
    print(f"       Created {num_projects} {tech_label} projects ({total_capacity:,.2f} MW total, {chunk_size_mw} MW each)")
    
    return pd.DataFrame(rows)


def generate_projects_multi_tech(
    solar_targets: pd.DataFrame,
    transmission_targets: pd.DataFrame,
    gas_targets: pd.DataFrame,
    battery_targets: pd.DataFrame,
    rep_sizes: Dict[str, float],
    durations_config: Dict[str, Dict[str, int]] | None = None,
    distributions_config: Dict[str, Dict] | None = None,
) -> pd.DataFrame:
    """
    Generate projects for multiple technologies from their capacity targets.
    This function inputs the target capacity for each technology and outputs
    the projects for each technology, based on the representative project size.
    The representative project size is a dictionary mapping technology names to representative project sizes (MW).
    The function will group the capacity targets by technology and zone and then generate the projects for each technology/zone.
    The function will then return a dataframe containing all the generated projects.

    Args:
        solar_targets: DataFrame with solar capacity targets
        transmission_targets: DataFrame with transmission capacity targets
        gas_targets: DataFrame with gas turbine capacity targets
        battery_targets: DataFrame with battery capacity targets
        rep_sizes: Dictionary mapping technology names to representative project sizes (MW)
        durations_config: Fixed durations dictionary (for fallback if distributions disabled)
        distributions_config: Duration distribution parameters (for per-project sampling)
    
    Returns:
        DataFrame containing all generated projects with columns: ID, TECH, YEAR, ZONE, Capacity
        (and optionally Definition_months, Approvals_months, Construction_months if distributions enabled)
    """
    print("\n" + "=" * 80)
    print("GENERATING PROJECTS FROM CAPACITY TARGETS")
    print("=" * 80)
    
    all_projects = []
    
    # Process each technology one by one. We keep this loop explicit so it’s
    # easy to comment out / swap in other techs later.
    for tech_name, df in {
        "Solar": solar_targets,
        "Transmission": transmission_targets,
        "Gas Turbine": gas_targets,
        "Battery": battery_targets,
    }.items():
        if df is None or df.empty:
            print(f"[INFO] Skipping {tech_name}: no targets provided")
            continue
        
        print(f"\n[INFO] Processing {tech_name} targets...")
        
        # Group by technology and zone
        for (tech, zone), grp in df.groupby(["TECH", "ZONE"], dropna=False):
            # Convert to year-to-MW dictionary
            year_to_mw = {int(r["YEAR"]): float(r["add_capacity_MW"]) for _, r in grp.iterrows()}
            
            # Get representative project size
            rep = float(rep_sizes.get(tech, rep_sizes.get(tech_name, 200.0)))
            print(f"       Zone: {zone}, Representative size: {rep} MW")
            
            # Generate projects for this technology/zone (with duration sampling if enabled)
            proj_df = chunk_capacity_by_year(
                year_to_mw, 
                rep, 
                tech_label=tech, 
                zone=zone,
                durations_config=durations_config,
                distributions_config=distributions_config,
            )
            all_projects.append(proj_df)
    
    if not all_projects:
        print("[WARN] No projects generated!")
        return pd.DataFrame(columns=["ID", "TECH", "YEAR", "ZONE", "Capacity"])
    
    result_df = pd.concat(all_projects, ignore_index=True)
    
    print("\n" + "=" * 80)
    print("PROJECT GENERATION SUMMARY")
    print("=" * 80)
    tech_counts = result_df["TECH"].value_counts()
    for tech, count in tech_counts.items():
        total_cap = result_df[result_df["TECH"] == tech]["Capacity"].sum()
        print(f"  {tech}: {count} projects, {total_cap:,.2f} MW total")
    print(f"\n  Total: {len(result_df)} projects")
    
    return result_df


def generate_initial_queue_projects(
    initial_queue_state: Dict[str, Dict[str, float]],
    durations_config: Dict[str, Dict[str, int]],
    rep_sizes: Dict[str, float],
    zones: List[str],
    base_year: int,
    distributions_config: Dict[str, Dict] | None = None,
    distribute_across_zones: bool = False,
    zone_weights: Dict[str, float] | None = None,
) -> pd.DataFrame:
    """
    Generate virtual projects for initial queue capacity already in the pipeline.
    
    This function creates projects that represent capacity already in Definition, Approvals,
    or Construction stages at the start of the model period. These projects will have
    ES/EF times set to reflect they're already in progress.
    
    The target year for each initial queue project is calculated as:
    YEAR = base_year + floor(EF / 12), where EF is the finish time after all stages complete.
    
    Args:
        initial_queue_state: Dictionary mapping tech names to stage capacities.
                            Format: {tech: {"Definition_MW": X, "Approvals_MW": Y, "Construction_MW": Z}}
        durations_config: Dictionary mapping tech names to stage durations.
                         Format: {tech: {"Definition_months": A, "Approvals_months": B, "Construction_months": C}}
        rep_sizes: Dictionary mapping tech names to representative project sizes (MW).
        zones: List of zones to create projects for.
        base_year: Base year for the model (earliest year).
        distributions_config: Duration distribution parameters (for per-project sampling)
        distribute_across_zones: If True and multiple zones are provided, distribute the initial
                                queue capacity across zones instead of duplicating it into each
                                zone. This avoids artificially inflating the initial queue when
                                running multi-zone scenarios.
        zone_weights: Optional weights for distributing initial queue across zones.
                      If provided, should map zone -> weight (non-negative). Unspecified zones
                      default to weight 0. If None, zones are weighted equally.
    
    Returns:
        DataFrame with columns: ID, TECH, YEAR, ZONE, Capacity, initial_stage
        (and optionally Definition_months, Approvals_months, Construction_months if distributions enabled)
        where initial_stage indicates which stage the project is currently in ("Definition", "Approvals", or "Construction")
    """
    print("\n" + "=" * 80)
    print("GENERATING INITIAL QUEUE PROJECTS")
    print("=" * 80)
    
    all_projects = []
    
    # Normalize/validate zone weights (only used when distributing across zones)
    zone_weights_norm: Dict[str, float] | None = None
    if distribute_across_zones:
        # Defensive: treat empty/None zones list as a single "all" bucket.
        zones_effective = zones if zones else ["all"]
        if zone_weights is None:
            zone_weights_norm = {z: 1.0 for z in zones_effective}
        else:
            zone_weights_norm = {z: float(max(0.0, zone_weights.get(z, 0.0))) for z in zones_effective}
            if sum(zone_weights_norm.values()) <= 1e-12:
                # Fall back to equal weights if user passed all zeros.
                zone_weights_norm = {z: 1.0 for z in zones_effective}

    # Process each technology
    for tech_name in ["Solar", "Transmission", "Gas Turbine", "Battery"]:
        if tech_name not in initial_queue_state:
            continue
        
        queue_state = initial_queue_state[tech_name]
        
        # Get durations for this technology
        if tech_name not in durations_config:
            print(f"       Skipping {tech_name} - no durations config found")
            continue
        
        tech_durations = durations_config[tech_name]
        # Extract durations needed for calculating finish times
        # Note: def_dur is not needed here - it's retrieved in set_initial_queue_es_ef() when setting ES/EF. this is just because it's the sum of app + const.
        app_dur = tech_durations.get("Approvals_months", 0)
        const_dur = tech_durations.get("Construction_months", 0)
        
        # Get representative project size
        rep_size = rep_sizes.get(tech_name, 100.0)
        
        # Process each stage that has initial queue capacity
        for stage_name, stage_key in [("Definition", "Definition_MW"), 
                                       ("Approvals", "Approvals_MW"), 
                                       ("Construction", "Construction_MW")]:
            initial_capacity_MW = queue_state.get(stage_key, 0.0)
            
            if initial_capacity_MW <= 1e-6:
                continue

            # Shared target-year calculation (same for all projects in this tech+stage)
            if stage_name == "Definition":
                ef_months = app_dur + const_dur
            elif stage_name == "Approvals":
                ef_months = const_dur
            else:
                ef_months = 0
            target_year = base_year + int(ef_months // 12)

            # Sample durations for this project if distributions are enabled
            use_distributions = (
                distributions_config is not None
                and tech_name in distributions_config
                and distributions_config[tech_name].get("enabled", False)
            )

            if distribute_across_zones and zone_weights_norm is not None and len(zones) > 1:
                # Distribute stage capacity across zones to avoid duplication.
                zones_effective = zones if zones else ["all"]
                total_w = float(sum(zone_weights_norm.get(z, 0.0) for z in zones_effective))
                zone_caps = {
                    z: (initial_capacity_MW * float(zone_weights_norm.get(z, 0.0)) / total_w)
                    for z in zones_effective
                }

                created_total = 0
                for zone in zones_effective:
                    remaining = float(zone_caps.get(zone, 0.0))
                    if remaining <= 1e-6:
                        continue
                    k = 1
                    while remaining > 1e-6:
                        cap = min(rep_size, remaining)
                        pid = f"{tech_name.lower().replace(' ', '_')}_initial_{stage_name.lower()}_{k:03d}"

                        sampled_durations = {}
                        if use_distributions:
                            # Seed incorporates zone to keep per-zone projects stable.
                            project_seed = _stable_int_seed("initial_queue_project", pid, zone)
                            sampled_durations = sample_project_durations(
                                tech=tech_name,
                                durations_config=durations_config,
                                distributions_config=distributions_config,
                                random_seed=project_seed,
                            )

                        project_row = {
                            "ID": f"{pid}_{zone}",
                            "TECH": tech_name,
                            "YEAR": target_year,
                            "ZONE": zone,
                            "Capacity": float(cap),
                            "initial_stage": stage_name,
                        }
                        if use_distributions:
                            project_row.update(sampled_durations)
                        all_projects.append(project_row)

                        remaining -= cap
                        k += 1
                        created_total += 1

                print(
                    f"       Created {created_total} {tech_name} projects in {stage_name} stage "
                    f"({initial_capacity_MW:,.2f} MW total, distributed across {len(zones_effective)} zones)"
                )
            else:
                # Legacy behavior: duplicate the same initial-queue capacity into each zone provided.
                remaining = float(initial_capacity_MW)
                k = 1
                while remaining > 1e-6:
                    cap = min(rep_size, remaining)
                    pid = f"{tech_name.lower().replace(' ', '_')}_initial_{stage_name.lower()}_{k:03d}"

                    sampled_durations = {}
                    if use_distributions:
                        project_seed = _stable_int_seed("initial_queue_project", pid)
                        sampled_durations = sample_project_durations(
                            tech=tech_name,
                            durations_config=durations_config,
                            distributions_config=distributions_config,
                            random_seed=project_seed,
                        )

                    for zone in zones:
                        project_row = {
                            "ID": f"{pid}_{zone}",
                            "TECH": tech_name,
                            "YEAR": target_year,
                            "ZONE": zone,
                            "Capacity": float(cap),
                            "initial_stage": stage_name,
                        }
                        if use_distributions:
                            project_row.update(sampled_durations)
                        all_projects.append(project_row)

                    remaining -= cap
                    k += 1

                print(
                    f"       Created {k-1} {tech_name} projects in {stage_name} stage "
                    f"({initial_capacity_MW:,.2f} MW total)"
                )
    
    if not all_projects:
        print("       No initial queue projects created (all values are zero)")
        return pd.DataFrame(columns=["ID", "TECH", "YEAR", "ZONE", "Capacity", "initial_stage"])
    
    result_df = pd.DataFrame(all_projects)
    total_cap = result_df["Capacity"].sum()
    print(f"\n       Total initial queue projects: {len(result_df)} projects, {total_cap:,.2f} MW")
    
    return result_df


def create_durations_dataframe(
    durations_config: Dict[str, Dict[str, int]] | None = None,
    params: dict | None = None,
) -> pd.DataFrame:
    """
    Create a durations DataFrame from a configuration dictionary.
    
    Args:
        durations_config: Dictionary mapping technology names to duration configs.
                         Each config should have: Definition_months, Approvals_months, Construction_months
                         If None, uses DEFAULT_DURATIONS
        params: Interdependency parameters dict (for transmission lead_time_months).
                If None, uses INTERDEPENDENCY_PARAMS
    
    Returns:
        DataFrame with columns: TECH, Definition_months, Approvals_months, Construction_months
    """
    if durations_config is None:
        durations_config = DEFAULT_DURATIONS.copy()
    
    if params is None:
        params = INTERDEPENDENCY_PARAMS
    
    rows = []
    for tech, durations in durations_config.items():
        tech_durations = durations.copy()
        
        # Special handling for Transmission - use lead_time_months from params
        if tech == "Transmission" and "Construction_months" in tech_durations:
            if tech_durations["Construction_months"] == DEFAULT_DURATIONS["Transmission"]["Construction_months"]:
                tech_durations["Construction_months"] = params["transmission"]["lead_time_months"]
        
        tech_durations["TECH"] = tech
        rows.append(tech_durations)
    
    return pd.DataFrame(rows)


def sample_project_durations(
    tech: str,
    durations_config: Dict[str, Dict[str, int]] | None = None,
    distributions_config: Dict[str, Dict] | None = None,
    random_seed: int | None = None,
) -> Dict[str, int]:
    """
    Sample stage durations for a single project from distributions.
    
    If distributions are disabled or not configured for this technology,
    returns fixed durations from durations_config.
    
    Args:
        tech: Technology name (e.g., "Solar", "Transmission")
        durations_config: Fixed durations dictionary (fallback if distributions disabled)
                         Format: {tech: {"Definition_months": X, "Approvals_months": Y, ...}}
        distributions_config: Distribution parameters (from DURATION_DISTRIBUTIONS)
                             Format: {tech: {"enabled": bool, "Definition_months": {...}, ...}}
        random_seed: Optional seed for reproducibility (uses project-specific seed if None)
    
    Returns:
        Dictionary with Definition_months, Approvals_months, Construction_months (as integers)
    """
    # Default to fixed durations if no configs provided
    if durations_config is None:
        durations_config = DEFAULT_DURATIONS.copy()
    
    # Get fixed durations as fallback
    fixed_durations = durations_config.get(tech, {})
    
    # Check if distributions are enabled for this technology
    if distributions_config is None:
        # No distribution config, then this method will return the regular fixed durations 
        return {
            "Definition_months": int(fixed_durations.get("Definition_months", 0)),
            "Approvals_months": int(fixed_durations.get("Approvals_months", 0)),
            "Construction_months": int(fixed_durations.get("Construction_months", 0)),
        }
    # this means that the distributions is enabled for this technology
    tech_dist = distributions_config.get(tech, {})
    if not tech_dist.get("enabled", False):
        # Distributions disabled for this tech, use fixed durations
        return {
            "Definition_months": int(fixed_durations.get("Definition_months", 0)),
            "Approvals_months": int(fixed_durations.get("Approvals_months", 0)),
            "Construction_months": int(fixed_durations.get("Construction_months", 0)),
        }
    
    # Set random seed if provided (for reproducibility)
    if random_seed is not None:
        # If you want “same inputs, same outputs”, set a seed.
        np.random.seed(random_seed)
    
    sampled_durations = {}
    
    # Sample duration for each stage. Use the scenario's fixed duration (durations_config)
    # as the distribution mean when set, so capital discipline (e.g. def multiplier) is
    # respected when both fixed durations and distributions are used (e.g. OAT capital_combo).
    for stage in ["Definition", "Approvals", "Construction"]:
        stage_key = f"{stage}_months"
        
        # Get the parameters for the distribution for this stage
        stage_dist = tech_dist.get(stage_key, {})
        # get the distribution type, ie normal, lognormal, or uniform
        dist_type = stage_dist.get("distribution", "normal").lower()
        
        fixed_dur = int(fixed_durations.get(stage_key, 0)) if fixed_durations else 0
        # Center the distribution on the scenario's nominal duration when provided
        dist_mean = stage_dist.get("mean", fixed_dur)
        mean = fixed_dur if (fixed_dur and fixed_dur > 0) else dist_mean
        
        if dist_type == "normal":
            std = stage_dist.get("std", max(1, mean * 0.1))  # Default 10% std if not specified
            sample = np.random.normal(mean, std)
        elif dist_type == "lognormal":
            std = stage_dist.get("std", max(1, mean * 0.1))
            # Convert to lognormal parameters (right-skewed, so long tails are possible).
            mu = np.log(mean) - 0.5 * np.log(1 + (std / mean) ** 2)
            sigma = np.sqrt(np.log(1 + (std / mean) ** 2))
            sample = np.random.lognormal(mu, sigma)
            # `sample` is the random draw from the lognormal distribution.
        elif dist_type == "uniform":
            min_val = stage_dist.get("min", max(1, fixed_dur * 0.5))
            max_val = stage_dist.get("max", fixed_dur * 1.5)
            sample = np.random.uniform(min_val, max_val)
        else:
            # Unknown distribution type, use fixed duration
            sample = fixed_dur
        
        # Apply bounds if specified
        min_bound = stage_dist.get("min", None)
        max_bound = stage_dist.get("max", None)
        if min_bound is not None:
            sample = max(sample, min_bound)
        if max_bound is not None:
            sample = min(sample, max_bound)
        
        # Ensure positive integer duration
        sampled_durations[stage_key] = int(max(1, round(sample)))
    
    return sampled_durations

def build_durations_with_commissioning(base_durations: pd.DataFrame) -> pd.DataFrame:
    """
    Add commissioning stage to project durations.
    
    Commissioning has zero duration and is used to enable joint commissioning nodes
    for coordinating completion of interdependent projects.
    
    We add it to all techs just to keep the DAG structure consistent (same stages everywhere),
    but the “joint commissioning” trick is mainly there for Solar+Batteries.
    
    Args:
        base_durations: DataFrame with columns TECH, Definition_months, Approvals_months, Construction_months
    
    Returns:
        DataFrame with added Commissioning_months column (set to 0)
    """
    # Copy so we don’t mutate the caller’s DataFrame.
    out = base_durations.copy()
    # Commissioning is a 0-month placeholder stage.
    out["Commissioning_months"] = 0
    return out


# ============================================================================
# DAG BUILDING FUNCTIONS
# ============================================================================

def split_fraction(ids: List[str], fraction_first: float) -> Tuple[List[str], List[str]]:
    """
    Split a list of IDs into two groups by fraction, with deterministic shuffling.
    
    Used to split solar projects into those that can proceed at FID vs completion
    of transmission projects.
    
    Args:
        ids: List of project IDs to split
        fraction_first: Fraction of IDs to put in first group (0.0 to 1.0)
    
    Returns:
        Tuple of (first_group_ids, second_group_ids)
    """
    n_first = int(round(len(ids) * fraction_first))
    ids_shuffled = sorted(ids)  # Deterministic sort
    
    # Shuffle using a seed based on the IDs themselves for reproducibility.
    # Same IDs in => same split out (but not just “first N IDs” every time).
    random.seed(_stable_int_seed("split_fraction", *ids_shuffled))
    ids_shuffled = ids_shuffled.copy()
    random.shuffle(ids_shuffled)
    
    return ids_shuffled[:n_first], ids_shuffled[n_first:]


def set_initial_queue_es_ef(
    G: nx.DiGraph,
    pid: str,
    initial_stage: str,
    durations_df: pd.DataFrame,
) -> None:
    """
    Set ES/EF times for initial queue projects to reflect they're already in progress.
    
    This implements Option B: projects start earlier to reflect prior progress.
    For a project in stage S:
    - Stages before S: Already completed (ES = negative, EF = negative, finishing before base_year)
    - Stage S: Currently in progress (ES = negative, EF = 0, finishing at base_year)
    - Stages after S: Not started yet (ES = 0, EF = duration, normal forward pass)
    
    Args:
        G: Directed acyclic graph
        pid: Project ID
        initial_stage: Which stage the project is currently in ("Definition", "Approvals", or "Construction")
        durations_df: DataFrame with stage durations for each technology
    """
    # Get project technology from node data
    tech = G.nodes[(pid, "Definition")].get("TECH", "")
    if not tech:
        return
    
    # Durations can be project-specific (if you enabled distributions) or tech-level defaults.
    # We try node attributes first, then fall back to the durations table.
    def_dur = int(G.nodes.get((pid, "Definition"), {}).get("duration", 0))
    app_dur = int(G.nodes.get((pid, "Approvals"), {}).get("duration", 0))
    const_dur = int(G.nodes.get((pid, "Construction"), {}).get("duration", 0))
    
    # If durations are zero (not set yet), fall back to tech-level durations
    if def_dur == 0 and app_dur == 0 and const_dur == 0:
        tech_durations = durations_df[durations_df["TECH"] == tech]
        if tech_durations.empty:
            return
        tech_row = tech_durations.iloc[0]
        def_dur = int(tech_row.get("Definition_months", 0))
        app_dur = int(tech_row.get("Approvals_months", 0))
        const_dur = int(tech_row.get("Construction_months", 0))
    
    # Calculate ES/EF for each stage based on initial_stage
    if initial_stage == "Definition":
        # Definition stage: Definition is in progress, Approvals/Construction not started
        # Definition: ES = -def_dur, EF = 0 (finishes at base_year)
        # Negative ES means “this started before the model base year”.
        G.nodes[(pid, "Definition")]["ES"] = float(-def_dur)
        G.nodes[(pid, "Definition")]["EF"] = 0.0
        
        # Approvals: Starts when Definition finishes, normal duration
        G.nodes[(pid, "Approvals")]["ES"] = 0.0
        G.nodes[(pid, "Approvals")]["EF"] = float(app_dur)
        
        # Construction: Starts when Approvals finishes, normal duration
        G.nodes[(pid, "Construction")]["ES"] = float(app_dur)
        G.nodes[(pid, "Construction")]["EF"] = float(app_dur + const_dur)
        
        # Commissioning: Starts when Construction finishes, zero duration
        G.nodes[(pid, "Commissioning")]["ES"] = float(app_dur + const_dur)
        G.nodes[(pid, "Commissioning")]["EF"] = float(app_dur + const_dur)
        
    elif initial_stage == "Approvals":
        # Approvals stage: Definition completed, Approvals in progress, Construction not started
        # Definition: Already completed (ES = -def_dur - app_dur, EF = -app_dur)
        G.nodes[(pid, "Definition")]["ES"] = float(-def_dur - app_dur)
        G.nodes[(pid, "Definition")]["EF"] = float(-app_dur)
        
        # Approvals: ES = -app_dur, EF = 0 (finishes at base_year)
        G.nodes[(pid, "Approvals")]["ES"] = float(-app_dur)
        G.nodes[(pid, "Approvals")]["EF"] = 0.0
        
        # Construction: Starts when Approvals finishes, normal duration
        G.nodes[(pid, "Construction")]["ES"] = 0.0
        G.nodes[(pid, "Construction")]["EF"] = float(const_dur)
        
        # Commissioning: Starts when Construction finishes, zero duration
        G.nodes[(pid, "Commissioning")]["ES"] = float(const_dur)
        G.nodes[(pid, "Commissioning")]["EF"] = float(const_dur)
        
    elif initial_stage == "Construction":
        # Construction stage: Definition and Approvals completed, Construction in progress
        # Definition: Already completed (ES = -def_dur - app_dur - const_dur, EF = -app_dur - const_dur)
        G.nodes[(pid, "Definition")]["ES"] = float(-def_dur - app_dur - const_dur)
        G.nodes[(pid, "Definition")]["EF"] = float(-app_dur - const_dur)
        
        # Approvals: Already completed (ES = -app_dur - const_dur, EF = -const_dur)
        G.nodes[(pid, "Approvals")]["ES"] = float(-app_dur - const_dur)
        G.nodes[(pid, "Approvals")]["EF"] = float(-const_dur)
        
        # Construction: ES = -const_dur, EF = 0 (finishes at base_year)
        G.nodes[(pid, "Construction")]["ES"] = float(-const_dur)
        G.nodes[(pid, "Construction")]["EF"] = 0.0
        
        # Commissioning: Starts when Construction finishes, zero duration 
        G.nodes[(pid, "Commissioning")]["ES"] = 0.0
        G.nodes[(pid, "Commissioning")]["EF"] = 0.0


def build_system_dag_sequencing(
    projects_df: pd.DataFrame,
    durations_df: pd.DataFrame,
) -> Tuple[nx.DiGraph, int]:
    """
    Build a directed acyclic graph (DAG) representing projects WITHOUT interdependencies.
    
    This is the "sequencing" model - it only includes intra-project sequencing edges
    (Definition → Approvals → Construction → Commissioning). No cross-project dependencies.
    
    Args:
        projects_df: DataFrame with project information (ID, TECH, YEAR, ZONE, Capacity)
        durations_df: DataFrame with stage durations for each technology
    
    Returns:
        Tuple of (DAG graph, base_year)
    """
    # Build duration lookup dictionary
    dur_lookup = {}
    for _, r in durations_df.iterrows():
        tech = r["TECH"]
        for s in STAGES4:
            dur_lookup[(tech, s)] = int(r.get(f"{s}_months", 0))

    # Get base year (earliest year in projects)
    # We measure time in “months from base_year”, so this sets t=0.
    base_year = int(projects_df["YEAR"].min())
    print(f"\n[INFO] Building sequencing DAG (no interdependencies) with base year: {base_year}")
    G = nx.DiGraph()

    # Add nodes and intra-project edges only
    print("[INFO] Adding project nodes and intra-project edges...")
    for _, p in projects_df.iterrows():
        pid = str(p["ID"])
        tech = str(p["TECH"])
        year = int(p["YEAR"])
        zone = str(p.get("ZONE", "all"))
        cap = float(p["Capacity"])
        
        # Check if this is an initial queue project (has "initial_stage" column)
        initial_stage = p.get("initial_stage", None)
        is_initial_queue = initial_stage is not None and initial_stage in ["Definition", "Approvals", "Construction"]
        
        # One node per stage per project. Nodes store durations and CPM timing fields (ES/EF).
        for s in STAGES4:
            # Check if this project has per-project durations (from distributions)
            # If so, use those; otherwise fall back to tech-level durations
            if f"{s}_months" in p and pd.notna(p.get(f"{s}_months", None)):
                # Use per-project duration
                node_duration = int(p[f"{s}_months"])
            else:
                # Fall back to tech-level duration lookup
                node_duration = dur_lookup.get((tech, s), 0)
            
            G.add_node(
                (pid, s),
                project_id=pid,
                stage=s,
                TECH=tech,
                YEAR=year,
                ZONE=zone,
                Capacity=cap,
                duration=node_duration,
                ES=0.0,  # Early Start (will be calculated by CPM or set for initial queue)
                EF=0.0,  # Early Finish (will be calculated by CPM or set for initial queue)
            )
        
        # If this is an initial queue project, set ES/EF to reflect it's already in progress
        if is_initial_queue:
            set_initial_queue_es_ef(G, pid, initial_stage, durations_df)
        
        # Simple within-project sequencing edges (Definition -> Approvals -> Construction -> Commissioning).
        for a, b in zip(STAGES4[:-1], STAGES4[1:]):
            w = G.nodes[(pid, a)]["duration"]
            G.add_edge((pid, a), (pid, b), weight=w)
    
    num_nodes = len(G.nodes)
    num_edges = len(G.edges)
    print(f"       Added {num_nodes:,} nodes and {num_edges:,} intra-project edges")
    print(f"[INFO] Technologies in DAG: {sorted(projects_df['TECH'].unique())}")
    
    return G, base_year


def build_system_dag_with_interdeps(
    projects_df: pd.DataFrame,
    durations_df: pd.DataFrame,
    params: dict,
) -> Tuple[nx.DiGraph, int]:
    """
    Build a directed acyclic graph (DAG) representing projects with interdependencies.
    
    Creates nodes for each project stage and adds edges for:
    1. Intra-project sequencing (Definition → Approvals → Construction → Commissioning)
    2. Transmission gating for Solar (split into FID and completion groups)
    3. Gas turbine support for Solar
    4. Battery co-commissioning with Solar (via joint commissioning nodes)
    
    Args:
        projects_df: DataFrame with project information (ID, TECH, YEAR, ZONE, Capacity)
        durations_df: DataFrame with stage durations for each technology
        params: Dictionary of interdependency parameters
    
    Returns:
        Tuple of (DAG graph, base_year)
    """
    # Build duration lookup dictionary
    dur_lookup = {}
    for _, r in durations_df.iterrows():
        tech = r["TECH"]
        for s in STAGES4:
            dur_lookup[(tech, s)] = int(r.get(f"{s}_months", 0))

    # Get base year (earliest year in projects)
    # Everything is timed as “months since base_year”.
    base_year = int(projects_df["YEAR"].min())
    print(f"\n[INFO] Building DAG with base year: {base_year}")
    G = nx.DiGraph()

    # ========================================================================
    # Add nodes and intra-project edges
    # ========================================================================
    print("[INFO] Adding project nodes and intra-project edges...")
    num_nodes_before = 0
    for _, p in projects_df.iterrows():
        pid = str(p["ID"])
        tech = str(p["TECH"])
        year = int(p["YEAR"])
        zone = str(p.get("ZONE", "all"))
        cap = float(p["Capacity"])
        
        # Check if this is an initial queue project (has "initial_stage" column)
        initial_stage = p.get("initial_stage", None)
        is_initial_queue = initial_stage is not None and initial_stage in ["Definition", "Approvals", "Construction"]
        
        # Add node for each stage
        for s in STAGES4:
            # Check if this project has per-project durations (from distributions)
            # If so, use those; otherwise fall back to tech-level durations
            if f"{s}_months" in p and pd.notna(p.get(f"{s}_months", None)):
                # Use per-project duration
                node_duration = int(p[f"{s}_months"])
            else:
                # Fall back to tech-level duration lookup
                node_duration = dur_lookup.get((tech, s), 0)
            
            G.add_node(
                (pid, s),
                project_id=pid,
                stage=s,
                TECH=tech,
                YEAR=year,
                ZONE=zone,
                Capacity=cap,
                duration=node_duration,
                ES=0.0,  # Early Start (will be calculated by CPM or set for initial queue)
                EF=0.0,  # Early Finish (will be calculated by CPM or set for initial queue)
            )
        
        # If this is an initial queue project, set ES/EF to reflect it's already in progress
        if is_initial_queue:
            set_initial_queue_es_ef(G, pid, initial_stage, durations_df)
        
        # Within-project sequencing always exists, even when we add cross-project dependencies.
        for a, b in zip(STAGES4[:-1], STAGES4[1:]):
            w = G.nodes[(pid, a)]["duration"]
            G.add_edge((pid, a), (pid, b), weight=w)
    
    num_nodes_after_intra = len(G.nodes)
    num_edges_after_intra = len(G.edges)
    print(f"       Added {num_nodes_after_intra:,} nodes and {num_edges_after_intra:,} intra-project edges")

    # Group projects by technology
    by_tech = {t: projects_df.loc[projects_df["TECH"] == t].copy() for t in projects_df["TECH"].unique()}
    print(f"[INFO] Technologies in DAG: {list(by_tech.keys())}")

    # Matching policy knobs (see INTERDEPENDENCY_PARAMS["matching"])
    allow_unmatched_solar = bool(params.get("matching", {}).get("allow_unmatched_solar", True))
    killed_solar_ids: set[str] = set()

    def _kill_solar_project(pid: str, reason: str) -> None:
        """
        Mark a Solar project as non-deploying due to missing supporting capacity.
        We use the existing `attrited` flag so exports naturally drop it from deployment.
        """
        killed_solar_ids.add(pid)
        for stage in STAGES4:
            n = (pid, stage)
            if n in G.nodes:
                G.nodes[n]["attrited"] = True
                G.nodes[n]["unmatched_kill"] = True
                G.nodes[n]["unmatched_kill_reason"] = reason

    # ========================================================================
    # Transmission gating for Solar
    # ========================================================================
    # Solar projects are split into two groups, drawing edges:
    # - FID group: Can start Construction when Transmission reaches Approvals (FID)
    # - Completion group: Must wait until Transmission Construction completes
    if "Solar" in by_tech and "Transmission" in by_tech:
        print("\n[INFO] Adding Transmission gating dependencies for Solar...")
        solar_df = by_tech["Solar"].copy()
        tx_df = by_tech["Transmission"].copy()
        
        # Get parameters
        f_fid = float(params["transmission"]["solar_fraction_proceed_at_FID"])
        ratio_s, ratio_tx = params["transmission"]["solar_to_transmission_ratio"]
        ratio = float(ratio_tx / ratio_s)  # Transmission needed per MW of Solar

        # Split solar projects into FID and completion groups
        solar_ids = solar_df["ID"].astype(str).tolist()
        solar_fid_ids, solar_comp_ids = split_fraction(solar_ids, f_fid)

        # Matching strategy (important):
        # We do a simple greedy “consume capacity in order” match within each zone.
        # It’s not an optimizer — just deterministic bookkeeping that creates edges.
        for zone, s_grp in solar_df.groupby("ZONE"):
            t_grp = tx_df.loc[tx_df["ZONE"] == zone]
            if t_grp.empty:
                if not allow_unmatched_solar:
                    for s_row in s_grp.to_dict("records"):
                        s_id = str(s_row["ID"])
                        _kill_solar_project(s_id, reason="unmatched_transmission_no_tx_in_zone")
                continue
            
            # Sort for deterministic matching
            s_list = s_grp.sort_values(["YEAR", "ID"]).to_dict("records")
            t_list = t_grp.sort_values(["YEAR", "ID"]).to_dict("records")
            
            t_idx, t_rem = 0, (t_list[0]["Capacity"] if t_list else 0.0)
            
            # Match each solar project to (one or more) transmission projects until the ratio is satisfied.
            for s_row in s_list:
                s_id = str(s_row["ID"])
                if s_id in killed_solar_ids:
                    continue
                
                # Skip dependencies for initial queue projects already in Construction
                # Projects in Definition or Approvals will still get matched to transmission
                initial_stage = s_row.get("initial_stage", None)
                if initial_stage == "Construction":
                    continue  # Skip this project - it's already past the dependency point
                
                s_need_tx = s_row["Capacity"] * ratio
                used_tx_indices = []
                # Local copies so we only consume TX capacity if the match succeeds.
                t_idx_local = t_idx
                t_rem_local = t_rem
                
                # Consume transmission capacity
                while s_need_tx > 1e-6 and t_idx_local < len(t_list):
                    take = min(s_need_tx, t_rem_local)
                    if take > 0 and t_idx_local not in used_tx_indices:
                        used_tx_indices.append(t_idx_local)
                    s_need_tx -= take
                    t_rem_local -= take
                    
                    if t_rem_local <= 1e-6:
                        t_idx_local += 1
                        if t_idx_local < len(t_list):
                            t_rem_local = t_list[t_idx_local]["Capacity"]
                
                # Only create dependency edges if solar project was fully matched to transmission
                # (i.e., s_need_tx <= 1e-6 means all required transmission capacity was found)
                if s_need_tx <= 1e-6 and len(used_tx_indices) > 0:
                    # Commit TX consumption only on successful match
                    t_idx, t_rem = t_idx_local, t_rem_local
                    tx_ids_used = [str(t_list[k]["ID"]) for k in used_tx_indices]
                    
                    # Add edges based on which group the solar project is in
                    if s_id in solar_fid_ids:
                        # FID group: can start when Transmission reaches Approvals
                        for tx_id in tx_ids_used:
                            G.add_edge(
                                (tx_id, "Approvals"),
                                (s_id, "Construction"),
                                weight=G.nodes[(tx_id, "Approvals")]["duration"]
                            )
                    else:
                        # Completion group: must wait until Transmission Construction completes
                        for tx_id in tx_ids_used:
                            G.add_edge(
                                (tx_id, "Construction"),
                                (s_id, "Construction"),
                                weight=G.nodes[(tx_id, "Construction")]["duration"]
                            )
                else:
                    # Unmatched TX: proceed unconstrained by default, or kill if configured.
                    if not allow_unmatched_solar:
                        _kill_solar_project(s_id, reason="unmatched_transmission_insufficient_capacity")
        
        tx_edges = len([e for e in G.edges if G.nodes[e[0]].get("TECH") == "Transmission" and G.nodes[e[1]].get("TECH") == "Solar"])
        print(f"       Added {tx_edges} transmission-to-solar edges")

    # ========================================================================
    # Gas turbine support for Solar
    # ========================================================================
    # Solar Construction depends on matched Gas Construction completing
    if "Solar" in by_tech and "Gas Turbine" in by_tech:
        print("\n[INFO] Adding Gas turbine support dependencies for Solar...")
        solar_df = by_tech["Solar"].copy()
        gas_df = by_tech["Gas Turbine"].copy()
        
        ratio_s, ratio_g = params["gas"]["solar_to_gas_ratio"]
        ratio = float(ratio_g / ratio_s)  # Gas needed per MW of Solar

        # Same idea as transmission: greedy capacity matching within each zone.
        for zone, s_grp in solar_df.groupby("ZONE"):
            g_grp = gas_df.loc[gas_df["ZONE"] == zone]
            if g_grp.empty:
                if not allow_unmatched_solar:
                    for s_row in s_grp.to_dict("records"):
                        s_id = str(s_row["ID"])
                        if s_id not in killed_solar_ids:
                            _kill_solar_project(s_id, reason="unmatched_gas_no_gas_in_zone")
                continue
            
            s_list = s_grp.sort_values(["YEAR", "ID"]).to_dict("records")
            g_list = g_grp.sort_values(["YEAR", "ID"]).to_dict("records")
            
            g_idx, g_rem = 0, (g_list[0]["Capacity"] if g_list else 0.0)
            
            # Match each solar project to gas projects
            for s_row in s_list:
                s_id = str(s_row["ID"])
                if s_id in killed_solar_ids:
                    continue
                
                # Skip dependencies for initial queue projects already in Construction
                # Projects in Definition or Approvals will still get matched to gas
                initial_stage = s_row.get("initial_stage", None)
                if initial_stage == "Construction":
                    continue  # Skip this project - it's already past the dependency point
                
                s_need = s_row["Capacity"] * ratio
                used_gas_indices = []
                g_idx_local = g_idx
                g_rem_local = g_rem
                
                # Consume gas capacity
                while s_need > 1e-6 and g_idx_local < len(g_list):
                    take = min(s_need, g_rem_local)
                    if take > 0 and g_idx_local not in used_gas_indices:
                        used_gas_indices.append(g_idx_local)
                    s_need -= take
                    g_rem_local -= take
                    
                    if g_rem_local <= 1e-6:
                        g_idx_local += 1
                        if g_idx_local < len(g_list):
                            g_rem_local = g_list[g_idx_local]["Capacity"]
                
                # Only create dependency edges if solar project was fully matched to gas
                # (i.e., s_need <= 1e-6 means all required gas capacity was found)
                if s_need <= 1e-6 and len(used_gas_indices) > 0:
                    # Add edge for each gas project that was used
                    for gas_idx in used_gas_indices:
                        gas_id = str(g_list[gas_idx]["ID"])
                        G.add_edge(
                            (gas_id, "Construction"),
                            (s_id, "Construction"),
                            weight=G.nodes[(gas_id, "Construction")]["duration"]
                        )
                    # Update global gas tracking to reflect consumed capacity
                    g_idx = g_idx_local
                    g_rem = g_rem_local
                else:
                    # Unmatched Gas: proceed unconstrained by default, or kill if configured.
                    if not allow_unmatched_solar:
                        _kill_solar_project(s_id, reason="unmatched_gas_insufficient_capacity")
        
        gas_edges = len([e for e in G.edges if G.nodes[e[0]].get("TECH") == "Gas Turbine" and G.nodes[e[1]].get("TECH") == "Solar"])
        print(f"       Added {gas_edges} gas-to-solar edges")

    # ========================================================================
    # Battery co-commissioning with Solar
    # ========================================================================
    # Solar cannot complete until matched Battery projects complete Construction
    # Enforced via zero-duration joint commissioning nodes
    if "Solar" in by_tech and "Battery" in by_tech:
        print("\n[INFO] Adding Battery co-commissioning dependencies for Solar...")
        solar_df = by_tech["Solar"].copy()
        bat_df = by_tech["Battery"].copy()
        
        ratio_s, ratio_b = params["battery"]["solar_to_battery_ratio"]
        ratio = float(ratio_b / ratio_s)  # Battery needed per MW of Solar

        # Battery matching is also greedy by capacity within each zone.
        for zone, s_grp in solar_df.groupby("ZONE"):
            b_grp = bat_df.loc[bat_df["ZONE"] == zone]
            if b_grp.empty:
                if not allow_unmatched_solar:
                    for s_row in s_grp.to_dict("records"):
                        s_id = str(s_row["ID"])
                        if s_id not in killed_solar_ids:
                            _kill_solar_project(s_id, reason="unmatched_battery_no_battery_in_zone")
                continue
            
            s_list = s_grp.sort_values(["YEAR", "ID"]).to_dict("records")
            b_list = b_grp.sort_values(["YEAR", "ID"]).to_dict("records")
            
            b_idx, b_rem = 0, (b_list[0]["Capacity"] if b_list else 0.0)
            
            # Match each solar project to battery projects
            for s_row in s_list:
                s_id = str(s_row["ID"])
                if s_id in killed_solar_ids:
                    continue
                
                # Skip dependencies for initial queue projects already in Construction
                # These projects are already past the point where they need battery co-commissioning
                # (Battery co-commissioning happens at Commissioning, but if Construction is already done,
                #  the project can proceed independently)
                initial_stage = s_row.get("initial_stage", None)
                if initial_stage == "Construction":
                    continue  # Skip this project - it's already past the dependency point
                
                s_need = s_row["Capacity"] * ratio
                used_indices = []
                # Local copies so we only consume battery capacity if the match succeeds.
                b_idx_local = b_idx
                b_rem_local = b_rem
                
                # Consume battery capacity
                while s_need > 1e-6 and b_idx_local < len(b_list):
                    take = min(s_need, b_rem_local)
                    if take > 0 and b_idx_local not in used_indices:
                        used_indices.append(b_idx_local)
                    s_need -= take
                    b_rem_local -= take
                    
                    if b_rem_local <= 1e-6:
                        b_idx_local += 1
                        if b_idx_local < len(b_list):
                            b_rem_local = b_list[b_idx_local]["Capacity"]
                
                # Only create joint commissioning node if solar project was fully matched to battery
                # (i.e., s_need <= 1e-6 means all required battery capacity was found)
                if s_need <= 1e-6 and len(used_indices) > 0:
                    # Commit battery consumption only on successful match
                    b_idx, b_rem = b_idx_local, b_rem_local
                    battery_ids_used = [str(b_list[k]["ID"]) for k in used_indices if 0 <= k < len(b_list)]
                    
                    # Joint commissioning node:
                    # This is the trick that enforces “solar can’t finish until battery construction is done”.
                    battery_suffix = "+".join(bid.split("_")[-1] for bid in battery_ids_used)
                    joint_project_id = f"{s_id}::Joint[{battery_suffix}]" if battery_suffix else f"{s_id}::Joint"
                    joint_node = (joint_project_id, "Commissioning")
                    
                    # Add joint commissioning node
                    G.add_node(
                        joint_node,
                        project_id=joint_project_id,
                        stage="Commissioning",
                        TECH="Joint",
                        YEAR=s_row["YEAR"],
                        ZONE=s_row["ZONE"],
                        Capacity=0.0,
                        duration=0,
                        ES=0.0,
                        EF=0.0,
                    )
                    
                    # Add edges: Solar Construction → Joint Commissioning
                    G.add_edge((s_id, "Construction"), joint_node, weight=0)
                    
                    # Add edges: Battery Construction → Joint Commissioning
                    for bid in battery_ids_used:
                        G.add_edge((bid, "Construction"), joint_node, weight=0)

                    # CRITICAL: Gate Solar commissioning on the joint node.
                    # Without this, the joint node exists but does not actually constrain Solar completion,
                    # because Solar still has its normal intra-project edge (Construction → Commissioning).
                    # By adding Joint → Solar(Commissioning), Solar commissioning waits for BOTH:
                    #   - Solar Construction (already a predecessor)
                    #   - All matched Battery Construction (via the joint node)
                    G.add_edge(joint_node, (s_id, "Commissioning"), weight=0)
                else:
                    # Unmatched Battery: proceed unconstrained by default, or kill if configured.
                    if not allow_unmatched_solar:
                        _kill_solar_project(s_id, reason="unmatched_battery_insufficient_capacity")
        
        joint_nodes = len([n for n in G.nodes if G.nodes[n].get("TECH") == "Joint"])
        battery_edges = len([e for e in G.edges if G.nodes[e[0]].get("TECH") == "Battery"])
        print(f"       Created {joint_nodes} joint commissioning nodes")
        print(f"       Added {battery_edges} battery-to-joint edges")

    print(f"\n[INFO] DAG construction complete:")
    print(f"       Total nodes: {len(G.nodes):,}")
    print(f"       Total edges: {len(G.edges):,}")
    
    return G, base_year


# ============================================================================
# CRITICAL PATH METHOD (CPM) FUNCTIONS
# ============================================================================

def cpm_forward_with_seeds(G: nx.DiGraph, base_year: int) -> nx.DiGraph:
    """
    Run forward pass of Critical Path Method (CPM) with year-floor constraints.

    Seeding (when work starts):
        We back-seed using STANDARD (default) durations per technology only — not actual
        durations. So: start_def = required_finish - total_standard_dur, where
        required_finish = (YEAR - base_year)*12 and total_standard_dur comes from
        DEFAULT_DURATIONS for that project's tech. Thus capital discipline (longer
        definition) and duration distributions push project FINISH (EF) past the target
        year and show up in deployment trajectories and delay metrics.

    Year-floor:
        We enforce that no project commissions before its target year: Commissioning
        EF = max(computed_EF, required_finish). To change or remove this, set
        YEAR_FLOOR_ENABLED = False in this function (search for "YEAR_FLOOR").

    Args:
        G: Directed acyclic graph with project nodes
        base_year: Base year for time calculations (earliest year in projects)

    Returns:
        Graph with ES (Early Start) and EF (Early Finish) times calculated for all nodes
    """
    print("\n" + "=" * 80)
    print("RUNNING CRITICAL PATH METHOD (CPM)")
    print("=" * 80)
    
    # Get all unique project IDs
    projects = sorted({G.nodes[n]["project_id"] for n in G.nodes})
    print(f"[INFO] Processing {len(projects):,} projects")

    # Standard (default) durations per tech for SEEDING only. Actual node durations
    # still come from the scenario (capital discipline, distributions, etc.).
    _def = "Definition_months"
    _app = "Approvals_months"
    _const = "Construction_months"
    standard_total_dur_by_tech = {}
    for tech, d in DEFAULT_DURATIONS.items():
        def_m = int(d.get(_def, 0))
        app_m = int(d.get(_app, 0))
        const_m = int(d.get(_const, 0))
        standard_total_dur_by_tech[tech] = def_m + app_m + const_m

    # Target year and tech per project (for seeding and year-floor)
    year_by_pid = {}
    tech_by_pid = {}
    for pid in projects:
        py = None
        pt = None
        for s in STAGES4:
            n = (pid, s)
            if n in G.nodes:
                py = G.nodes[n].get("YEAR", None) or py
                pt = G.nodes[n].get("TECH", None) or pt
        year_by_pid[pid] = py if py is not None else base_year
        tech_by_pid[pid] = pt if pt is not None else "Solar"
    
    # Seeding step:
    # For “new” projects, we back-seed all stages so that (unconstrained) commissioning
    # lands exactly at required_finish = (YEAR - base_year) * 12 months.
    # For initial-queue projects, we do NOT do that — they already have ES/EF set to reflect
    # “work started before base_year”.
    for pid in projects:
        # Check if this is an initial queue project (has negative ES already set)
        dn = (pid, "Definition")
        if dn in G.nodes:
            existing_es = G.nodes[dn].get("ES", 0.0)
            # If ES is already set to a negative value, this is an initial queue project
            # Don't override its ES/EF - it was set by set_initial_queue_es_ef()
            if existing_es < 0:
                continue  # Skip seed setting for initial queue projects
        
        # Calculate required finish time (in months from base_year)
        required_finish = (year_by_pid[pid] - base_year) * 12
        
        # Back-seed start time so that (unconstrained) commissioning finishes at required_finish.
        # Note: This can be negative for early-year targets (meaning "the plan implies work
        # started before base_year"). That's OK — it’s a planning baseline.
        # Use STANDARD (default) durations for seeding so actual durations can push finish past target year.
        tech = tech_by_pid.get(pid, "Solar")
        total_standard = standard_total_dur_by_tech.get(tech, 0)
        start_def = float(required_finish - total_standard)

        # Seed only the start of Definition (ES). Do NOT seed EF for Definition or any
        # Approvals/Construction times, so the forward pass sets all ES/EF from predecessors
        # and actual durations. Otherwise max(es_seed, pred_max) would keep later seeded
        # times and actual shorter durations would never pull Commissioning earlier.
        n_def = (pid, "Definition")
        if n_def in G.nodes:
            G.nodes[n_def]["ES"] = float(start_def)

    # Forward pass: calculate ES and EF for all nodes
    for n in nx.topological_sort(G):
        # Get predecessors
        preds = list(G.predecessors(n))
        
        # ES is maximum of:
        # 1. Seed ES (if set) - for back-seeded projects this may be negative
        # 2. Maximum EF of predecessors
        pred_max = max((G.nodes[p]["EF"] for p in preds), default=0.0)
        es_seed = float(G.nodes[n].get("ES", 0.0))
        
        # Special case: initial-queue/back-seeded nodes can have negative ES (started before base_year).
        # We keep that unless predecessor constraints push it later.
        if es_seed < 0:
            # For initial queue projects, use the pre-set ES value
            # But ensure EF respects predecessor constraints if there are any
            if preds:
                # If there are predecessors, ensure we don't start before they finish
                es = max(es_seed, pred_max)
            else:
                # No predecessors, use the pre-set ES value
                es = es_seed
        else:
            # Normal project: ES is maximum of seed and predecessor EF
            es = max(es_seed, pred_max)
        
        # EF = ES + duration
        G.nodes[n]["ES"] = es
        G.nodes[n]["EF"] = es + G.nodes[n]["duration"]

    # -------------------------------------------------------------------------
    # YEAR_FLOOR: Enforce that no project commissions before its target year.
    # Currently DISABLED so projects can finish before their target year when
    # actual durations are shorter than the standard. Set to True to restore
    # the floor behavior.
    # -------------------------------------------------------------------------
    YEAR_FLOOR_ENABLED = False
    if YEAR_FLOOR_ENABLED:
        for pid in projects:
            comm_node = (pid, "Commissioning")
            if comm_node not in G.nodes:
                continue
            required_finish = (year_by_pid[pid] - base_year) * 12
            ef = G.nodes[comm_node]["EF"]
            if ef < required_finish:
                G.nodes[comm_node]["EF"] = float(required_finish)
                G.nodes[comm_node]["ES"] = float(required_finish)

    # Calculate makespan
    makespan_months = max(d["EF"] for _, d in G.nodes(data=True))
    makespan_years = base_year + (makespan_months / 12)
    print(f"[INFO] CPM complete - Makespan: {makespan_months:.1f} months ({makespan_years:.1f} years from {base_year})")
    
    return G


def relabel_joint_commissioning_nodes(G: nx.DiGraph) -> nx.DiGraph:
    """
    Relabel joint commissioning nodes to ensure consistent naming.
    
    Updates joint commissioning node project IDs based on their actual predecessors.
    
    Args:
        G: Directed acyclic graph with joint commissioning nodes
    
    Returns:
        Graph with relabeled joint commissioning nodes
    """
    print("\n[INFO] Relabeling joint commissioning nodes...")
    mapping = {}
    joint_count = 0
    
    for node, data in list(G.nodes(data=True)):
        if data.get("TECH") != "Joint":
            continue
        joint_count += 1
        
        old_pid, stage = node
        
        # Joint nodes are defined by their predecessors (which solar, which batteries).
        solar_preds = [pred for pred in G.predecessors(node) if G.nodes[pred].get("TECH") == "Solar"]
        solar_id = G.nodes[solar_preds[0]].get("project_id") if solar_preds else data.get("project_id", "Joint")
        
        battery_preds = [pred for pred in G.predecessors(node) if G.nodes[pred].get("TECH") == "Battery"]
        battery_suffix = "+".join(sorted(G.nodes[pred]["project_id"].split("_")[-1] for pred in battery_preds))
        
        # Create new project ID
        new_project_id = f"{solar_id}::Joint[{battery_suffix}]" if battery_suffix else f"{solar_id}::Joint"
        
        # Update project_id attribute
        if new_project_id != data.get("project_id"):
            G.nodes[node]["project_id"] = new_project_id
        
        # Create new node tuple
        new_node = (new_project_id, stage)
        
        # Add to mapping if node name changed
        if new_node != node:
            mapping[node] = new_node
    
    # Relabel nodes if any changes were made
    if mapping:
        print(f"       Relabeled {len(mapping)} joint commissioning nodes")
        nx.relabel_nodes(G, mapping, copy=False)
    else:
        print(f"       No relabeling needed for {joint_count} joint nodes")
    
    return G


# ============================================================================
# ATTRITION FUNCTIONS
# ============================================================================

def calculate_attrition_probability(base_rate: float, delay_years: float, delay_threshold_years: float, max_rate: float) -> float:
    """
    Calculate attrition probability based on project delay.
    
    Args:
        base_rate: Base attrition rate (e.g., 0.05 = 5%)
        delay_years: How many years behind schedule (delay in years)
        delay_threshold_years: Years of delay where attrition becomes significant
        max_rate: Maximum attrition rate (e.g., 0.50 = 50%)
    
    Returns:
        Attrition probability between base_rate and max_rate
    """
    if delay_years <= 0:
        return base_rate
    else:
        # Simple “gets worse quickly” curve. This is a heuristic — tune to taste.
        delay_factor = delay_years / delay_threshold_years
        return min(max_rate, base_rate * (1 + np.exp(delay_factor)))


def apply_attrition(
    G: nx.DiGraph,
    base_year: int,
    params: Dict,
    random_seed: int = 42,
) -> Tuple[nx.DiGraph, pd.DataFrame]:
    """
    Apply attrition to projects based on delays after CPM.
    
    Process:
    1. Calculate delays for each project (EF(Construction) - target_year)
    2. Calculate attrition probabilities
    3. Randomly determine which projects attrit
    4. Handle cascades:
       - If Transmission attrits → dependent Solar/Battery attrit (unless they have other Transmission deps)
       - If Solar/Battery/Gas attrits → no rematching (surplus capacity)
       - If Battery attrits → Solar continues independently
    5. Remove outgoing edges from attrited projects
    6. Re-run CPM
    7. Track attrition results
    
    Args:
        G: Directed acyclic graph after initial CPM
        base_year: Base year for time calculations
        params: Interdependency parameters dictionary (must include attrition params)
        random_seed: Random seed for reproducibility
    
    Returns:
        Tuple of (updated_graph, attrition_df)
        - updated_graph: Graph with attrited projects marked and edges removed
        - attrition_df: DataFrame tracking which projects attrited and why
    """
    print("\n" + "=" * 80)
    print("APPLYING ATTRITION")
    print("=" * 80)
    
    # Check if attrition is enabled
    attrition_params = params.get("attrition", {})
    if not attrition_params.get("enabled", False):
        print("[INFO] Attrition is disabled. Skipping attrition process.")
        return G, pd.DataFrame(columns=["project_id", "TECH", "reason", "delay_years", "attrition_probability", "cascade_source"])
    
    base_rate = attrition_params.get("base_rate", 0.05)
    delay_threshold = attrition_params.get("delay_threshold_years", 1.0)
    max_rate = attrition_params.get("max_rate", 0.50)
    
    # Set random seed for reproducibility
    np.random.seed(random_seed)
    random.seed(random_seed)
    
    # Get all unique project IDs (excluding Joint nodes)
    projects = sorted({G.nodes[n]["project_id"] for n in G.nodes if G.nodes[n].get("TECH") != "Joint"})
    
    # Step 1: Calculate delays for each project
    print(f"\n[INFO] Calculating delays for {len(projects):,} projects...")
    project_delays = {}
    project_techs = {}
    project_target_years = {}
    
    for pid in projects:
        # Get Construction stage EF
        construction_node = (pid, "Construction")
        if construction_node not in G.nodes:
            continue
        
        ef_construction = G.nodes[construction_node]["EF"]  # in months
        target_year = G.nodes[construction_node].get("YEAR", base_year)
        tech = G.nodes[construction_node].get("TECH")
        
        # Calculate delay in years
        finish_year_cpm = base_year + (ef_construction / 12)
        delay_years = finish_year_cpm - target_year
        
        project_delays[pid] = delay_years
        project_techs[pid] = tech
        project_target_years[pid] = target_year
    
    # Step 2: Calculate attrition probabilities and randomly determine attrition
    print(f"\n[INFO] Determining which projects attrit...")
    attrited_projects = set()
    attrition_results = []
    
    for pid in projects:
        if pid not in project_delays:
            continue
        
        delay_years = project_delays[pid]
        tech = project_techs[pid]
        
        # Calculate attrition probability
        prob = calculate_attrition_probability(base_rate, delay_years, delay_threshold, max_rate)
        
        # Random draw
        if np.random.random() < prob:
            attrited_projects.add(pid)
            attrition_results.append({
                "project_id": pid,
                "TECH": tech,
                "reason": "own_delay",
                "delay_years": delay_years,
                "attrition_probability": prob,
                "cascade_source": None,
            })
    
    print(f"       {len(attrited_projects):,} projects attrited due to own delay")
    
    # Step 3: Handle cascades from Transmission attrition
    print(f"\n[INFO] Checking cascade effects...")
    
    # Find all Transmission projects that attrited
    attrited_transmission = {pid for pid in attrited_projects if project_techs[pid] == "Transmission"}
    
    if attrited_transmission:
        print(f"       {len(attrited_transmission):,} Transmission projects attrited - checking cascades...")
        
        # Build dependency map: which projects depend on which Transmission projects
        transmission_dependencies = {}  # {solar/battery_pid: set of transmission_pids it depends on}
        
        for pid in projects:
            tech = project_techs.get(pid)
            if tech not in ["Solar", "Battery"]:
                continue
            
            # Find all Transmission projects this project depends on
            construction_node = (pid, "Construction")
            if construction_node not in G.nodes:
                continue
            
            # Get all incoming edges to this Construction node
            incoming_edges = list(G.in_edges(construction_node))
            
            tx_deps = set()
            for pred_node, _ in incoming_edges:
                pred_tech = G.nodes[pred_node].get("TECH")
                if pred_tech == "Transmission":
                    pred_pid = G.nodes[pred_node]["project_id"]
                    tx_deps.add(pred_pid)
            
            if tx_deps:
                transmission_dependencies[pid] = tx_deps
        
        # For each Solar/Battery project, check if all its Transmission dependencies attrited
        for pid, tx_deps in transmission_dependencies.items():
            # Check if any Transmission dependencies attrited
            attrited_tx_deps = tx_deps & attrited_transmission
            
            if attrited_tx_deps:
                # Check if project has other Transmission dependencies that did NOT attrit
                remaining_tx_deps = tx_deps - attrited_transmission
                
                if not remaining_tx_deps:
                    # All Transmission dependencies attrited - this project attrits
                    if pid not in attrited_projects:
                        attrited_projects.add(pid)
                        tech = project_techs[pid]
                        delay = project_delays.get(pid, 0.0)
                        attrition_results.append({
                            "project_id": pid,
                            "TECH": tech,
                            "reason": "cascade",
                            "delay_years": delay,
                            "attrition_probability": 1.0,  # Cascade is deterministic
                            "cascade_source": f"Transmission:{','.join(sorted(attrited_tx_deps))}",
                        })
                        print(f"         {pid} ({tech}) attrited due to Transmission cascade")
    
    print(f"       Total attrited projects: {len(attrited_projects):,}")
    
    # Step 4: Mark attrited projects in graph and remove outgoing edges
    print(f"\n[INFO] Removing edges from attrited projects...")
    edges_removed = 0
    
    for pid in attrited_projects:
        # Mark all nodes of this project as attrited
        for stage in STAGES4:
            node = (pid, stage)
            if node in G.nodes:
                G.nodes[node]["attrited"] = True
        
        # When a project attrits, we cut its outgoing edges so it stops constraining others.
        # (We leave the nodes in place so you can still see “what dropped out”.)
        for stage in STAGES4:
            node = (pid, stage)
            if node in G.nodes:
                outgoing_edges = list(G.out_edges(node))
                for edge in outgoing_edges:
                    G.remove_edge(*edge)
                    edges_removed += 1
        
        # Also check for joint commissioning nodes
        # If Battery attrits, remove edge from Battery to joint node
        tech = project_techs.get(pid)
        if tech == "Battery":
            # Find joint nodes that depend on this battery
            battery_construction = (pid, "Construction")
            if battery_construction in G.nodes:
                outgoing_to_joint = [e for e in G.out_edges(battery_construction) 
                                   if G.nodes[e[1]].get("TECH") == "Joint"]
                for edge in outgoing_to_joint:
                    G.remove_edge(*edge)
                    edges_removed += 1
    
    print(f"       Removed {edges_removed:,} edges from attrited projects")
    
    # Step 5: Re-run CPM to propagate impacts
    print(f"\n[INFO] Re-running CPM after attrition...")
    G = cpm_forward_with_seeds(G, base_year)
    
    # Step 6: Create attrition results DataFrame
    attrition_df = pd.DataFrame(attrition_results)
    if len(attrition_df) > 0:
        print(f"\n[INFO] Attrition summary:")
        print(f"       Total projects attrited: {len(attrition_df):,}")
        print(f"       Own delay: {len(attrition_df[attrition_df['reason'] == 'own_delay']):,}")
        print(f"       Cascade: {len(attrition_df[attrition_df['reason'] == 'cascade']):,}")
        print(f"       By technology:")
        for tech in attrition_df['TECH'].unique():
            count = len(attrition_df[attrition_df['TECH'] == tech])
            print(f"         {tech}: {count}")
    else:
        print(f"\n[INFO] No projects attrited")
    
    return G, attrition_df


# ============================================================================
# RESULTS EXPORT FUNCTIONS
# ============================================================================

def export_results_to_dataframes(
    G: nx.DiGraph,
    base_year: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Export CPM results to DataFrames for analysis.
    
    Args:
        G: Directed acyclic graph with calculated ES/EF times
        base_year: Base year used in CPM calculations
    
    Returns:
        Tuple of (results_df, deployment_df)
        - results_df: All nodes with their timing information
        - deployment_df: Annual capacity deployment by technology and zone
    """
    print("\n" + "=" * 80)
    print("EXPORTING RESULTS TO DATAFRAMES")
    print("=" * 80)
    
    # Build results DataFrame
    rows = []
    for n in G.nodes:
        d = G.nodes[n]
        rows.append(
            {
                "ID": d["project_id"],
                "TECH": d.get("TECH"),
                "ZONE": d.get("ZONE"),
                "STAGE": d.get("stage"),
                "YEAR_anchor": d.get("YEAR"),
                "Capacity_MW": d.get("Capacity", 0.0),
                "duration_months": d.get("duration"),
                "t_start_month": d.get("ES"),
                "t_end_month": d.get("EF"),
                "attrited": d.get("attrited", False),
            }
        )
    
    res_df = pd.DataFrame(rows).sort_values(["ID", "STAGE"])
    print(f"[INFO] Results DataFrame: {len(res_df):,} rows (all stages)")
    
    # Calculate finish year
    res_df["finish_year"] = res_df.apply(
        lambda r: base_year + int((r["t_end_month"] - 1) // 12) if r["t_end_month"] > 0 else base_year,
        axis=1,
    )
    
    # Build deployment DataFrame (only Commissioning stage, excluding attrited projects)
    # In this model, “deployed in year Y” means the Commissioning node finished in year Y.
    deploy_df = (
        res_df.query("STAGE == 'Commissioning' and attrited == False")
        .groupby(["TECH", "ZONE", "finish_year"], as_index=False)["Capacity_MW"].sum()
        .rename(columns={"finish_year": "YEAR", "Capacity_MW": "annual_additions_MW"})
    )
    
    print(f"[INFO] Deployment DataFrame: {len(deploy_df):,} rows (commissioning stage only)")
    print("\n[INFO] Deployment summary by technology:")
    for tech in sorted(deploy_df["TECH"].unique()):
        tech_deploy = deploy_df[deploy_df["TECH"] == tech]
        total_cap = tech_deploy["annual_additions_MW"].sum()
        years = sorted(tech_deploy["YEAR"].unique())
        print(f"       {tech}: {total_cap:,.2f} MW deployed over {len(years)} years ({years[0]}-{years[-1]})")
    
    return res_df, deploy_df


# ============================================================================
# MAIN EXECUTION (for running from command line)
# ============================================================================

def run_sequencing_model(
    csv_path: Path,
    zones: List[str] | None = None,
    durations_config: Dict[str, Dict[str, int]] | None = None,
    distributions_config: Dict[str, Dict] | None = None,
    rep_sizes: Dict[str, float] | None = None,
    initial_queue_state: Dict[str, Dict[str, float]] | None = None,
    subtract_initial_queue: bool = True,
    save_results: bool = False,
    results_dir: Path | None = None,
    run_name: str | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, nx.DiGraph, int]:
    """
    Run the sequencing model WITHOUT interdependencies.
    
    This creates a baseline for comparison - projects are sequenced through their stages
    but there are no cross-project dependencies (no transmission gating, gas support, etc.).
    
    Args:
        csv_path: Path to the capacity CSV file
        zones: List of zones to analyze. If None, uses DEFAULT_ZONES
        durations_config: Dictionary of stage durations for each technology.
                         If None, uses DEFAULT_DURATIONS
        distributions_config: Duration distribution parameters for per-project sampling.
                              If None or enabled=False, uses fixed durations from durations_config
        rep_sizes: Dictionary mapping technology names to representative project sizes (MW).
                   If None, uses defaults
        initial_queue_state: Optional dictionary mapping tech names to initial queue capacities.
                            Format: {tech: {"Definition_MW": X, "Approvals_MW": Y, "Construction_MW": Z}}
        subtract_initial_queue: If True and initial_queue_state provided, subtract initial queue from targets.
        save_results: Whether to save results to CSV files
        results_dir: Directory to save results. If None and save_results=True, uses "results/"
    
    Returns:
        Tuple of (results_df, deployment_df, graph, base_year)
    """
    if zones is None:
        zones = DEFAULT_ZONES.copy()
    
    if rep_sizes is None:
        rep_sizes = {
            "Solar": 200.0,
            "Transmission": 500.0,
            "Gas Turbine": 100.0,
            "Battery": 100.0,
        }
    
    # Default initial queue state (empty - starts from zero)
    if initial_queue_state is None:
        initial_queue_state = {
            "Solar": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
            "Transmission": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
            "Gas Turbine": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
            "Battery": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
        }
    
    print("=" * 80)
    print("SEQUENCING MODEL EXECUTION (NO INTERDEPENDENCIES)")
    print("=" * 80)
    print(f"\nConfiguration:")
    print(f"  CSV file: {csv_path}")
    print(f"  Zones: {zones}")
    print(f"  Representative project sizes: {rep_sizes}")
    if initial_queue_state and any(sum(v.values()) > 0 for v in initial_queue_state.values()):
        print(f"  Initial queue state: {initial_queue_state}")
        print(f"  Subtract initial queue from targets: {subtract_initial_queue}")
    
    # Load targets (with optional initial queue subtraction)
    solar_targets = load_solar_targets(
        csv_path, zones=zones, initial_queue_state=initial_queue_state, subtract_initial_queue=subtract_initial_queue, run_name=run_name
    )
    gas_targets = load_gas_targets(
        csv_path, zones=zones, initial_queue_state=initial_queue_state, subtract_initial_queue=subtract_initial_queue, run_name=run_name
    )
    battery_targets = load_battery_targets(
        csv_path, zones=zones, initial_queue_state=initial_queue_state, subtract_initial_queue=subtract_initial_queue, run_name=run_name
    )
    transmission_targets = load_transmission_targets(
        csv_path, zones=zones, initial_queue_state=initial_queue_state, subtract_initial_queue=subtract_initial_queue, run_name=run_name
    )
    
    # Build durations (needed for fallback if distributions disabled)
    if durations_config is None:
        durations_config = DEFAULT_DURATIONS.copy()
    base_durations = create_durations_dataframe(durations_config, params=None)
    print(f"\n[INFO] Stage durations:")
    for _, row in base_durations.iterrows():
        print(f"       {row['TECH']}: Def={row['Definition_months']}, App={row['Approvals_months']}, Const={row['Construction_months']} months")
    
    # Check if distributions are enabled
    if distributions_config:
        enabled_techs = [tech for tech, config in distributions_config.items() if config.get("enabled", False)]
        if enabled_techs:
            print(f"[INFO] Duration distributions enabled for: {', '.join(enabled_techs)}")
            print(f"       Projects will have varying durations sampled from distributions")
    
    durations4 = build_durations_with_commissioning(base_durations)
    
    # Generate projects from targets (with duration sampling if distributions enabled)
    projects_all = generate_projects_multi_tech(
        solar_targets=solar_targets,
        transmission_targets=transmission_targets,
        gas_targets=gas_targets,
        battery_targets=battery_targets,
        rep_sizes=rep_sizes,
        durations_config=durations_config,
        distributions_config=distributions_config,
    )
    
    # Generate initial queue projects (if any)
    # Get base year from regular projects first (or use earliest target year)
    if len(projects_all) > 0:
        base_year_temp = int(projects_all["YEAR"].min())
    else:
        # If no regular projects, use earliest year from targets
        all_years = []
        for df in [solar_targets, gas_targets, battery_targets, transmission_targets]:
            if len(df) > 0:
                all_years.extend(df["YEAR"].tolist())
        base_year_temp = min(all_years) if all_years else 2030
    
    initial_queue_projects = generate_initial_queue_projects(
        initial_queue_state=initial_queue_state,
        durations_config=durations_config,
        rep_sizes=rep_sizes,
        zones=zones,
        base_year=base_year_temp,
        distributions_config=distributions_config,
        # Split each tech's queue total across zones (equal weights). Do not duplicate the same
        # MW into every zone — that would inflate totals vs subtract_initial_queue_from_targets.
        distribute_across_zones=bool(zones) and len(zones) > 1,
        zone_weights=None,
    )
    
    # Combine regular projects with initial queue projects
    if len(initial_queue_projects) > 0:
        # Remove "initial_stage" column from regular projects if it doesn't exist (for consistency)
        if "initial_stage" not in projects_all.columns:
            projects_all["initial_stage"] = None
        projects_all = pd.concat([projects_all, initial_queue_projects], ignore_index=True)
        print(f"\n[INFO] Combined {len(projects_all)} total projects ({len(initial_queue_projects)} initial queue + {len(projects_all) - len(initial_queue_projects)} new)")
    
    # Build DAG WITHOUT interdependencies and run CPM
    Gseq, base_year = build_system_dag_sequencing(projects_all, durations4)
    Gseq = cpm_forward_with_seeds(Gseq, base_year)
    # Note: relabel_joint_commissioning_nodes not needed for sequencing model (no joint nodes)
    
    # Export results
    res_df, deploy_df = export_results_to_dataframes(Gseq, base_year)
    
    # Save results if requested
    if save_results:
        if results_dir is None:
            results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        
        res_df.to_csv(results_dir / "dag_sequencing_results.csv", index=False)
        deploy_df.to_csv(results_dir / "dag_sequencing_deployment_results.csv", index=False)
        print(f"\n[INFO] Results saved to {results_dir}/")
    
    print("\n" + "=" * 80)
    print("SEQUENCING MODEL EXECUTION COMPLETE")
    print("=" * 80)
    
    return res_df, deploy_df, Gseq, base_year


def run_model(
    csv_path: Path,
    zones: List[str] | None = None,
    durations_config: Dict[str, Dict[str, int]] | None = None,
    distributions_config: Dict[str, Dict] | None = None,
    rep_sizes: Dict[str, float] | None = None,
    params: dict | None = None,
    initial_queue_state: Dict[str, Dict[str, float]] | None = None,
    subtract_initial_queue: bool = True,
    save_results: bool = False,
    results_dir: Path | None = None,
    run_name: str | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, nx.DiGraph, int]:
    """
    Run the COMPLETE INTERDEPENDENCY MODEL.
    
    Args:
        csv_path: Path to the capacity delta CSV file (annual additions/delta format), e.g. capacity_delta_subset(in).csv
        zones: List of zones to analyze. If None, uses DEFAULT_ZONES
        durations_config: Dictionary of stage durations for each technology.
                         If None, uses DEFAULT_DURATIONS
        distributions_config: Duration distribution parameters for per-project sampling.
                              If None or enabled=False, uses fixed durations from durations_config
        rep_sizes: Dictionary mapping technology names to representative project sizes (MW).
                   If None, uses defaults
        params: Interdependency parameters dictionary. If None, uses INTERDEPENDENCY_PARAMS
        initial_queue_state: Optional dictionary mapping tech names to initial queue capacities.
                            Format: {tech: {"Definition_MW": X, "Approvals_MW": Y, "Construction_MW": Z}}
        subtract_initial_queue: If True and initial_queue_state provided, subtract initial queue from targets.
        save_results: Whether to save results to CSV files
        results_dir: Directory to save results. If None and save_results=True, uses "results/"
    
    Returns:
        Tuple of (results_df, deployment_df, graph, base_year)
    """
    if zones is None:
        zones = DEFAULT_ZONES.copy()
    
    if params is None:
        params = INTERDEPENDENCY_PARAMS
    
    if rep_sizes is None:
        rep_sizes = {
            "Solar": 200.0,
            "Transmission": params["transmission"]["rep_project_size_MW"],
            "Gas Turbine": 100.0,
            "Battery": 100.0,
        }
    
    # Default initial queue state (empty - starts from zero)
    if initial_queue_state is None:
        initial_queue_state = {
            "Solar": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
            "Transmission": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
            "Gas Turbine": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
            "Battery": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
        }
    
    print("=" * 80)
    print("INTERDEPENDENCY MODEL EXECUTION")
    print("=" * 80)
    print(f"\nConfiguration:")
    print(f"  CSV file: {csv_path}")
    print(f"  Zones: {zones}")
    print(f"  Representative project sizes: {rep_sizes}")
    if initial_queue_state and any(sum(v.values()) > 0 for v in initial_queue_state.values()):
        print(f"  Initial queue state: {initial_queue_state}")
        print(f"  Subtract initial queue from targets: {subtract_initial_queue}")
    
    # Load targets (with optional initial queue subtraction)
    solar_targets = load_solar_targets(
        csv_path, zones=zones, initial_queue_state=initial_queue_state, subtract_initial_queue=subtract_initial_queue, run_name=run_name
    )
    gas_targets = load_gas_targets(
        csv_path, zones=zones, initial_queue_state=initial_queue_state, subtract_initial_queue=subtract_initial_queue, run_name=run_name
    )
    battery_targets = load_battery_targets(
        csv_path, zones=zones, initial_queue_state=initial_queue_state, subtract_initial_queue=subtract_initial_queue, run_name=run_name
    )
    transmission_targets = load_transmission_targets(
        csv_path, zones=zones, initial_queue_state=initial_queue_state, subtract_initial_queue=subtract_initial_queue, run_name=run_name
    )
    
    # Build durations (needed for fallback if distributions disabled)
    if durations_config is None:
        durations_config = DEFAULT_DURATIONS.copy()
    base_durations = create_durations_dataframe(durations_config, params)
    print(f"\n[INFO] Stage durations:")
    for _, row in base_durations.iterrows():
        print(f"       {row['TECH']}: Def={row['Definition_months']}, App={row['Approvals_months']}, Const={row['Construction_months']} months")
    
    # Check if distributions are enabled
    if distributions_config:
        enabled_techs = [tech for tech, config in distributions_config.items() if config.get("enabled", False)]
        if enabled_techs:
            print(f"[INFO] Duration distributions enabled for: {', '.join(enabled_techs)}")
            print(f"       Projects will have varying durations sampled from distributions")
    
    durations4 = build_durations_with_commissioning(base_durations)
    
    # Generate projects from targets (with duration sampling if distributions enabled)
    projects_all = generate_projects_multi_tech(
        solar_targets=solar_targets,
        transmission_targets=transmission_targets,
        gas_targets=gas_targets,
        battery_targets=battery_targets,
        rep_sizes=rep_sizes,
        durations_config=durations_config,
        distributions_config=distributions_config,
    )
    
    # Generate initial queue projects (if any)
    # Get base year from regular projects first (or use earliest target year)
    if len(projects_all) > 0:
        base_year_temp = int(projects_all["YEAR"].min())
    else:
        # If no regular projects, use earliest year from targets
        all_years = []
        for df in [solar_targets, gas_targets, battery_targets, transmission_targets]:
            if len(df) > 0:
                all_years.extend(df["YEAR"].tolist())
        base_year_temp = min(all_years) if all_years else 2030
    
    initial_queue_projects = generate_initial_queue_projects(
        initial_queue_state=initial_queue_state,
        durations_config=durations_config,
        rep_sizes=rep_sizes,
        zones=zones,
        base_year=base_year_temp,
        distributions_config=distributions_config,
        distribute_across_zones=bool(zones) and len(zones) > 1,
        zone_weights=None,
    )
    
    # Combine regular projects with initial queue projects
    if len(initial_queue_projects) > 0:
        # Remove "initial_stage" column from regular projects if it doesn't exist (for consistency)
        if "initial_stage" not in projects_all.columns:
            projects_all["initial_stage"] = None
        projects_all = pd.concat([projects_all, initial_queue_projects], ignore_index=True)
        print(f"\n[INFO] Combined {len(projects_all)} total projects ({len(initial_queue_projects)} initial queue + {len(projects_all) - len(initial_queue_projects)} new)")
    
    # Build DAG and run CPM
    Ginter, base_year = build_system_dag_with_interdeps(projects_all, durations4, params)
    Ginter = cpm_forward_with_seeds(Ginter, base_year)
    Ginter = relabel_joint_commissioning_nodes(Ginter)
    
    # Apply attrition (if enabled)
    Ginter, attrition_df = apply_attrition(Ginter, base_year, params)
    
    # Export results (after attrition)
    res_df, deploy_df = export_results_to_dataframes(Ginter, base_year)
    
    # Save results if requested
    if save_results:
        if results_dir is None:
            results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        
        res_df.to_csv(results_dir / "dag_interdeps_results.csv", index=False)
        deploy_df.to_csv(results_dir / "dag_interdeps_deployment_results.csv", index=False)
        
        # Save attrition results if any projects attrited
        if len(attrition_df) > 0:
            attrition_df.to_csv(results_dir / "dag_interdeps_attrition_results.csv", index=False)
            print(f"       Attrition results saved to {results_dir}/dag_interdeps_attrition_results.csv")
        
        print(f"\n[INFO] Results saved to {results_dir}/")
    
    print("\n" + "=" * 80)
    print("MODEL EXECUTION COMPLETE")
    print("=" * 80)
    
    return res_df, deploy_df, Ginter, base_year


if __name__ == "__main__":
    """
    Execution of the interdependency model.
    Run this file directly to see the model in action with print statements.
    
    Paramters can be customized below - we can change zones and durations
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the interdependency or sequencing model")
    parser.add_argument(
        "--csv",
        type=str,
        default="capacity_delta_subset(in).csv",
        help="Path to capacity_delta_subset(in).csv file (annual additions/delta format)"
    )
    parser.add_argument(
        "--zones",
        type=str,
        nargs="+",
        default=None,
        help="Zones to analyze (e.g., --zones qld-north qld-south). Default: qld-north"
    )
    parser.add_argument(
        "--sequencing",
        action="store_true",
        help="Run sequencing model (no interdependencies) instead of interdependent model"
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save results to CSV files"
    )
    parser.add_argument(
        "--results-dir",
        type=str,
        default="base_model_results",
        help="Directory to save results (default: base_model_results/)"
    )
    
    args = parser.parse_args()
    
    csv_path = Path(args.csv)
    zones = args.zones if args.zones else DEFAULT_ZONES
    
    if not csv_path.exists():
        print(f"\n[ERROR] CSV file not found at {csv_path}")
        print("Please run this from the Thesis directory, or specify the correct path with --csv")
        print("\nExample usage:")
        print("  python3 base_interdependency_model.py --zones qld-north qld-south --save")
        print("  python3 base_interdependency_model.py --sequencing --zones qld-north --save")
        print("\nOr customize durations in Python:")
        print("  from base_interdependency_model import run_model, DEFAULT_DURATIONS")
        print("  custom_durations = DEFAULT_DURATIONS.copy()")
        print("  custom_durations['Solar']['Construction_months'] = 24")
        print("  run_model(Path('capacity_delta_subset(in).csv'), zones=['qld-north'], durations_config=custom_durations)")
    else:
        # Run the model
        if args.sequencing:
            res_df, deploy_df, G, base_year = run_sequencing_model(
                csv_path=csv_path,
                zones=zones,
                durations_config=None,  
                save_results=args.save,
                results_dir=Path(args.results_dir) if args.save else None,
            )
            print(f"\nResults available:")
            print(f"  - Results DataFrame: {len(res_df):,} rows")
            print(f"  - Deployment DataFrame: {len(deploy_df):,} rows")
            if not args.save:
                print(f"\nTo save results, use --save flag or:")
                print(f"  res_df.to_csv('results/dag_sequencing_results.csv', index=False)")
                print(f"  deploy_df.to_csv('results/dag_sequencing_deployment_results.csv', index=False)")
        else:
            res_df, deploy_df, G, base_year = run_model(
                csv_path=csv_path,
                zones=zones,
                durations_config=None,  
                save_results=args.save,
                results_dir=Path(args.results_dir) if args.save else None,
            )
            
            print(f"\nResults available:")
            print(f"  - Results DataFrame: {len(res_df):,} rows")
            print(f"  - Deployment DataFrame: {len(deploy_df):,} rows")
            if not args.save:
                print(f"\nTo save results, use --save flag or:")
                print(f"  res_df.to_csv('results/dag_interdeps_results.csv', index=False)")
                print(f"  deploy_df.to_csv('results/dag_interdeps_deployment_results.csv', index=False)")
