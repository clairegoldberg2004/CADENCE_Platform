"""
Flask backend API for Interdependency Model Platform
"""
from flask import Flask, request, jsonify, send_file, abort
from flask_cors import CORS
from pathlib import Path
import os
import json
import copy
import io
import base64
from typing import Dict, List, Tuple, Optional

from base_interdependency_model import (
    run_model,
    run_sequencing_model,
    DEFAULT_DURATIONS,
    INTERDEPENDENCY_PARAMS,
    DEFAULT_ZONES,
    export_results_to_dataframes,
    apply_attrition,
    build_system_dag_with_interdeps,
    cpm_forward_with_seeds,
    relabel_joint_commissioning_nodes,
    generate_projects_multi_tech,
    generate_initial_queue_projects,
    load_solar_targets,
    load_gas_targets,
    load_battery_targets,
    load_transmission_targets,
    build_durations_with_commissioning,
    create_durations_dataframe,
)
import pandas as pd
import random

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend


@app.route("/")
def root():
    """Root URL: this service is JSON APIs only (no HTML). Browsers hitting / get 200 + pointers."""
    return jsonify(
        service="CADENCE Platform API",
        status="ok",
        health_check="GET /api/defaults",
    )


# -----------------------------------------------------------------------------
# Scenario results gallery (precomputed artifacts) created by scenario analysis
# -----------------------------------------------------------------------------

RESULTS_RUN_PREFIXES = (
    "results_scenarios",
    "results_scenarios_with_zero",
    "results_scenarios_mc10",
    "results_scenarios_ablation",
    "final_results",
)


def _run_has_gallery_curves(run_name: str) -> bool:
    """True if the run folder has at least one main cumulative-deployment PNG."""
    d = Path(run_name)
    if not d.is_dir():
        return False
    return (d / "cumulative_deployment_curves_main_scenarios.png").is_file() or (
        d / "cumulative_deployment_curves.png"
    ).is_file()


def _pick_default_gallery_run(runs: List[str]) -> Optional[str]:
    """
    Prefer runs that actually contain gallery plots (some results_* dirs are CSV-only).
    Order: results_scenarios_with_zero -> final_results -> first run with curves -> first listed.
    """
    if not runs:
        return None
    if "results_scenarios_with_zero" in runs:
        return "results_scenarios_with_zero"
    if "final_results" in runs:
        return "final_results"
    for r in runs:
        if _run_has_gallery_curves(r):
            return r
    return runs[0]


def _list_results_runs() -> List[str]:
    """List available precomputed results directories (runs) under the repo root."""
    runs: List[str] = []
    for p in sorted(Path(".").iterdir()):
        if not p.is_dir():
            continue
        if any(p.name.startswith(pref) for pref in RESULTS_RUN_PREFIXES):
            runs.append(p.name)
    return runs

def _safe_run_dir(run: str) -> Path:
    """
    Return a resolved directory path for a run, preventing path traversal.
    Only allows directories in repo root that match the allowed prefixes.
    """
    if not run or "/" in run or "\\" in run or ".." in run:
        abort(400, description="Invalid run.")
    if not any(run.startswith(pref) for pref in RESULTS_RUN_PREFIXES):
        abort(400, description="Run not allowed.")
    p = Path(run).resolve()
    root = Path(".").resolve()
    if not str(p).startswith(str(root)):
        abort(400, description="Invalid run path.")
    if not p.exists() or not p.is_dir():
        abort(404, description=f"Run not found: {run}")
    return p

def _safe_run_file(run_dir: Path, filename: str) -> Path:
    """Return a resolved file path under run_dir, preventing path traversal."""
    if not filename or "/" in filename or "\\" in filename or ".." in filename:
        abort(400, description="Invalid filename.")
    f = (run_dir / filename).resolve()
    if not str(f).startswith(str(run_dir.resolve())):
        abort(400, description="Invalid file path.")
    if not f.exists() or not f.is_file():
        abort(404, description=f"File not found: {filename}")
    return f

def _build_gallery_manifest(run_dir: Path) -> Dict:
    """
    Build a gallery manifest for a given run directory:
    - groups PNGs by naming convention
    - loads scenario_metrics.csv (if present) for tabular display
    """
    pngs = sorted([p.name for p in run_dir.glob("*.png")])
    csvs = sorted([p.name for p in run_dir.glob("*.csv")])

    def pick(names: List[str], starts: str) -> List[str]:
        return [n for n in names if n.startswith(starts)]

    # High-signal plots from scenario_visualize.py (not all runs will have all files)
    main = [
        "cumulative_deployment_curves_main_scenarios.png",
        "cumulative_deployment_curves.png",
        "absolute_bars.png",
        "delta_bars_vs_baseline.png",
        "attrition_and_delay.png",
        "solar_binding_constraint_attribution.png",
        "scenario_parameters_summary_table.png",
    ]

    # paths for oat plots
    per_scenario_oat = {
        "S1_REAL_WORLD_2026_FRAGMENTED": "cumulative_deployment_curves_S1_REAL_WORLD_2026_FRAGMENTED__with_oat.png",
        "S2_IRA_INCENTIVE_RUSH": "cumulative_deployment_curves_S2_IRA_INCENTIVE_RUSH__with_oat.png",
        "S3_CHINA_COORDINATED": "cumulative_deployment_curves_S3_CHINA_COORDINATED__with_oat.png",
    }

    tornado_mw = pick(pngs, "ablation_tornado_")
    # Split MW vs delay tornado for easier UI grouping
    tornado_mw = [n for n in tornado_mw if n.endswith("__cum_MW_by_2050.png")]
    tornado_delay = pick(pngs, "ablation_tornado_")
    tornado_delay = [n for n in tornado_delay if n.endswith("__delay_mean_years.png")]

    param_pages = [n for n in pngs if n.startswith("scenario_parameters_full_table_p") and n.endswith(".png")]

    # Metrics (optional)
    metrics_path = run_dir / "scenario_metrics.csv"
    metrics_rows = None
    if metrics_path.exists():
        try:
            metrics_df = pd.read_csv(metrics_path)
            metrics_rows = metrics_df.to_dict("records")
        except Exception:
            metrics_rows = None

    def exists(name: str) -> Optional[str]:
        return name if name in pngs else None

    return {
        "run": run_dir.name,
        "pngs": pngs,
        "csvs": csvs,
        "groups": {
            "main": [n for n in (exists(x) for x in main) if n],
            "per_scenario_oat": {k: v for k, v in per_scenario_oat.items() if v in pngs},
            "parameter_pages": param_pages,
            "tornado_cum_MW_by_2050": tornado_mw,
            "tornado_delay_mean_years": tornado_delay,
        },
        "metrics": metrics_rows,
    }

# Default parameters matching the notebook
DEFAULT_REP_SIZES = {
    "Solar": 200.0,
    "Transmission": INTERDEPENDENCY_PARAMS["transmission"]["rep_project_size_MW"],
    "Gas Turbine": 100.0,
    "Battery": 100.0,
}

DEFAULT_INITIAL_QUEUE_STATE = {
    "Solar": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
    "Transmission": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
    "Gas Turbine": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
    "Battery": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
}


def _zones_from_capacity_csv(csv_path: Optional[Path] = None) -> List[str]:
    """Sorted unique zones from the capacity CSV; empty if file missing or unreadable."""
    p = csv_path or Path("capacity_delta_subset(in).csv")
    if not p.exists():
        return []
    try:
        df = pd.read_csv(p)
        if "zone" not in df.columns:
            return []
        return sorted([str(z) for z in df["zone"].dropna().unique() if pd.notna(z)])
    except Exception:
        return []


@app.route('/api/defaults', methods=['GET'])
def get_defaults():
    """Get all default parameters"""
    zones = _zones_from_capacity_csv()
    if not zones:
        zones = DEFAULT_ZONES.copy()
    return jsonify({
        'zones': zones,
        'durations_config': DEFAULT_DURATIONS.copy(),
        'interdependency_params': INTERDEPENDENCY_PARAMS.copy(),
        'rep_sizes': DEFAULT_REP_SIZES.copy(),
        'initial_queue_state': DEFAULT_INITIAL_QUEUE_STATE.copy(),
    })


@app.route("/api/gallery/runs", methods=["GET"])
def gallery_runs():
    """List available precomputed scenario result runs."""
    runs = _list_results_runs()
    default_run = _pick_default_gallery_run(runs)
    if default_run and default_run in runs:
        runs = [default_run] + [r for r in runs if r != default_run]
    return jsonify({"runs": runs, "default_run": default_run})


@app.route("/api/gallery/run/<run>/manifest", methods=["GET"])
def gallery_manifest(run: str):
    """Return a manifest of images/CSVs/metrics for a given run directory."""
    run_dir = _safe_run_dir(run)
    return jsonify(_build_gallery_manifest(run_dir))


@app.route("/api/gallery/run/<run>/file/<filename>", methods=["GET"])
def gallery_file(run: str, filename: str):
    """Serve a PNG/CSV artifact from a run directory."""
    run_dir = _safe_run_dir(run)
    f = _safe_run_file(run_dir, filename)
    return send_file(f, as_attachment=False)


@app.route('/api/available-zones', methods=['GET'])
def get_available_zones():
    """Get list of available zones from CSV file"""
    try:
        csv_path = Path('capacity_delta_subset(in).csv')
        if not csv_path.exists():
            return jsonify({'error': 'CSV file not found'}), 404

        zones = _zones_from_capacity_csv(csv_path)
        return jsonify({'zones': zones})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/run-model', methods=['POST'])
def run_model_endpoint():
    """Run the interdependency model with provided parameters"""
    try:
        data = request.json
        mode = data.get('mode', 'custom')  # 'default', 'custom', or 'compare'
        compare_mode = mode == 'compare'
        
        # Get CSV path
        csv_path = Path(data.get('csv_path', 'capacity_delta_subset(in).csv'))
        if not csv_path.exists():
            return jsonify({'error': f'CSV file not found: {csv_path}'}), 400
        
        # Get parameters
        zones_input = data.get('zones', DEFAULT_ZONES.copy())
        # Handle zones as string (comma-separated) or array
        if isinstance(zones_input, str):
            zones = [z.strip() for z in zones_input.split(',') if z.strip()]
        else:
            zones = zones_input if isinstance(zones_input, list) else DEFAULT_ZONES.copy()
        
        durations_config = data.get('durations_config')
        distributions_config = data.get('distributions_config')
        interdependency_params = data.get('interdependency_params')
        rep_sizes = data.get('rep_sizes')
        initial_queue_state = data.get('initial_queue_state')
        subtract_initial_queue = data.get('subtract_initial_queue', True)
        definition_multiplier = data.get('definition_duration_multiplier', 1.0)
        
        results = {}
        
        if compare_mode:
            # Run both default and custom scenarios
            # In compare mode, use the same zones for both scenarios for fair comparison
            # Use custom zones if provided, otherwise use default zones
            selected_zones = zones if zones else DEFAULT_ZONES.copy()
            
            # Default scenario - use selected zones
            default_zones = selected_zones.copy()
            default_durations = copy.deepcopy(DEFAULT_DURATIONS)
            default_params = copy.deepcopy(INTERDEPENDENCY_PARAMS)
            default_rep_sizes = DEFAULT_REP_SIZES.copy()
            default_initial_queue = copy.deepcopy(DEFAULT_INITIAL_QUEUE_STATE)
            
            # Ensure Transmission Construction_months uses lead_time_months
            if "Transmission" in default_durations:
                default_durations["Transmission"]["Construction_months"] = default_params["transmission"]["lead_time_months"]
            
            # Convert nested dicts to proper format
            default_durations = {k: {kk: int(vv) if isinstance(vv, (int, float)) else vv 
                                   for kk, vv in v.items()} 
                               for k, v in default_durations.items()}
            
            # Run default scenario using the same approach as notebook
            # Use selected_zones (same as custom) for fair comparison
            res_df_default, deploy_df_default, G_default, base_year_default, attrition_df_default = run_model_scenario(
                csv_path=csv_path,
                zones=selected_zones.copy(),
                durations_config=default_durations,
                distributions_config=None,
                rep_sizes=default_rep_sizes,
                params=default_params,
                initial_queue_state=default_initial_queue,
                subtract_initial_queue=True,
            )
            
            results['default'] = {
                'results_df': res_df_default.to_dict('records'),
                'deployment_df': deploy_df_default.to_dict('records'),
                'base_year': int(base_year_default),
                'attrition_df': attrition_df_default.to_dict('records') if len(attrition_df_default) > 0 else [],
            }
            
            # Custom scenario
            custom_durations = copy.deepcopy(durations_config) if durations_config else copy.deepcopy(DEFAULT_DURATIONS)
            custom_params = copy.deepcopy(interdependency_params) if interdependency_params else copy.deepcopy(INTERDEPENDENCY_PARAMS)
            custom_rep_sizes = rep_sizes.copy() if rep_sizes else DEFAULT_REP_SIZES.copy()
            custom_initial_queue = copy.deepcopy(initial_queue_state) if initial_queue_state else copy.deepcopy(DEFAULT_INITIAL_QUEUE_STATE)
            
            # Apply definition duration multiplier
            if definition_multiplier != 1.0:
                for tech in custom_durations:
                    if "Definition_months" in custom_durations[tech]:
                        custom_durations[tech]["Definition_months"] = int(
                            custom_durations[tech]["Definition_months"] * definition_multiplier
                        )
            
            # Ensure Transmission Construction_months uses lead_time_months if not explicitly set
            if "Transmission" in custom_durations and custom_params.get("transmission", {}).get("lead_time_months"):
                # Only override if Construction_months matches the default (120), otherwise use custom value
                if custom_durations["Transmission"]["Construction_months"] == DEFAULT_DURATIONS["Transmission"]["Construction_months"]:
                    custom_durations["Transmission"]["Construction_months"] = custom_params["transmission"]["lead_time_months"]
            
            # Convert nested dicts to proper format
            custom_durations = {k: {kk: int(vv) if isinstance(vv, (int, float)) else vv 
                                  for kk, vv in v.items()} 
                              for k, v in custom_durations.items()}
            
            # Run custom scenario using the same approach as notebook
            # Use selected_zones (same as default) in compare mode for fair comparison
            res_df_custom, deploy_df_custom, G_custom, base_year_custom, attrition_df_custom = run_model_scenario(
                csv_path=csv_path,
                zones=selected_zones.copy(),
                durations_config=custom_durations,
                distributions_config=distributions_config,
                rep_sizes=custom_rep_sizes,
                params=custom_params,
                initial_queue_state=custom_initial_queue,
                subtract_initial_queue=subtract_initial_queue,
            )
            
            results['custom'] = {
                'results_df': res_df_custom.to_dict('records'),
                'deployment_df': deploy_df_custom.to_dict('records'),
                'base_year': int(base_year_custom),
                'attrition_df': attrition_df_custom.to_dict('records') if len(attrition_df_custom) > 0 else [],
            }
            
            # Store graphs for DAG visualization
            results['graphs'] = {
                'default': serialize_graph(G_default),
                'custom': serialize_graph(G_custom),
            }
            
        elif mode == 'default':
            # Run default scenario only — use the same zones as compare mode (sidebar selection)
            selected_zones = zones if zones else DEFAULT_ZONES.copy()
            default_durations = copy.deepcopy(DEFAULT_DURATIONS)
            default_params = copy.deepcopy(INTERDEPENDENCY_PARAMS)
            if "Transmission" in default_durations:
                default_durations["Transmission"]["Construction_months"] = default_params["transmission"]["lead_time_months"]
            
            res_df, deploy_df, G, base_year, attrition_df = run_model_scenario(
                csv_path=csv_path,
                zones=selected_zones.copy(),
                durations_config=default_durations,
                distributions_config=None,
                rep_sizes=DEFAULT_REP_SIZES.copy(),
                params=default_params,
                initial_queue_state=DEFAULT_INITIAL_QUEUE_STATE.copy(),
                subtract_initial_queue=True,
            )
            
            results['default'] = {
                'results_df': res_df.to_dict('records'),
                'deployment_df': deploy_df.to_dict('records'),
                'base_year': int(base_year),
                'attrition_df': attrition_df.to_dict('records') if len(attrition_df) > 0 else [],
            }
            results['graphs'] = {
                'default': serialize_graph(G),
            }
            
        else:  # custom mode
            custom_durations = copy.deepcopy(durations_config) if durations_config else copy.deepcopy(DEFAULT_DURATIONS)
            custom_params = copy.deepcopy(interdependency_params) if interdependency_params else copy.deepcopy(INTERDEPENDENCY_PARAMS)
            custom_rep_sizes = rep_sizes.copy() if rep_sizes else DEFAULT_REP_SIZES.copy()
            custom_initial_queue = copy.deepcopy(initial_queue_state) if initial_queue_state else copy.deepcopy(DEFAULT_INITIAL_QUEUE_STATE)
            
            # Apply definition duration multiplier
            if definition_multiplier != 1.0:
                for tech in custom_durations:
                    if "Definition_months" in custom_durations[tech]:
                        custom_durations[tech]["Definition_months"] = int(
                            custom_durations[tech]["Definition_months"] * definition_multiplier
                        )
            
            # Ensure Transmission Construction_months uses lead_time_months if not explicitly set
            if "Transmission" in custom_durations and custom_params.get("transmission", {}).get("lead_time_months"):
                if custom_durations["Transmission"]["Construction_months"] == DEFAULT_DURATIONS["Transmission"]["Construction_months"]:
                    custom_durations["Transmission"]["Construction_months"] = custom_params["transmission"]["lead_time_months"]
            
            # Convert nested dicts to proper format
            custom_durations = {k: {kk: int(vv) if isinstance(vv, (int, float)) else vv 
                                  for kk, vv in v.items()} 
                              for k, v in custom_durations.items()}
            
            res_df, deploy_df, G, base_year, attrition_df = run_model_scenario(
                csv_path=csv_path,
                zones=zones,
                durations_config=custom_durations,
                distributions_config=distributions_config,
                rep_sizes=custom_rep_sizes,
                params=custom_params,
                initial_queue_state=custom_initial_queue,
                subtract_initial_queue=subtract_initial_queue,
            )
            
            results['custom'] = {
                'results_df': res_df.to_dict('records'),
                'deployment_df': deploy_df.to_dict('records'),
                'base_year': int(base_year),
                'attrition_df': attrition_df.to_dict('records') if len(attrition_df) > 0 else [],
            }
            results['graphs'] = {
                'custom': serialize_graph(G),
            }
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def run_model_scenario(csv_path, zones, durations_config, rep_sizes, params, 
                       initial_queue_state=None, subtract_initial_queue=True, 
                       distributions_config=None):
    """
    Run a single model scenario matching the notebook's run_model_scenario function.
    Returns: (res_df, deploy_df, G, base_year, attrition_df)
    """
    # Default initial queue state
    if initial_queue_state is None:
        initial_queue_state = {
            "Solar": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
            "Transmission": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
            "Gas Turbine": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
            "Battery": {"Definition_MW": 0.0, "Approvals_MW": 0.0, "Construction_MW": 0.0},
        }
    
    # Load targets
    solar_targets = load_solar_targets(csv_path, zones=zones, initial_queue_state=initial_queue_state, subtract_initial_queue=subtract_initial_queue)
    gas_targets = load_gas_targets(csv_path, zones=zones, initial_queue_state=initial_queue_state, subtract_initial_queue=subtract_initial_queue)
    battery_targets = load_battery_targets(csv_path, zones=zones, initial_queue_state=initial_queue_state, subtract_initial_queue=subtract_initial_queue)
    transmission_targets = load_transmission_targets(csv_path, zones=zones, initial_queue_state=initial_queue_state, subtract_initial_queue=subtract_initial_queue)
    
    # Build durations
    base_durations = create_durations_dataframe(durations_config, params)
    durations4 = build_durations_with_commissioning(base_durations)
    
    # Generate projects
    projects_all = generate_projects_multi_tech(
        solar_targets=solar_targets,
        transmission_targets=transmission_targets,
        gas_targets=gas_targets,
        battery_targets=battery_targets,
        rep_sizes=rep_sizes,
        durations_config=durations_config,
        distributions_config=distributions_config,
    )
    
    # Get base year
    if len(projects_all) > 0:
        base_year_temp = int(projects_all["YEAR"].min())
    else:
        all_years = []
        for df in [solar_targets, gas_targets, battery_targets, transmission_targets]:
            if len(df) > 0:
                all_years.extend(df["YEAR"].tolist())
        base_year_temp = min(all_years) if all_years else 2030
    
    # Generate initial queue projects
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
    
    # Combine projects
    if len(initial_queue_projects) > 0:
        if "initial_stage" not in projects_all.columns:
            projects_all["initial_stage"] = None
        projects_all = pd.concat([projects_all, initial_queue_projects], ignore_index=True)
    
    # Build DAG and run CPM
    Ginter, base_year = build_system_dag_with_interdeps(projects_all, durations4, params)
    Ginter = cpm_forward_with_seeds(Ginter, base_year)
    Ginter = relabel_joint_commissioning_nodes(Ginter)
    
    # Apply attrition (matching notebook's approach)
    RANDOM_SEED = 42  # Match notebook
    Ginter, attrition_df = apply_attrition(Ginter, base_year, params, random_seed=RANDOM_SEED)
    
    # Export results
    rows = []
    for n in Ginter.nodes:
        d = Ginter.nodes[n]
        rows.append({
            "ID": d.get("project_id", ""),
            "TECH": d.get("TECH"),
            "ZONE": d.get("ZONE"),
            "STAGE": d.get("stage"),
            "YEAR_anchor": d.get("YEAR"),
            "Capacity_MW": d.get("Capacity", 0.0),
            "duration_months": d.get("duration"),
            "t_start_month": d.get("ES"),
            "t_end_month": d.get("EF"),
            "attrited": d.get("attrited", False),
        })
    
    res_df = pd.DataFrame(rows).sort_values(["ID", "STAGE"])
    res_df["finish_year"] = res_df.apply(
        lambda r: base_year + int((r["t_end_month"] - 1) // 12) if r["t_end_month"] > 0 else base_year,
        axis=1,
    )
    
    # Build deployment DataFrame (excluding attrited projects)
    deploy_df = (
        res_df.query("STAGE == 'Commissioning' and attrited == False")
        .groupby(["TECH", "ZONE", "finish_year"], as_index=False)["Capacity_MW"].sum()
        .rename(columns={"finish_year": "YEAR", "Capacity_MW": "annual_additions_MW"})
    )
    
    return res_df, deploy_df, Ginter, base_year, attrition_df


def serialize_graph(G):
    """Convert NetworkX graph to JSON-serializable format"""
    nodes = []
    edges = []
    
    for node_id, data in G.nodes(data=True):
        nodes.append({
            'id': str(node_id),
            'project_id': data.get('project_id', ''),
            'tech': data.get('TECH', ''),
            'zone': data.get('ZONE', ''),
            'stage': data.get('stage', ''),
            'capacity': data.get('Capacity', 0.0),
            'duration': data.get('duration', 0),
            'es': data.get('ES', 0),
            'ef': data.get('EF', 0),
            'year': data.get('YEAR', 0),
        })
    
    for source, target, data in G.edges(data=True):
        edges.append({
            'source': str(source),
            'target': str(target),
            'edge_type': data.get('edge_type', ''),
        })
    
    return {'nodes': nodes, 'edges': edges}


@app.route('/api/sequencing-baseline', methods=['POST'])
def get_sequencing_baseline():
    """Get sequencing baseline (non-interdependent model) results for comparison"""
    try:
        data = request.json
        csv_path = Path(data.get('csv_path', 'capacity_delta_subset(in).csv'))
        zones_input = data.get('zones', DEFAULT_ZONES.copy())
        
        # Handle zones as string or array
        if isinstance(zones_input, str):
            zones = [z.strip() for z in zones_input.split(',') if z.strip()]
        else:
            zones = zones_input if isinstance(zones_input, list) else DEFAULT_ZONES.copy()

        # Always generate from run_sequencing_model with the requested zones.
        # Do not serve inputs/results/dag_sequencing_deployment_results.csv: that file is a
        # one-off export (often single-zone) and would ignore zones from the client.
        durations_config = DEFAULT_DURATIONS.copy()
        res_df, deploy_df, G, base_year = run_sequencing_model(
            csv_path=csv_path,
            zones=zones,
            durations_config=durations_config,
            save_results=False,
        )
        return jsonify({
            'deployment_df': deploy_df.to_dict('records'),
            'base_year': int(base_year),
            'source': 'generated',
            'zones': zones,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/download-results', methods=['POST'])
def download_results():
    """Download results as CSV"""
    try:
        data = request.json
        results_type = data.get('type')  # 'results' or 'deployment'
        scenario = data.get('scenario', 'custom')  # 'default' or 'custom'
        results_df = data.get('data', [])
        
        import pandas as pd
        df = pd.DataFrame(results_df)
        
        # Create CSV in memory
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        
        filename = f'dag_interdeps_{results_type}_{scenario}.csv'
        
        return send_file(
            io.BytesIO(output.getvalue().encode()),
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Default 5001: macOS AirPlay Receiver often binds to 5000, causing 403 / wrong responses
    # when the React proxy targets localhost:5000. Production (e.g. gunicorn) uses PORT from the environment.
    port = int(os.environ.get("PORT", "5001"))
    app.run(debug=True, port=port)
