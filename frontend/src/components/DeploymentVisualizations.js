import React, { useCallback, useEffect, useMemo, useRef } from 'react';
import Plotly from 'plotly.js';
import axios from 'axios';
import { computeScenarioMetrics } from '../utils/runMetrics';
import { LABEL_BASE_SEQUENCING_MODEL } from '../uiLabels';
import './DeploymentVisualizations.css';

const LABEL_DEFAULT = LABEL_BASE_SEQUENCING_MODEL;
const LABEL_CUSTOM = 'Custom parameters';
/** Same scenario key as scenario_analysis.build_scenarios — sequencing-only, no interdeps / attrition */
const LABEL_PERFECT =
  'Perfect coordination (PERFECT_COORDINATION)';

const COLOR_DEFAULT = '#1565C0';
const COLOR_CUSTOM = '#C62828';
const COLOR_PERFECT = '#6A1B9A';

function zonesFromRunResults(results) {
  const payload = results?.custom || results?.default;
  const df = payload?.deployment_df;
  if (!df?.length) return ['qld-north'];
  const z = [...new Set(df.map((r) => r.ZONE).filter(Boolean))];
  return z.length ? z : ['qld-north'];
}

/** Sidebar / request zones — must match what was sent to /api/run-model (not inferred from deployment rows). */
function normalizeZones(z) {
  if (!z) return null;
  if (Array.isArray(z)) return z.map(String).map((s) => s.trim()).filter(Boolean);
  if (typeof z === 'string') return z.split(',').map((s) => s.trim()).filter(Boolean);
  return null;
}

/**
 * Charts aligned with scenario_visualize.py:
 * - System-wide cumulative commissioned capacity (GW) — cumulative_deployment_curves
 * - Annual system-wide additions (GW)
 * - Absolute headline metrics — absolute_bars
 * - Compare deltas — delta_bars_vs_baseline (baseline = default)
 * - Attrition rate & mean delay — attrition_and_delay
 */
const DeploymentVisualizations = ({ results, mode, zones: zonesProp }) => {
  const cumRef = useRef(null);
  const annualRef = useRef(null);
  const absoluteRef = useRef(null);
  const deltaRef = useRef(null);
  const attritionRef = useRef(null);
  const delayRef = useRef(null);
  /** Sequencing-only run matching PERFECT_COORDINATION (see /api/sequencing-baseline) */
  const [perfectCoord, setPerfectCoord] = React.useState(null);

  React.useEffect(() => {
    setPerfectCoord(null);
  }, [results]);

  const scenarioFlags = useMemo(() => {
    if (mode === 'compare') return { def: !!results?.default, cust: !!results?.custom };
    if (mode === 'default') return { def: !!results?.default, cust: false };
    return { def: false, cust: !!results?.custom };
  }, [results, mode]);

  /** Zones for PERFECT_COORDINATION / sequencing baseline — same list as the interdependent run (sidebar), not only zones that appear in deployment_df rows. */
  const zonesForPerfectCoord = useMemo(() => {
    const fromSidebar = normalizeZones(zonesProp);
    if (fromSidebar?.length) return fromSidebar;
    return zonesFromRunResults(results);
  }, [zonesProp, results]);

  const traceLabel = useCallback((isDefault) => {
    if (mode === 'compare') return isDefault ? LABEL_DEFAULT : LABEL_CUSTOM;
    if (mode === 'default') return LABEL_DEFAULT;
    return LABEL_CUSTOM;
  }, [mode]);

  const loadPerfectCoordination = React.useCallback(async () => {
    try {
      const apiBase = process.env.REACT_APP_API_URL || '';
      const response = await axios.post(`${apiBase}/api/sequencing-baseline`, {
        csv_path: 'capacity_delta_subset(in).csv',
        zones: zonesForPerfectCoord,
      });
      if (response.data.deployment_df) {
        setPerfectCoord({
          deployment_df: response.data.deployment_df,
          base_year: Number(response.data.base_year) || 2030,
        });
      }
    } catch (error) {
      console.error('Failed to load Perfect coordination reference:', error);
    }
  }, [zonesForPerfectCoord]);

  useEffect(() => {
    if (!results) return;
    if (!perfectCoord) {
      loadPerfectCoordination();
    }
  }, [results, perfectCoord, loadPerfectCoordination, zonesForPerfectCoord]);

  useEffect(() => {
    if (!results) return;
    const mDef = scenarioFlags.def ? computeScenarioMetrics(results.default) : null;
    const mCust = scenarioFlags.cust ? computeScenarioMetrics(results.custom) : null;
    const mPerfect = perfectCoord
      ? computeScenarioMetrics({
          deployment_df: perfectCoord.deployment_df,
          results_df: [],
          attrition_df: [],
          base_year: perfectCoord.base_year,
        })
      : null;

    // 1) System cumulative (GW)
    if (cumRef.current) {
      const traces = [];
      if (mDef) {
        const { years, cumMW } = mDef.systemSeries;
        traces.push({
          x: years,
          y: cumMW.map((v) => v / 1000.0),
          type: 'scatter',
          mode: 'lines',
          name: traceLabel(true),
          line: { color: COLOR_DEFAULT, width: 2.5 },
        });
      }
      if (mCust) {
        const { years, cumMW } = mCust.systemSeries;
        traces.push({
          x: years,
          y: cumMW.map((v) => v / 1000.0),
          type: 'scatter',
          mode: 'lines',
          name: traceLabel(false),
          line: { color: COLOR_CUSTOM, width: 2.5 },
        });
      }
      if (mPerfect && (mDef || mCust)) {
        const { years, cumMW } = mPerfect.systemSeries;
        traces.push({
          x: years,
          y: cumMW.map((v) => v / 1000.0),
          type: 'scatter',
          mode: 'lines',
          name: LABEL_PERFECT,
          line: { color: COLOR_PERFECT, width: 2, dash: 'dash' },
        });
      }
      Plotly.newPlot(
        cumRef.current,
        traces,
        {
          title: {
            text:
              'System-wide cumulative commissioned capacity<br><sup style="font-size:12px">All technologies summed — same framing as scenario cumulative_deployment_curves (GW)</sup>',
          },
          xaxis: { title: 'Year' },
          yaxis: { title: 'Cumulative commissioned (GW)' },
          height: 420,
          showlegend: traces.length > 1,
          legend: { orientation: 'h', y: -0.15 },
          margin: { t: 88 },
        },
        { responsive: true }
      );
    }

    // 2) Annual system-wide (GW)
    if (annualRef.current) {
      const traces = [];
      if (mDef) {
        const { years, annualMW } = mDef.systemSeries;
        traces.push({
          x: years,
          y: annualMW.map((v) => v / 1000.0),
          type: 'scatter',
          mode: 'lines+markers',
          name: traceLabel(true),
          line: { color: COLOR_DEFAULT, width: 2 },
          marker: { size: 5 },
        });
      }
      if (mCust) {
        const { years, annualMW } = mCust.systemSeries;
        traces.push({
          x: years,
          y: annualMW.map((v) => v / 1000.0),
          type: 'scatter',
          mode: 'lines+markers',
          name: traceLabel(false),
          line: { color: COLOR_CUSTOM, width: 2 },
          marker: { size: 5 },
        });
      }
      if (mPerfect && (mDef || mCust)) {
        const { years, annualMW } = mPerfect.systemSeries;
        traces.push({
          x: years,
          y: annualMW.map((v) => v / 1000.0),
          type: 'scatter',
          mode: 'lines+markers',
          name: LABEL_PERFECT,
          line: { color: COLOR_PERFECT, width: 2, dash: 'dash' },
          marker: { size: 4 },
        });
      }
      Plotly.newPlot(
        annualRef.current,
        traces,
        {
          title: {
            text:
              'System-wide annual commissioned capacity<br><sup style="font-size:12px">Sum of annual additions across technologies (GW per year)</sup>',
          },
          xaxis: { title: 'Year' },
          yaxis: { title: 'Annual additions (GW)' },
          height: 380,
          showlegend: traces.length > 1,
          legend: { orientation: 'h', y: -0.15 },
          margin: { t: 88 },
        },
        { responsive: true }
      );
    }

    // 3) Absolute headline bars (horizontal)
    if (absoluteRef.current && (mDef || mCust)) {
      const metrics = [
        { key: 'total_commissioned_MW', label: 'Total commissioned (GW)', scale: 1 / 1000 },
        { key: 'cum_MW_by_2050', label: 'Cumulative by 2050 (GW)', scale: 1 / 1000 },
        {
          key: 'attrition_project_rate',
          label: 'Attrition rate (projects, %)',
          scale: 100,
        },
        { key: 'delay_mean_years', label: 'Mean delay vs anchor (years)', scale: 1 },
      ];
      const traces = [];
      if (mDef) {
        traces.push({
          type: 'bar',
          orientation: 'h',
          y: metrics.map((m) => m.label),
          x: metrics.map((m) => mDef[m.key] * m.scale),
          name: LABEL_DEFAULT,
          marker: { color: COLOR_DEFAULT },
        });
      }
      if (mCust) {
        traces.push({
          type: 'bar',
          orientation: 'h',
          y: metrics.map((m) => m.label),
          x: metrics.map((m) => mCust[m.key] * m.scale),
          name: LABEL_CUSTOM,
          marker: { color: COLOR_CUSTOM },
        });
      }
      if (mPerfect && (mDef || mCust)) {
        traces.push({
          type: 'bar',
          orientation: 'h',
          y: metrics.map((m) => m.label),
          x: metrics.map((m) => mPerfect[m.key] * m.scale),
          name: LABEL_PERFECT,
          marker: { color: COLOR_PERFECT },
        });
      }
      const nTraces = traces.length;
      Plotly.newPlot(
        absoluteRef.current,
        traces,
        {
          title: { text: 'Headline outcomes (absolute)' },
          xaxis: { title: 'Value', zeroline: true, zerolinewidth: 1 },
          barmode: nTraces > 1 ? 'group' : undefined,
          height: 420,
          showlegend: nTraces > 1,
          margin: { l: 200, t: 56 },
        },
        { responsive: true }
      );
    }

    // 4) Delta vs default (compare only)
    if (deltaRef.current && mode === 'compare' && mDef && mCust) {
      const dTotal =
        (mCust.total_commissioned_MW - mDef.total_commissioned_MW) / 1000.0;
      const dCum = (mCust.cum_MW_by_2050 - mDef.cum_MW_by_2050) / 1000.0;
      const dAttr =
        (mCust.attrition_project_rate - mDef.attrition_project_rate) * 100;
      const dDelay = mCust.delay_mean_years - mDef.delay_mean_years;
      const labels = [
        'Δ Total commissioned (GW)',
        'Δ Cum by 2050 (GW)',
        'Δ Attrition rate (pp)',
        'Δ Mean delay (years)',
      ];
      const vals = [dTotal, dCum, dAttr, dDelay];
      Plotly.newPlot(
        deltaRef.current,
        [
          {
            type: 'bar',
            orientation: 'h',
            y: labels,
            x: vals,
            marker: { color: vals.map((v) => (v >= 0 ? '#2E7D32' : '#AD1457')) },
          },
        ],
        {
          title: {
            text: `Change from ${LABEL_BASE_SEQUENCING_MODEL} → custom<br><sup style="font-size:12px">Same framing as delta_bars_vs_baseline (custom − baseline)</sup>`,
          },
          xaxis: { title: 'Δ (custom − baseline)', zeroline: true, zerolinewidth: 1 },
          height: 320,
          margin: { l: 220, t: 88 },
        },
        { responsive: true }
      );
    }

    if (attritionRef.current && (mDef || mCust)) {
      const scenarios = [];
      const attrPct = [];
      if (mDef) {
        scenarios.push(LABEL_BASE_SEQUENCING_MODEL);
        attrPct.push(mDef.attrition_project_rate * 100);
      }
      if (mCust) {
        scenarios.push('Custom');
        attrPct.push(mCust.attrition_project_rate * 100);
      }
      if (mPerfect && (mDef || mCust)) {
        scenarios.push('Perfect coord.');
        attrPct.push(mPerfect.attrition_project_rate * 100);
      }
      Plotly.newPlot(
        attritionRef.current,
        [
          {
            x: scenarios,
            y: attrPct,
            type: 'bar',
            marker: { color: '#5D4037' },
          },
        ],
        {
          title: 'Attrition rate (projects, %)',
          yaxis: { title: '%', rangemode: 'tozero' },
          height: 320,
          margin: { t: 56 },
        },
        { responsive: true }
      );
    }

    if (delayRef.current && (mDef || mCust)) {
      const scenarios = [];
      const delayY = [];
      if (mDef) {
        scenarios.push(LABEL_BASE_SEQUENCING_MODEL);
        delayY.push(mDef.delay_mean_years);
      }
      if (mCust) {
        scenarios.push('Custom');
        delayY.push(mCust.delay_mean_years);
      }
      if (mPerfect && (mDef || mCust)) {
        scenarios.push('Perfect coord.');
        delayY.push(mPerfect.delay_mean_years);
      }
      Plotly.newPlot(
        delayRef.current,
        [
          {
            x: scenarios,
            y: delayY,
            type: 'bar',
            marker: { color: '#00695C' },
          },
        ],
        {
          title: 'Mean delay (commissioning finish vs anchor, years)',
          yaxis: { title: 'years', rangemode: 'tozero' },
          height: 320,
          margin: { t: 56 },
        },
        { responsive: true }
      );
    }
  }, [results, mode, perfectCoord, scenarioFlags, traceLabel]);

  return (
    <div className="deployment-visualizations">
      <div className="visualization-container">
        <div className="visualization-title">System-wide cumulative capacity</div>
        <div ref={cumRef} className="plot-container" />
      </div>

      <div className="visualization-container">
        <div className="visualization-title">System-wide annual additions</div>
        <div ref={annualRef} className="plot-container" />
      </div>

      {(scenarioFlags.def || scenarioFlags.cust) && (
        <div className="visualization-container">
          <div className="visualization-title">Headline metrics (absolute)</div>
          <div ref={absoluteRef} className="plot-container" />
        </div>
      )}

      {mode === 'compare' && scenarioFlags.def && scenarioFlags.cust && (
        <div className="visualization-container">
          <div className="visualization-title">Custom vs {LABEL_BASE_SEQUENCING_MODEL} — deltas</div>
          <div ref={deltaRef} className="plot-container" />
        </div>
      )}

      {(scenarioFlags.def || scenarioFlags.cust) && (
        <div className="visualization-container viz-two-col">
          <div>
            <div className="visualization-title">Attrition rate</div>
            <div ref={attritionRef} className="plot-container" />
          </div>
          <div>
            <div className="visualization-title">Mean delay</div>
            <div ref={delayRef} className="plot-container" />
          </div>
        </div>
      )}
    </div>
  );
};

export default DeploymentVisualizations;
