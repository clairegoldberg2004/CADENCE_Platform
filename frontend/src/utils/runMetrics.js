/**
 * Metrics aligned with scenario_analysis._metrics_from_outputs / _delay_stats_from_res_df
 * so the Model Runner charts match scenario_visualize.py semantics.
 */

function quantileSorted(sortedArr, q) {
  if (!sortedArr.length) return 0;
  const idx = Math.min(sortedArr.length - 1, Math.floor((sortedArr.length - 1) * q));
  return sortedArr[idx];
}

/**
 * Delay: non-negative lateness vs anchor (continuous finish from t_end_month), non-Joint commissioning, post-attrition.
 */
export function delayStatsFromResDf(res_df, baseYear) {
  if (!res_df || !res_df.length) {
    return { meanYears: 0, p90Years: 0, maxYears: 0, n: 0 };
  }
  const comm = res_df.filter(
    (r) => r.STAGE === 'Commissioning' && r.TECH !== 'Joint' && !r.attrited
  );
  if (!comm.length) {
    return { meanYears: 0, p90Years: 0, maxYears: 0, n: 0 };
  }
  const delays = comm.map((r) => {
    const tEnd = Number(r.t_end_month) || 0;
    const finishYear = baseYear + tEnd / 12.0;
    const anchor = Number(r.YEAR_anchor);
    return Math.max(0, finishYear - anchor);
  });
  const sorted = [...delays].sort((a, b) => a - b);
  const meanYears = delays.reduce((a, b) => a + b, 0) / delays.length;
  const p90Years = quantileSorted(sorted, 0.9);
  const maxYears = delays.length ? Math.max(...delays) : 0;
  return { meanYears, p90Years, maxYears, n: comm.length };
}

/**
 * Attrition rate = attrited projects / unique Definition (non-Joint) projects.
 */
export function attritionRateFromOutputs(res_df, attrition_df) {
  const ids = new Set();
  (res_df || []).forEach((r) => {
    if (r.STAGE === 'Definition' && r.TECH !== 'Joint') {
      ids.add(r.ID);
    }
  });
  const totalProjects = ids.size;
  const attrited = (attrition_df || []).length;
  const rate = totalProjects > 0 ? attrited / totalProjects : 0;
  return { attritionProjectRate: rate, attritedProjects: attrited, totalDefinitionProjects: totalProjects };
}

/**
 * System-wide annual additions and cumulative (MW), by year.
 */
export function systemAnnualFromDeployment(deployment_df) {
  const byYear = {};
  (deployment_df || []).forEach((row) => {
    const y = Number(row.YEAR || row.year);
    const mw = Number(row.annual_additions_MW ?? row.annual_additions_mw ?? 0) || 0;
    if (Number.isNaN(y)) return;
    byYear[y] = (byYear[y] || 0) + mw;
  });
  const years = Object.keys(byYear)
    .map(Number)
    .sort((a, b) => a - b);
  let cum = 0;
  const annualMW = years.map((y) => byYear[y]);
  const cumMW = years.map((y) => {
    cum += byYear[y];
    return cum;
  });
  return { years, annualMW, cumMW };
}

export function cumMwByYear(deployment_df, endYear) {
  const { years, annualMW } = systemAnnualFromDeployment(deployment_df);
  let s = 0;
  for (let i = 0; i < years.length; i++) {
    if (years[i] <= endYear) s += annualMW[i];
  }
  return s;
}

/**
 * Full headline metrics for one scenario (same columns as scenario_metrics emphasis).
 */
export function computeScenarioMetrics(scenarioPayload) {
  if (!scenarioPayload) return null;
  const { deployment_df, results_df, attrition_df, base_year: baseYearRaw } = scenarioPayload;
  const base_year = Number(baseYearRaw) || 2030;
  const sys = systemAnnualFromDeployment(deployment_df);
  const total_commissioned_MW = sys.annualMW.reduce((a, b) => a + b, 0);
  const cum_MW_by_2050 = cumMwByYear(deployment_df, 2050);
  const delayStats = delayStatsFromResDf(results_df, base_year);
  const delay_mean_years = delayStats.meanYears;
  const {
    attritionProjectRate: attrition_project_rate,
    attritedProjects: attrited_projects,
    totalDefinitionProjects: total_definition_projects,
  } = attritionRateFromOutputs(results_df, attrition_df);
  return {
    base_year,
    total_commissioned_MW,
    cum_MW_by_2050,
    delay_mean_years,
    delay_max_years: delayStats.maxYears,
    attrition_project_rate,
    attrited_projects,
    total_definition_projects,
    systemSeries: sys,
  };
}
