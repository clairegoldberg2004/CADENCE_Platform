import React from 'react';
import { computeScenarioMetrics } from '../utils/runMetrics';
import { LABEL_BASE_SEQUENCING_MODEL } from '../uiLabels';
import './SummaryStatistics.css';

const SummaryStatistics = ({ results, mode }) => {
  const calculateStats = (scenario) => {
    if (!results[scenario]) return null;

    const payload = results[scenario];
    const m = computeScenarioMetrics(payload);
    if (!m) return null;

    const deploy_df = payload.deployment_df || [];
    const res_df = payload.results_df || [];

    const techTotals = {};
    deploy_df.forEach((row) => {
      const tech = row.TECH || row.tech;
      const capacity = row.annual_additions_MW || row.annual_additions_mw || 0;
      techTotals[tech] = (techTotals[tech] || 0) + capacity;
    });

    const years = deploy_df.map((row) => row.YEAR || row.year).filter((y) => y);
    const base_year = payload.base_year || 2030;
    const startYear = years.length > 0 ? Math.min(...years) : base_year;
    const endYear = years.length > 0 ? Math.max(...years) : base_year;
    const timelineYears = endYear - startYear + 1;

    const commissioningNodes = res_df.filter(
      (row) => row.STAGE === 'Commissioning' && !row.attrited
    );
    const maxFinishMonth =
      commissioningNodes.length > 0
        ? Math.max(...commissioningNodes.map((row) => row.t_end_month || 0))
        : 0;
    const makespanYears = (maxFinishMonth / 12).toFixed(1);

    return {
      avgDelay: m.delay_mean_years,
      maxDelay: m.delay_max_years,
      attritionRate: m.attrition_project_rate * 100,
      attritedProjects: m.attrited_projects,
      totalProjects: m.total_definition_projects,
      techTotals,
      startYear,
      endYear,
      timelineYears,
      makespanYears: parseFloat(makespanYears),
      totalCapacity: m.total_commissioned_MW,
      cumBy2050GW: m.cum_MW_by_2050 / 1000.0,
    };
  };

  const defaultStats = calculateStats('default');
  const customStats = calculateStats('custom');

  if (!defaultStats && !customStats) {
    return null;
  }

  const displayStats = mode === 'compare' ? customStats : defaultStats || customStats;
  const compareStats = mode === 'compare' ? defaultStats : null;

  return (
    <div className="summary-statistics">
      <h3>Summary statistics</h3>
      <p className="summary-lead">
        Delay and attrition match <code>scenario_analysis</code>: mean delay uses continuous finish time
        (non-Joint commissioning); attrition rate is attrited projects divided by Definition-stage
        projects.
      </p>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-label">Mean delay</div>
          <div className="stat-value">
            {displayStats.avgDelay.toFixed(2)} years
            {compareStats && (
              <span className="stat-diff">
                (
                {compareStats.avgDelay > displayStats.avgDelay ? '-' : '+'}
                {Math.abs(compareStats.avgDelay - displayStats.avgDelay).toFixed(2)} vs{' '}
                {LABEL_BASE_SEQUENCING_MODEL})
              </span>
            )}
          </div>
          <div className="stat-description">Non-negative slippage vs anchor (years)</div>
        </div>

        <div className="stat-card">
          <div className="stat-label">Max delay</div>
          <div className="stat-value">
            {displayStats.maxDelay.toFixed(2)} years
            {compareStats && (
              <span className="stat-diff">
                (
                {compareStats.maxDelay > displayStats.maxDelay ? '-' : '+'}
                {Math.abs(compareStats.maxDelay - displayStats.maxDelay).toFixed(2)} vs{' '}
                {LABEL_BASE_SEQUENCING_MODEL})
              </span>
            )}
          </div>
          <div className="stat-description">Worst non-Joint commissioning delay</div>
        </div>

        <div className="stat-card">
          <div className="stat-label">Attrition rate</div>
          <div className="stat-value">
            {displayStats.attritionRate.toFixed(2)}%
            {compareStats && (
              <span className="stat-diff">
                (
                {compareStats.attritionRate > displayStats.attritionRate ? '-' : '+'}
                {Math.abs(compareStats.attritionRate - displayStats.attritionRate).toFixed(2)} pp vs{' '}
                {LABEL_BASE_SEQUENCING_MODEL})
              </span>
            )}
          </div>
          <div className="stat-description">
            {displayStats.attritedProjects} attrited of {displayStats.totalProjects} Definition projects
          </div>
        </div>

        <div className="stat-card">
          <div className="stat-label">Timeline</div>
          <div className="stat-value">
            {displayStats.startYear} – {displayStats.endYear}
          </div>
          <div className="stat-description">
            {displayStats.timelineYears} calendar years ({displayStats.makespanYears} yr makespan)
          </div>
        </div>

        <div className="stat-card">
          <div className="stat-label">Total commissioned</div>
          <div className="stat-value">
            {(displayStats.totalCapacity / 1000).toFixed(3)} GW
            {compareStats && (
              <span className="stat-diff">
                (
                {compareStats.totalCapacity > displayStats.totalCapacity ? '-' : '+'}
                {Math.abs(compareStats.totalCapacity - displayStats.totalCapacity).toFixed(0)} MW vs{' '}
                {LABEL_BASE_SEQUENCING_MODEL})
              </span>
            )}
          </div>
          <div className="stat-description">Sum of annual additions (system-wide)</div>
        </div>

        <div className="stat-card">
          <div className="stat-label">Cumulative by 2050</div>
          <div className="stat-value">{displayStats.cumBy2050GW.toFixed(3)} GW</div>
          <div className="stat-description">Commissioned capacity through 2050</div>
        </div>

        <div className="stat-card stat-card-wide">
          <div className="stat-label">Capacity by technology</div>
          <div className="tech-breakdown">
            {Object.entries(displayStats.techTotals)
              .sort((a, b) => b[1] - a[1])
              .map(([tech, capacity]) => (
                <div key={tech} className="tech-item">
                  <span className="tech-name">{tech}:</span>
                  <span className="tech-capacity">{capacity.toFixed(2)} MW</span>
                  {compareStats && compareStats.techTotals[tech] !== undefined && (
                    <span className="tech-diff">
                      ({compareStats.techTotals[tech] > capacity ? '-' : '+'}
                      {Math.abs(compareStats.techTotals[tech] - capacity).toFixed(2)} MW)
                    </span>
                  )}
                </div>
              ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default SummaryStatistics;
