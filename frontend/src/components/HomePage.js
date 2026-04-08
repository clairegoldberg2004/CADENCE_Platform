import React from 'react';
import './HomePage.css';

/**
 * Landing page — thesis summary and entry points to runner + gallery.
 */
export default function HomePage({ onNavigate }) {
  return (
    <div className="home-page">
      <section className="home-section home-thesis" aria-labelledby="home-thesis-heading">
        <h2 id="home-thesis-heading">Thesis</h2>
        <div className="home-text-block">
          <p className="home-text">
            Energy transition models like Net Zero Australia tell us what needs to be built and where —
            but not how it actually gets deployed. CADENCE (Capital-Aware Deployment of Energy Networks
            with Coordinated Estimation) addresses this gap by modeling clean energy deployment as an
            interdependent, friction-filled scheduling problem rather than a seamless capacity rollout.
            Using a Critical Path Method DAG framework, CADENCE stress-tests NZAu capacity trajectories
            against real-world frictions — capital discipline, technology interdependencies, and project
            abandonment — to reveal realistic deployment timelines and the bottlenecks that determine them.
          </p>
        </div>
      </section>

      <div className="home-split">
        <section className="home-section home-card" aria-labelledby="home-runner-heading">
          <h2 id="home-runner-heading">CADENCE Runner</h2>
          <div className="home-text-block home-text-block--card">
            <p className="home-text">
              Run your own deployment scenario and compare it against two baselines: Base-Level
              Sequencing, which models structural technology interdependencies alone, and Perfect
              Coordination, the frictionless NZAu benchmark. Customize parameters including stage
              durations, capital discipline, investor uncertainty, transmission split, project
              abandonment, and initial queue status to reflect real-world or counterfactual conditions.
              Results show cumulative capacity deployment, mean delays, and project abandonment rates
              across your scenario and both baselines.
            </p>
          </div>
          <button type="button" className="home-cta" onClick={() => onNavigate('runner')}>
            Open CADENCE Runner
          </button>
        </section>

        <section className="home-section home-card" aria-labelledby="home-gallery-heading">
          <h2 id="home-gallery-heading">Scenario Results Gallery</h2>
          <div className="home-text-block home-text-block--card">
            <p className="home-text">
              Explore precomputed results from the three scenarios analyzed in this thesis: S1
              (Fragmented 2026), S2 (IRA Incentive Rush), and S3 (China Coordinated Deployment). Each
              scenario walkthrough includes deployment trajectories, bottleneck analysis, ablation
              contributions, and delay distributions. Use this gallery to compare how different
              coordination regimes and incentive structures affect deployment outcomes relative to NZAu
              2050 targets.
            </p>
          </div>
          <button type="button" className="home-cta" onClick={() => onNavigate('gallery')}>
            Open Scenario Results Gallery
          </button>
        </section>
      </div>
    </div>
  );
}
