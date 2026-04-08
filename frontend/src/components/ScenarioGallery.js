import React, { useEffect, useMemo, useRef, useState } from 'react';
import axios from 'axios';
import './ScenarioGallery.css';

const KEY_BASELINE = 'Base-level Sequencing';
const KEY_PERFECT = 'PERFECT_COORDINATION';

const SCENARIOS_MAIN = [
  'S1_REAL_WORLD_2026_FRAGMENTED',
  'S2_IRA_INCENTIVE_RUSH',
  'S3_CHINA_COORDINATED',
];

const SCENARIO_LABEL = {
  [KEY_BASELINE]: 'Base-level Sequencing',
  [KEY_PERFECT]: 'Perfect Coordination',
  S1_REAL_WORLD_2026_FRAGMENTED: 'Scenario 1 — Real World 2026: Fragmented Deployment',
  S2_IRA_INCENTIVE_RUSH: 'Scenario 2 — IRA Era 2023–2024: Incentive-Driven Rush',
  S3_CHINA_COORDINATED: 'Scenario 3 — The China Model: Coordinated Deployment',
};

const WALKTHROUGH_COPY = {
  S1_REAL_WORLD_2026_FRAGMENTED: {
    context:
      "To assess the functionality and generate relevant insights, the model includes three distinct scenarios. Each scenario represents a real-world deployment paradigm with different levels of coordination, political support, and capital discipline. Exploring these scenarios side-by-side (and individually) helps illuminate the implications of fundamentally different approaches to managing the energy transition.\n\n" +
      "Scenario 1 — Real World 2026: Fragmented Deployment — represents the current American reality: limited coordination, high investor uncertainty, and no master plan. Since 2025, federal policy shifts under the Trump Administration have slowed the pace of clean energy deployment. Negative federal attitudes toward renewables have increased market uncertainty and reduced investment. Infrastructural gaps and divergent state-level approaches have reinforced an uneven and fragmented transition.\n\n" +
      "This uncertainty and fragmentation coincides with an unprecedented surge in electricity demand in America. The country’s fragmented regulatory, permitting, and legal system threatens our ability to meet elevated demand with clean energy. As one venture capital investor put it: “Our inability to build quickly is a major problem. When NEPA reviews have a three-to-four-year time horizon, it's impossible to invest with any kind of certainty. It's not a functioning system.” Nearly a third of solar projects and half of wind projects that complete NEPA’s most rigorous environmental impact studies then face court challenges, adding years of delays and cost.\n\n" +
      "In 2026, the primary obstacle shaping the electricity sector’s trajectory is transmission—driven by permitting difficulties that one study described as “the unglamorous machinery that determines what can be built and where.” While permitting reform is central to enabling decarbonization at a reasonable pace, the path to reform is fraught with political battles and partisan divisions. This issue is compounded by national priorities that subjugate the race for clean energy to the race for AI dominance, weakening political incentives to accelerate the buildout.\n\n" +
      "Scenario 1 is designed to represent these conditions within the model by calibrating parameters toward real-world frictions and delays.",
    assumptions: 'Placeholder: Key assumptions for Scenario 1 (lognormal long tails, longer definition, higher attrition, transmission bottlenecks).',
    results: 'Placeholder: What happened under Scenario 1 compared to Base-level Sequencing and Perfect Coordination.',
    mechanisms: 'Placeholder: What mechanisms bound solar (TX, gas, intra-project, battery joint) and how that drove outcomes.',
    insights: 'Placeholder: Main insights and implications from Scenario 1.',
  },
  S2_IRA_INCENTIVE_RUSH: {
    context:
      "Scenario 2 represents a context of high political will, with strong incentives for decarbonization. At the same time, political will is time-limited and still constrained by coordination challenges. This scenario reflects the era of the Inflation Reduction Act (IRA) under the Biden Administration.\n\n" +
      "In August 2022, Congress passed the Inflation Reduction Act (IRA). The IRA represents a novel effort to decarbonize, with a key goal of reducing carbon emissions by around 40% by 2030. This included about $370 billion in funding for energy security and clean energy transition measures (IEA).\n\n" +
      "In the two years following its passage, the IRA drove rapid growth in renewable energy investments, with private infrastructure investors claiming at least $17 billion of tax equity while total IRA investments reached $115 billion (GIIA, 2024).\n\n" +
      "Importantly, the IRA credits were structured to establish clear time windows to attract private investment (UL Solutions). Tax credit structures were meant to give companies a more stable 10-year window to de-risk investment. This provision structure was meant to address the renewable struggles of the second quarter of 2022, when 32+ GW of renewables were delayed and new project development fell to its lowest levels since 2019—attributed in part to tax and policy uncertainty, along with transmission challenges (UL Solutions).\n\n" +
      "Despite the IRA’s efforts to establish a stable window for clean energy investment, subsequent IRA phaseouts still threaten to increase solar costs by 36%–55% and onshore wind by 32%–63%. This threat creates strong incentives for developers to rush their projects through earlier phases of development to reach commercial operation before incentives turn over (Deloitte, 2026).\n\n" +
      "Taken together, the IRA tax credits and their impending expiration are likely to create faster but riskier investment decisions (Columbia SIPA). IRA tax incentives can make projects feasible that may not stand on their own, especially when they carry additional risks pertaining to regulation, supply chain, and lack of supporting infrastructure (Columbia SIPA). As such, these conditions likely decrease capital discipline, allowing more projects to move through development with higher project abandonment along the way.",
    assumptions: 'Placeholder: Key assumptions for Scenario 2 (faster definition, normal durations, different TX split, higher attrition).',
    results: 'Placeholder: What happened under Scenario 2 compared to Base-level Sequencing and Perfect Coordination.',
    mechanisms: 'Placeholder: Binding constraints and bottlenecks under Scenario 2.',
    insights: 'Placeholder: Main insights and implications from Scenario 2.',
  },
  S3_CHINA_COORDINATED: {
    context:
      "The final scenario is informed by China’s remarkable clean-energy roll out. In 2024, China achieved a major milestone five years ahead of schedule: installing 80 GW of wind capacity and 277 GW of solar capacity, helping surpass the 1.2 TW renewable energy capacity target set by President Xi Jinping in 2020 (Renewable Energy Institute, 2025). China achieved this milestone by more than doubling solar and wind generation capacity from 2020–2024, from 635 GW to 1,408 GW. In doing so, combined wind and solar capacity overtook coal capacity for the first time (Ember, 2025).\n\n" +
      "This remarkable rollout constituted more than half of global additions in 2024, facilitated by investments of roughly $80 billion in power grid infrastructure (WEF, 2025). Notably, this included deliberate and extensive investment in ultra-high voltage transmission lines.\n\n" +
      "China achieved this growth through centralized coordination planning and key policies. Central to this achievement was China’s 14th Five-Year Plan (2021–2025) and the Dual Carbon Goals—together establishing decarbonization as a national priority and sending a clear signal to developers and investors (Energy Asia, 2025).\n\n" +
      "A clear example of the implications of these policy signals comes from the State Grid Corporation of China (SGCC), the world’s largest energy utility company. SGCC operates almost all of China’s transmission network (Curtis et al., 2024). According to China Daily, this grid has unlocked rapid renewable buildout and integration (China Daily, 2025).\n\n" +
      "SGCC has no plans of stopping there. The company has pledged to invest about $574 billion in building out China’s power grid between 2026 and 2030. This investment would expand the transmission grid by roughly 40%, supporting China’s transition goals by enabling additional wind and solar integration. This coordinated buildout is part of a strategic roadmap to build a “new power system” that can integrate 900 GW of distributed solar and wind power (China Daily, Jan 2026).\n\n" +
      "Importantly, China has not circumvented decarbonization bottlenecks entirely. For example, renewable capacity expansion has outpaced supporting resources like storage and flexible generation (WEF, 2025). The transmission network will also face challenges, since significant renewable potential is in the northwest while demand hubs are clustered along the eastern coast (WEF, 2025). Thus, while China’s transmission buildout and policy structures have enabled unprecedented progress, their transition does not avoid real-world frictions entirely.\n\n" +
      "Scenario 3 is inspired by China’s Five-Year Plan coordination model, which has materially realized decarbonization goals ahead of schedule. It is important to clarify, however, that this scenario is a generalized paradigm of centralized infrastructure coordination—not a direct simulation of China’s specific deployment context.",
    assumptions: 'Placeholder: Key assumptions for Scenario 3 (deterministic durations, faster definition, data-driven TX prebuild, very low attrition).',
    results: 'Placeholder: What happened under Scenario 3 compared to Base-level Sequencing and Perfect Coordination.',
    mechanisms: 'Placeholder: How transmission prebuild changed which constraints were binding and when.',
    insights: 'Placeholder: Main insights and implications from Scenario 3.',
  },
};

const ASSUMPTION_TABLES = {
  S1_REAL_WORLD_2026_FRAGMENTED: [
    {
      feature: 'Duration Distributions',
      setting: 'Enabled; lognormal for definitions and approvals',
      rationale: 'Captures long-tail delays and realistic skew in development timelines',
    },
    {
      feature: 'Definition Multiplier',
      setting: '1.3–1.5',
      rationale: 'Extended timelines due to low political certainty; reflects higher capital discipline and risk aversion',
    },
    {
      feature: 'Initial Queue',
      setting: 'Realistic 2026 pipeline data (likely sparse)',
      rationale: 'Avoids unrealistic ramp-up from zero; reflects current interconnection conditions',
    },
    {
      feature: 'Transmission Split',
      setting: '80% wait for transmission completion; 20% proceed at FID',
      rationale: 'Represents high transmission uncertainty and staggered project progression',
    },
    {
      feature: 'Attrition',
      setting: 'Enabled; higher base rate (~0.07–0.10)',
      rationale: 'Accounts for coordination failures, capital exhaustion, and development risk',
    },
    {
      feature: 'Transmission Coordination',
      setting: 'Projects must self-coordinate (TBD – still refining)',
      rationale: 'Exploring endogenous coordination constraints and dependency sequencing',
    },
  ],
  S2_IRA_INCENTIVE_RUSH: [
    {
      feature: 'Duration Distributions',
      setting: 'Normal distribution',
      rationale: 'Less skew due to strong incentives; allows for faster and more predictable decisions',
    },
    {
      feature: 'Definition Multiplier',
      setting: '0.8–0.9x',
      rationale: 'Streamlined timelines due to supportive policy environment',
    },
    {
      feature: 'Initial Queue',
      setting: 'Moderate 2023 pipeline',
      rationale: 'Reflects stronger starting development conditions',
    },
    {
      feature: 'Transmission Split',
      setting: '40% proceed at FID; 60% wait for completion',
      rationale: 'Greater confidence from political will reduces transmission-related hesitation',
    },
    {
      feature: 'Attrition',
      setting: 'Higher base rate (~0.08–0.12)',
      rationale: 'Faster decision-making reduces feasibility rigor; time pressure from expiring IRA incentives increases abandonment risk',
    },
    {
      feature: 'Capital Discipline',
      setting: 'Relaxed',
      rationale: 'Shorter early-stage timelines but higher downstream abandonment risk',
    },
  ],
  S3_CHINA_COORDINATED: [
    {
      feature: 'Duration Distributions',
      setting: 'Fixed, deterministic durations',
      rationale: 'High certainty; minimal variance in development timelines',
    },
    {
      feature: 'Definition Multiplier',
      setting: '0.7x',
      rationale: 'Streamlined, centralized approval processes',
    },
    {
      feature: 'Initial Queue',
      setting: 'Large, pre-built transmission capacity',
      rationale: 'Critical distinction from Scenarios 1 and 2; reflects substantial upfront infrastructure',
    },
    {
      feature: 'Transmission Split',
      setting: '60–70% proceed at FID',
      rationale: 'Greater ability to move forward due to coordinated transmission buildout',
    },
    {
      feature: 'Attrition',
      setting: 'Very low (~0.02–0.03)',
      rationale: 'Failures largely filtered out during early planning stages',
    },
    {
      feature: 'Dependencies',
      setting: 'Largely pre-satisfied via coordinated buildout (model translation TBD)',
      rationale: 'Reflects centralized sequencing and proactive infrastructure planning',
    },
  ],
};

const STEP_KEYS = ['context', 'assumptions', 'results', 'mechanisms', 'insights'];
const STEP_LABEL = {
  context: 'Context',
  assumptions: 'Assumptions',
  results: 'Results',
  mechanisms: 'Mechanisms',
  insights: 'Insights',
};

function isOatScenarioKey(s) {
  return String(s || '').includes('__OAT_');
}

function meanByScenario(rows) {
  if (!rows || rows.length === 0) return [];
  const by = {};
  rows.forEach(r => {
    const sc = r.scenario;
    if (!by[sc]) by[sc] = [];
    by[sc].push(r);
  });

  const numericKeys = Object.keys(rows[0]).filter(k => (
    !['seed', 'scenario', 'notes'].includes(k) && typeof rows[0][k] === 'number'
  ));

  const out = Object.keys(by).map(sc => {
    const arr = by[sc];
    const o = { scenario: sc };
    numericKeys.forEach(k => {
      const vals = arr.map(x => x[k]).filter(v => typeof v === 'number' && !Number.isNaN(v));
      const m = vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
      o[k] = m;
    });
    return o;
  });

  // stable-ish ordering
  const order = [KEY_PERFECT, KEY_BASELINE, ...SCENARIOS_MAIN];
  out.sort((a, b) => order.indexOf(a.scenario) - order.indexOf(b.scenario));
  return out;
}

// PlotCard component for displaying a single plot
const PlotCard = ({ title, caption, src, onOpen, downloadHref }) => (
  <div className="sg-plot-card">
    <div className="sg-plot-card-header">
      <div className="sg-plot-title">{title}</div>
      <div className="sg-plot-actions">
        <a className="sg-plot-action" href={downloadHref} download target="_blank" rel="noreferrer">
          Download
        </a>
        <button className="sg-plot-action sg-plot-action-btn" onClick={onOpen}>
          Zoom
        </button>
      </div>
    </div>
    {caption ? <div className="sg-plot-caption">{caption}</div> : null}
    <button className="sg-img-button" onClick={onOpen} title="Click to zoom">
      <img className="sg-img" src={src} alt={title} />
    </button>
  </div>
);

const Pill = ({ active, children, onClick, tone = 'default' }) => (
  <button
    className={`sg-pill ${active ? 'active' : ''} ${tone ? `sg-pill-${tone}` : ''}`}
    onClick={onClick}
    type="button"
  >
    {children}
  </button>
);

const Modal = ({ open, title, src, downloadHref, onClose }) => {
  if (!open) return null;
  return (
    <div className="sg-modal-backdrop" onClick={onClose}>
      <div className="sg-modal" onClick={(e) => e.stopPropagation()}>
        <div className="sg-modal-header">
          <div className="sg-modal-title">{title}</div>
          <div className="sg-modal-header-actions">
            <a className="sg-modal-download" href={downloadHref} download target="_blank" rel="noreferrer">
              Download
            </a>
            <button className="sg-modal-close" onClick={onClose}>Close</button>
          </div>
        </div>
        <div className="sg-modal-body">
          <img className="sg-modal-img" src={src} alt={title} />
        </div>
      </div>
    </div>
  );
};

const SettingsModal = ({ open, onClose, children, title = 'Customize' }) => {
  if (!open) return null;
  return (
    <div className="sg-modal-backdrop" onClick={onClose}>
      <div className="sg-settings-modal" onClick={(e) => e.stopPropagation()}>
        <div className="sg-modal-header">
          <div className="sg-modal-title">{title}</div>
          <div className="sg-modal-header-actions">
            <button className="sg-modal-close" onClick={onClose}>Close</button>
          </div>
        </div>
        <div className="sg-settings-body">
          {children}
        </div>
      </div>
    </div>
  );
};

export default function ScenarioGallery() {
  const apiBase = process.env.REACT_APP_API_URL || '';
  /** Resolved from GET /api/gallery/runs (prefers results_scenarios_with_zero when present). */
  const [run, setRun] = useState(null);
  const [runsListReady, setRunsListReady] = useState(false);
  const [manifest, setManifest] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);
  const [view, setView] = useState('summary'); // 'summary' | 'walkthrough'
  const [focus, setFocus] = useState('deployment'); // requested focus (click)
  const [activeFocus, setActiveFocus] = useState('deployment'); // scrollspy focus (highlight)
  // Default ON so the Sensitivity section shows content immediately when users switch focus to it.
  const [showSensitivity, setShowSensitivity] = useState(true);
  const [selectedScenarios, setSelectedScenarios] = useState([...SCENARIOS_MAIN]);
  const [showBaseline, setShowBaseline] = useState(true);
  const [showPerfect, setShowPerfect] = useState(true);
  const [showCustomize, setShowCustomize] = useState(false);

  const [walkScenario, setWalkScenario] = useState('S1_REAL_WORLD_2026_FRAGMENTED');
  const [walkStepIdx, setWalkStepIdx] = useState(0);

  const [modal, setModal] = useState({ open: false, title: '', src: '', downloadHref: '' });

  const sectionRefs = {
    deployment: useRef(null),
    outcomes: useRef(null),
    mechanisms: useRef(null),
    parameters: useRef(null),
    sensitivity: useRef(null),
    downloads: useRef(null),
  };

  useEffect(() => {
    let mounted = true;
    setErr(null);
    setRunsListReady(false);
    axios
      .get(`${apiBase}/api/gallery/runs`)
      .then(res => {
        const runs = res.data?.runs || [];
        const picked =
          res.data?.default_run ||
          runs[0] ||
          null;
        if (!mounted) return;
        if (!picked) {
          setErr(
            'No scenario results folders found. Add a directory under the repo root whose name starts with ' +
              'results_scenarios (for example results_scenarios_mc10) and run scenario analysis to populate it.',
          );
          setRun(null);
          return;
        }
        setRun(picked);
      })
      .catch(e => {
        if (!mounted) return;
        setErr(`Failed to list gallery runs: ${e.message}`);
        setRun(null);
      })
      .finally(() => {
        if (mounted) setRunsListReady(true);
      });
    return () => {
      mounted = false;
    };
  }, [apiBase]);

  useEffect(() => {
    if (!run) return;
    let mounted = true;
    setLoading(true);
    setErr(null);
    axios
      .get(`${apiBase}/api/gallery/run/${encodeURIComponent(run)}/manifest`)
      .then(res => {
        if (!mounted) return;
        setManifest(res.data);
      })
      .catch(e => {
        if (!mounted) return;
        setErr(`Failed to load manifest for ${run}: ${e.message}`);
      })
      .finally(() => mounted && setLoading(false));
    return () => {
      mounted = false;
    };
  }, [apiBase, run]);

  const metricsMean = useMemo(() => meanByScenario(manifest?.metrics), [manifest]);

  const fileUrl = (filename) => `${apiBase}/api/gallery/run/${encodeURIComponent(run)}/file/${encodeURIComponent(filename)}`;

  const openModal = (title, filename) => {
    const href = fileUrl(filename);
    setModal({ open: true, title, src: href, downloadHref: href });
  };

  const mainImgs = manifest?.groups?.main || [];
  const perScenarioOat = manifest?.groups?.per_scenario_oat || {};
  const parameterPages = manifest?.groups?.parameter_pages || [];
  const tornadoMW = manifest?.groups?.tornado_cum_MW_by_2050 || [];
  const tornadoDelay = manifest?.groups?.tornado_delay_mean_years || [];

  const csvs = manifest?.csvs || [];

  const isDefaultSelection =
    showBaseline === true &&
    showPerfect === true &&
    showSensitivity === true &&
    selectedScenarios.length === SCENARIOS_MAIN.length &&
    selectedScenarios.every(s => SCENARIOS_MAIN.includes(s));

  const resetToDefaultSelection = () => {
    setSelectedScenarios([...SCENARIOS_MAIN]);
    setShowBaseline(true);
    setShowPerfect(true);
    setShowSensitivity(true);
  };

  const mainSuiteKeys = useMemo(() => {
    const out = [];
    if (showPerfect) out.push(KEY_PERFECT);
    if (showBaseline) out.push(KEY_BASELINE);
    selectedScenarios.forEach(s => out.push(s));
    return out;
  }, [selectedScenarios, showBaseline, showPerfect]);

  const metricsMainSuite = useMemo(() => {
    const keep = new Set(mainSuiteKeys);
    return metricsMean.filter(r => keep.has(r.scenario) && !isOatScenarioKey(r.scenario));
  }, [metricsMean, mainSuiteKeys]);

  const walkKey = walkScenario;
  const walkStep = STEP_KEYS[walkStepIdx] || 'context';
  const walkCopy = WALKTHROUGH_COPY[walkKey] || {};
  const assumptionRows = ASSUMPTION_TABLES[walkKey] || [];
  const walkText = String(walkCopy[walkStep] || '');
  const walkParagraphs = walkText
    .split(/\n\s*\n/g)
    .map(s => s.trim())
    .filter(Boolean);

  const findImg = (filename) => mainImgs.includes(filename) ? filename : null;
  const imgCumulativeMain = findImg('cumulative_deployment_curves_main_scenarios.png') || findImg('cumulative_deployment_curves.png');
  const imgAbsolute = findImg('absolute_bars.png');
  const imgDelta = findImg('delta_bars_vs_baseline.png');
  const imgAttrDelay = findImg('attrition_and_delay.png');
  const imgBinding = findImg('solar_binding_constraint_attribution.png');
  const imgParamSummary = findImg('scenario_parameters_summary_table.png');

  const imgWalkCurves = perScenarioOat[walkKey] || null;

  const scrollTo = (key) => {
    const ref = sectionRefs[key]?.current;
    if (!ref) return;
    ref.scrollIntoView({ behavior: 'smooth', block: 'start' });
  };

  useEffect(() => {
    if (view !== 'summary') return;
    scrollTo(focus);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [focus, view]);

  // Scrollspy: highlight the nav tab for the section whose top has passed the activation line
  // (just below the sticky metric nav). Uses scroll position, not IntersectionObserver, because
  // IO callbacks only include changed entries — not reliable for "which section is current".
  useEffect(() => {
    if (view !== 'summary') return;
    const keys = ['deployment', 'outcomes', 'mechanisms', 'parameters', 'sensitivity', 'downloads'];

    let raf = 0;
    const computeActive = () => {
      const wrap = document.querySelector('.scenario-gallery .sg-metric-nav-wrap');
      const line = wrap
        ? Math.min(wrap.getBoundingClientRect().bottom + 8, window.innerHeight * 0.5)
        : window.innerHeight * 0.22;
      let current = keys[0];
      for (const k of keys) {
        const el = sectionRefs[k]?.current;
        if (!el) continue;
        if (el.getBoundingClientRect().top <= line) current = k;
      }
      setActiveFocus(prev => (prev === current ? prev : current));
    };

    const schedule = () => {
      if (raf) cancelAnimationFrame(raf);
      raf = requestAnimationFrame(() => {
        raf = 0;
        computeActive();
      });
    };

    window.addEventListener('scroll', schedule, { passive: true });
    window.addEventListener('resize', schedule);
    const galleryRoot = document.querySelector('.scenario-gallery');
    let ro;
    if (galleryRoot && typeof ResizeObserver !== 'undefined') {
      ro = new ResizeObserver(schedule);
      ro.observe(galleryRoot);
    }
    computeActive();
    const t = window.setTimeout(computeActive, 200);

    return () => {
      window.removeEventListener('scroll', schedule);
      window.removeEventListener('resize', schedule);
      if (raf) cancelAnimationFrame(raf);
      window.clearTimeout(t);
      if (ro) ro.disconnect();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [view, showSensitivity]);

  // If a user explicitly focuses on Sensitivity, ensure plots are enabled so something shows up.
  useEffect(() => {
    if (view !== 'summary') return;
    if (focus === 'sensitivity' && !showSensitivity) {
      setShowSensitivity(true);
    }
  }, [focus, showSensitivity, view]);

  const showingTags = useMemo(() => {
    const tags = [];
    if (selectedScenarios.includes('S1_REAL_WORLD_2026_FRAGMENTED')) tags.push('S1');
    if (selectedScenarios.includes('S2_IRA_INCENTIVE_RUSH')) tags.push('S2');
    if (selectedScenarios.includes('S3_CHINA_COORDINATED')) tags.push('S3');
    if (showBaseline) tags.push('Baseline');
    if (showPerfect) tags.push('Perfect coordination');
    if (showSensitivity) tags.push('Sensitivity analysis');
    return tags;
  }, [selectedScenarios, showBaseline, showPerfect, showSensitivity]);

  const ScenarioToggle = ({ sc }) => {
    const checked = selectedScenarios.includes(sc);
    return (
      <Pill
        active={checked}
        tone="scenario"
        onClick={() => {
          if (checked) {
            setSelectedScenarios(selectedScenarios.filter(x => x !== sc));
          } else {
            setSelectedScenarios([...selectedScenarios, sc]);
          }
        }}
      >
        {SCENARIO_LABEL[sc] || sc}
      </Pill>
    );
  };

  const Kpi = ({ label, value, sub }) => (
    <div className="sg-kpi">
      <div className="sg-kpi-label">{label}</div>
      <div className="sg-kpi-value">{value}</div>
      {sub ? <div className="sg-kpi-sub">{sub}</div> : null}
    </div>
  );

  const ScenarioKpiCard = ({ row, tone }) => {
    const sc = row.scenario;
    const label = SCENARIO_LABEL[sc] || sc;
    const totalGW = (row.total_commissioned_MW / 1000.0);
    const cum2050 = (row.cum_MW_by_2050 / 1000.0);
    const attr = (row.attrition_project_rate * 100.0);
    const delay = row.delay_mean_years;
    return (
      <div className={`sg-scenario-card ${tone || ''}`}>
        <div className="sg-scenario-card-title">{label}</div>
        <div className="sg-kpi-grid">
          <Kpi label="Total commissioned" value={`${totalGW.toFixed(1)} GW`} sub="All techs" />
          <Kpi label="Cum by 2050" value={`${cum2050.toFixed(1)} GW`} sub="All techs" />
          <Kpi label="Attrition" value={`${attr.toFixed(1)}%`} sub="Projects" />
          <Kpi label="Mean delay" value={`${delay.toFixed(2)} yrs`} sub="Commissioning vs target" />
        </div>
      </div>
    );
  };

  const Section = ({ id, title, subtitle, children }) => (
    <section className="sg-section" ref={sectionRefs[id]}>
      <div className="sg-section-header">
        <div className="sg-section-title">{title}</div>
        {subtitle ? <div className="sg-section-subtitle">{subtitle}</div> : null}
      </div>
      {children}
    </section>
  );

  const MetricNav = () => {
    const items = [
      { key: 'deployment', label: 'Deployment' },
      { key: 'outcomes', label: 'Outcomes' },
      { key: 'mechanisms', label: 'Mechanisms' },
      { key: 'parameters', label: 'Parameters' },
      { key: 'sensitivity', label: 'Sensitivity analysis' },
      { key: 'downloads', label: 'Downloads' },
    ];
    return (
      <div className="sg-metric-nav" role="navigation" aria-label="Metric focus navigation">
        {items.map(it => (
          <button
            key={it.key}
            className={`sg-metric-tab ${activeFocus === it.key ? 'active' : ''}`}
            onClick={() => {
              setActiveFocus(it.key);
              setFocus(it.key);
            }}
            type="button"
          >
            {it.label}
          </button>
        ))}
      </div>
    );
  };

  return (
    <div className="scenario-gallery">
      <div className="sg-topbar">
        <div className="sg-topbar-left">
          <h2>Scenario Results Gallery</h2>
          {run ? (
            <div className="sg-run-label" title="Precomputed scenario-analysis output folder">
              Showing run: <code>{run}</code>
            </div>
          ) : null}
        </div>
        <div className="sg-topbar-right">
          <div className="sg-view-tabs">
            <button className={view === 'summary' ? 'active' : ''} onClick={() => setView('summary')}>
              Summary Explorer
            </button>
            <button className={view === 'walkthrough' ? 'active' : ''} onClick={() => setView('walkthrough')}>
              Scenario Walkthrough
            </button>
          </div>
        </div>
      </div>

      {(!runsListReady || loading) && (
        <div className="sg-banner sg-banner-info">Loading gallery…</div>
      )}
      {err && (
        <div className="sg-banner sg-banner-error">{err}</div>
      )}

      {view === 'summary' ? (
        <>
          <div className="sg-presets">
            <div className="sg-presets-left">
              <div className="sg-presets-title">Showing</div>
              <div className="sg-presets-sub">
                {isDefaultSelection ? (
                  <>All scenarios + references</>
                ) : (
                  <>Custom selection</>
                )}
              </div>
              <div className="sg-presets-tags">
                {showingTags.map(t => (
                  <span key={t} className="sg-tag">{t}</span>
                ))}
              </div>
            </div>
            <div className="sg-presets-right">
              <button className="sg-secondary" onClick={() => setShowCustomize(true)}>
                Customize…
              </button>
              {!isDefaultSelection ? (
                <button className="sg-secondary" onClick={resetToDefaultSelection}>
                  Reset
                </button>
              ) : null}
            </div>
          </div>

          <div className="sg-metric-nav-wrap">
            <MetricNav />
          </div>

          <SettingsModal open={showCustomize} onClose={() => setShowCustomize(false)} title="Customize view">
            <div className="sg-settings-section">
              <div className="sg-settings-label">Scenarios</div>
              <div className="sg-pill-row">
                <ScenarioToggle sc="S1_REAL_WORLD_2026_FRAGMENTED" />
                <ScenarioToggle sc="S2_IRA_INCENTIVE_RUSH" />
                <ScenarioToggle sc="S3_CHINA_COORDINATED" />
              </div>
            </div>
            <div className="sg-settings-section">
              <div className="sg-settings-label">References (compare against)</div>
              <div className="sg-pill-row">
                <Pill active={showBaseline} tone="ref" onClick={() => setShowBaseline(!showBaseline)}>
                  Baseline (Base-level Sequencing)
                </Pill>
                <Pill active={showPerfect} tone="ref" onClick={() => setShowPerfect(!showPerfect)}>
                  Perfect coordination
                </Pill>
              </div>
            </div>
            <div className="sg-settings-section">
              <div className="sg-settings-label">Sensitivity analysis</div>
              <div className="sg-pill-row">
                <Pill active={showSensitivity} tone="default" onClick={() => setShowSensitivity(!showSensitivity)}>
                  Show sensitivity charts (tornado)
                </Pill>
              </div>
              <div className="sg-muted">Tip: Use this if you want to see which parameter blocks drive outcomes.</div>
            </div>
          </SettingsModal>

          <Section
            id="deployment"
            title="At a glance"
            subtitle="Headline metrics for the selected scenarios. Base-level Sequencing and Perfect Coordination are shown as references unless you change the checkboxes."
          >
            {!metricsMainSuite.length ? (
              <div className="sg-muted">No <code>scenario_metrics.csv</code> found for this run.</div>
            ) : (
              <div className="sg-scenario-cards">
                {metricsMainSuite.map(r => {
                  const tone = r.scenario === KEY_BASELINE || r.scenario === KEY_PERFECT ? 'sg-tone-ref' : 'sg-tone-main';
                  return <ScenarioKpiCard key={r.scenario} row={r} tone={tone} />;
                })}
              </div>
            )}
          </Section>

          <Section
            id="outcomes"
            title="Deployment overview"
            subtitle="System-wide cumulative deployment trajectories. Includes Base-level Sequencing and Perfect Coordination when selected."
          >
            {imgCumulativeMain ? (
              <PlotCard
                title="Cumulative deployment (main scenarios)"
                caption="Compare Scenario 1/2/3 against Base-level Sequencing and Perfect Coordination."
                src={fileUrl(imgCumulativeMain)}
                downloadHref={fileUrl(imgCumulativeMain)}
                onOpen={() => openModal('Cumulative deployment (main scenarios)', imgCumulativeMain)}
              />
            ) : <div className="sg-muted">No cumulative curves PNG found in this run.</div>}
          </Section>

          <Section
            id="mechanisms"
            title="Outcomes + mechanisms"
            subtitle="What happened (capacity, delay, attrition) and what constraints were binding."
          >
            <div className="sg-two-col">
              <div>
                {imgAbsolute ? (
                  <PlotCard
                    title="Absolute outcomes"
                    caption="Absolute totals (not deltas): capacity by 2050, attrition, and mean delay vs target year."
                    src={fileUrl(imgAbsolute)}
                    downloadHref={fileUrl(imgAbsolute)}
                    onOpen={() => openModal('Absolute outcomes', imgAbsolute)}
                  />
                ) : null}
                {imgAttrDelay ? (
                  <PlotCard
                    title="Attrition + delay (absolute)"
                    caption="Mean delay is measured at commissioning finish vs target year."
                    src={fileUrl(imgAttrDelay)}
                    downloadHref={fileUrl(imgAttrDelay)}
                    onOpen={() => openModal('Attrition + delay', imgAttrDelay)}
                  />
                ) : null}
              </div>
              <div>
                {imgBinding ? (
                  <PlotCard
                    title="What binds solar?"
                    caption="Shares of solar projects whose schedule is bound by transmission/gas/battery joint vs intra-project sequencing."
                    src={fileUrl(imgBinding)}
                    downloadHref={fileUrl(imgBinding)}
                    onOpen={() => openModal('Solar binding constraint attribution', imgBinding)}
                  />
                ) : null}
                {imgDelta ? (
                  <PlotCard
                    title="Deltas vs Base-level Sequencing (optional)"
                    caption="This is scenario − baseline. Negative delay delta means lower delay than the baseline."
                    src={fileUrl(imgDelta)}
                    downloadHref={fileUrl(imgDelta)}
                    onOpen={() => openModal('Deltas vs baseline', imgDelta)}
                  />
                ) : null}
              </div>
            </div>
          </Section>

          <Section
            id="parameters"
            title="Scenario parameters"
            subtitle="Key knobs used for each scenario (placeholder narrative for now)."
          >
            {imgParamSummary ? (
              <PlotCard
                title="Parameter summary (key knobs)"
                caption="Click to zoom. Full parameter table pages are available below."
                src={fileUrl(imgParamSummary)}
                downloadHref={fileUrl(imgParamSummary)}
                onOpen={() => openModal('Scenario parameter summary', imgParamSummary)}
              />
            ) : null}
            {parameterPages.length ? (
              <div className="sg-inline-actions">
                <button
                  className="sg-secondary"
                  onClick={() => openModal('Scenario parameters (full table)', parameterPages[0])}
                >
                  Open full parameter tables
                </button>
                <div className="sg-muted">Tip: Use the zoom modal’s Download button for any page.</div>
              </div>
            ) : null}
          </Section>

          <Section
            id="sensitivity"
            title="Sensitivity analysis"
            subtitle="How much each parameter block contributes to outcomes (tornado charts)."
          >
            {!showSensitivity ? (
              <div className="sg-muted">Turn on sensitivity charts in Customize to view tornado charts.</div>
            ) : (
              <>
                <div className="sg-grid">
                  {tornadoMW.map(fn => (
                    <PlotCard
                      key={fn}
                      title={fn}
                      caption=""
                      src={fileUrl(fn)}
                      downloadHref={fileUrl(fn)}
                      onOpen={() => openModal(fn, fn)}
                    />
                  ))}
                  {tornadoDelay.map(fn => (
                    <PlotCard
                      key={fn}
                      title={fn}
                      caption=""
                      src={fileUrl(fn)}
                      downloadHref={fileUrl(fn)}
                      onOpen={() => openModal(fn, fn)}
                    />
                  ))}
                </div>
              </>
            )}
          </Section>

          <Section
            id="downloads"
            title="Downloads"
            subtitle="Download the underlying CSVs for this run."
          >
            <div className="sg-downloads">
              {csvs.map(fn => (
                <a key={fn} className="sg-download" href={fileUrl(fn)} download={fn} target="_blank" rel="noreferrer">
                  {fn}
                </a>
              ))}
              {csvs.length === 0 && <div className="sg-muted">No CSVs found for this run.</div>}
            </div>
          </Section>
        </>
      ) : (
        <>
          <div className="sg-walk-top">
            <div className="sg-walk-left">
              <div className="sg-walk-title">Scenario Walkthrough</div>
              <div className="sg-walk-sub">
                Each scenario is compared to <strong>{SCENARIO_LABEL[KEY_BASELINE]}</strong> and <strong>{SCENARIO_LABEL[KEY_PERFECT]}</strong> by default.
              </div>
            </div>
            <div className="sg-walk-right">
              <label className="sg-field">
                Scenario
                <select
                  value={walkScenario}
                  onChange={(e) => {
                    setWalkScenario(e.target.value);
                    setWalkStepIdx(0);
                  }}
                >
                  {SCENARIOS_MAIN.map(k => (
                    <option key={k} value={k}>{SCENARIO_LABEL[k]}</option>
                  ))}
                </select>
              </label>
            </div>
          </div>

          <div className="sg-stepper">
            {STEP_KEYS.map((k, idx) => (
              <button
                key={k}
                className={`sg-step ${idx === walkStepIdx ? 'active' : ''} ${idx < walkStepIdx ? 'done' : ''}`}
                onClick={() => setWalkStepIdx(idx)}
              >
                <span className="sg-step-num">{idx + 1}</span>
                <span className="sg-step-label">{STEP_LABEL[k]}</span>
              </button>
            ))}
          </div>

          <div className="sg-walk-body">
            <div className="sg-walk-h1">{SCENARIO_LABEL[walkKey]}</div>
            <div className="sg-walk-copy">
              {walkParagraphs.map((p, idx) => (
                <p key={idx}>{p}</p>
              ))}
            </div>

            <div className="sg-walk-nav sg-walk-nav-top">
              <button
                className="sg-secondary"
                onClick={() => setWalkStepIdx(Math.max(0, walkStepIdx - 1))}
                disabled={walkStepIdx === 0}
              >
                Back
              </button>
              <button
                className="sg-primary sg-primary-lg"
                onClick={() => setWalkStepIdx(Math.min(STEP_KEYS.length - 1, walkStepIdx + 1))}
                disabled={walkStepIdx === STEP_KEYS.length - 1}
              >
                Next
              </button>
            </div>

            {walkStep === 'context' ? (
              null
            ) : null}

            {walkStep === 'assumptions' ? (
              <>
                <div className="sg-assumptions">
                  <div className="sg-assumptions-title">Scenario assumptions (summary)</div>
                  <div className="sg-assumptions-sub">
                    Placeholder: you can add deeper narrative here. Table below uses the thesis-facing definitions you provided.
                  </div>
                  <div className="sg-assumptions-table-wrap">
                    <table className="sg-assumptions-table">
                      <thead>
                        <tr>
                          <th>Parameter / Feature</th>
                          <th>Setting / Assumption</th>
                          <th>Rationale / Notes</th>
                        </tr>
                      </thead>
                      <tbody>
                        {assumptionRows.map((r, i) => (
                          <tr key={`${r.feature}-${i}`}>
                            <td className="sg-td-feature">{r.feature}</td>
                            <td>{r.setting}</td>
                            <td>{r.rationale}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </>
            ) : null}

            {walkStep === 'results' ? (
              <>
                {imgWalkCurves ? (
                  <PlotCard
                    title="Cumulative deployment (scenario vs references)"
                    caption="Includes Base-level Sequencing + Perfect Coordination as dashed references."
                    src={fileUrl(imgWalkCurves)}
                    downloadHref={fileUrl(imgWalkCurves)}
                    onOpen={() => openModal('Cumulative deployment (scenario vs references)', imgWalkCurves)}
                  />
                ) : null}
                <div className="sg-two-col">
                  <div>
                    {imgAbsolute ? (
                      <PlotCard
                        title="Absolute outcomes"
                        caption="Total commissioned, cumulative by 2050, attrition, and delay."
                        src={fileUrl(imgAbsolute)}
                        downloadHref={fileUrl(imgAbsolute)}
                        onOpen={() => openModal('Absolute outcomes', imgAbsolute)}
                      />
                    ) : null}
                  </div>
                  <div>
                    {imgAttrDelay ? (
                      <PlotCard
                        title="Attrition + delay"
                        caption="Delay measured at commissioning finish vs target year."
                        src={fileUrl(imgAttrDelay)}
                        downloadHref={fileUrl(imgAttrDelay)}
                        onOpen={() => openModal('Attrition + delay', imgAttrDelay)}
                      />
                    ) : null}
                  </div>
                </div>
              </>
            ) : null}

            {walkStep === 'mechanisms' ? (
              <>
                {imgBinding ? (
                  <PlotCard
                    title="Solar binding constraint attribution"
                    caption="Which dependencies actually bind Solar construction/commissioning."
                    src={fileUrl(imgBinding)}
                    downloadHref={fileUrl(imgBinding)}
                    onOpen={() => openModal('Solar binding constraint attribution', imgBinding)}
                  />
                ) : null}
                {imgDelta ? (
                  <PlotCard
                    title="Deltas vs Base-level Sequencing"
                    caption="Useful for quick ‘better/worse’ comparison relative to baseline."
                    src={fileUrl(imgDelta)}
                    downloadHref={fileUrl(imgDelta)}
                    onOpen={() => openModal('Deltas vs baseline', imgDelta)}
                  />
                ) : null}
              </>
            ) : null}

            {walkStep === 'insights' ? (
              <>
                <div className="sg-insights">
                  <div className="sg-insight-card">
                    <div className="sg-insight-title">Major findings</div>
                    <div className="sg-insight-body">Placeholder: bullet list of major findings.</div>
                  </div>
                  <div className="sg-insight-card">
                    <div className="sg-insight-title">Interpretation</div>
                    <div className="sg-insight-body">Placeholder: narrative interpretation of why results look this way.</div>
                  </div>
                  <div className="sg-insight-card">
                    <div className="sg-insight-title">Implications</div>
                    <div className="sg-insight-body">Placeholder: policy / system design implications.</div>
                  </div>
                </div>
              </>
            ) : null}

          </div>
        </>
      )}

      <Modal
        open={modal.open}
        title={modal.title}
        src={modal.src}
        downloadHref={modal.downloadHref}
        onClose={() => setModal({ open: false, title: '', src: '', downloadHref: '' })}
      />
    </div>
  );
}

