import React, { useState, useEffect, useCallback } from 'react';
import './App.css';
import ParameterConfig from './components/ParameterConfig';
import ResultsView from './components/ResultsView';
import ScenarioGallery from './components/ScenarioGallery';
import HomePage from './components/HomePage';
import AboutMePage from './components/AboutMePage';
import axios from 'axios';
import { LABEL_BASE_SEQUENCING_MODEL } from './uiLabels';

function App() {
  const [page, setPage] = useState('home'); // 'home' | 'runner' | 'gallery' | 'about'
  const [mode, setMode] = useState('compare'); // 'default', 'custom', or 'compare'
  const [parameters, setParameters] = useState(null);
  const [defaults, setDefaults] = useState(null);
  const [availableZones, setAvailableZones] = useState([]);
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [autoRun, setAutoRun] = useState(false);
  const [autoRunTimeout, setAutoRunTimeout] = useState(null);

  const apiBase = process.env.REACT_APP_API_URL || '';

  const loadDefaults = useCallback(async () => {
    try {
      const response = await axios.get(`${apiBase}/api/defaults`);
      setDefaults(response.data);
      setParameters({
        zones: response.data.zones,
        durations_config: JSON.parse(JSON.stringify(response.data.durations_config)),
        interdependency_params: JSON.parse(JSON.stringify(response.data.interdependency_params)),
        rep_sizes: JSON.parse(JSON.stringify(response.data.rep_sizes)),
        initial_queue_state: JSON.parse(JSON.stringify(response.data.initial_queue_state)),
        subtract_initial_queue: true,
        definition_duration_multiplier: 1.0,
        distributions_config: null,
      });
    } catch (err) {
      setError(`Failed to load defaults: ${err.message}`);
    }
  }, [apiBase]);

  const loadAvailableZones = useCallback(async () => {
    try {
      const response = await axios.get(`${apiBase}/api/available-zones`);
      setAvailableZones(response.data.zones || []);
    } catch (err) {
      console.error('Failed to load available zones:', err);
    }
  }, [apiBase]);

  useEffect(() => {
    loadDefaults();
    loadAvailableZones();
  }, [loadDefaults, loadAvailableZones]);

  const handleRunModel = async (paramsToUse = null) => {
    const params = paramsToUse || parameters;
    if (!params) {
      setError('Parameters not loaded');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await axios.post(`${apiBase}/api/run-model`, {
        mode: mode,
        csv_path: 'capacity_delta_subset(in).csv',
        ...params,
      });

      setResults(response.data);
    } catch (err) {
      setError(`Model execution failed: ${err.message}`);
      if (err.response?.data?.error) {
        setError(err.response.data.error);
      }
    } finally {
      setLoading(false);
    }
  };

  const handleParameterChange = (updatedParams) => {
    setParameters(updatedParams);

    if (autoRun && updatedParams) {
      if (autoRunTimeout) {
        clearTimeout(autoRunTimeout);
      }

      const timeout = setTimeout(() => {
        handleRunModel(updatedParams);
      }, 1500);

      setAutoRunTimeout(timeout);
    }
  };

  useEffect(() => {
    return () => {
      if (autoRunTimeout) {
        clearTimeout(autoRunTimeout);
      }
    };
  }, [autoRunTimeout]);

  return (
    <div className="App">
      <header className="App-header">
        <div className="App-header-shell">
          <button
            type="button"
            className={`App-header-about ${page === 'about' ? 'App-header-about--active' : ''}`}
            onClick={() => setPage('about')}
          >
            About Me
          </button>
          <div className="App-header-inner">
            <h1>CADENCE Platform</h1>
            <p className="App-header-tagline">
              Energy infrastructure deployment modeling — run scenarios, compare results, and explore
              precomputed outputs.
            </p>
            <div className="App-nav">
              <button
                className={page === 'home' ? 'active' : ''}
                onClick={() => setPage('home')}
                type="button"
              >
                Home
              </button>
              <button
                className={page === 'runner' ? 'active' : ''}
                onClick={() => setPage('runner')}
                type="button"
              >
                CADENCE Runner
              </button>
              <button
                className={page === 'gallery' ? 'active' : ''}
                onClick={() => setPage('gallery')}
                type="button"
              >
                Scenario Results Gallery
              </button>
            </div>
          </div>
        </div>
      </header>

      {page === 'home' && (
        <div className="App-container App-container-full">
          <div className="App-main App-main-full">
            <HomePage onNavigate={setPage} />
          </div>
        </div>
      )}

      {page === 'runner' && (
        <div className="App-container">
          <div className="App-sidebar">
            <div className="mode-selector">
              <h2>Mode</h2>
              <div className="mode-buttons">
                <button
                  className={mode === 'default' ? 'active' : ''}
                  onClick={() => setMode('default')}
                >
                  {LABEL_BASE_SEQUENCING_MODEL}
                </button>
                <button
                  className={mode === 'custom' ? 'active' : ''}
                  onClick={() => setMode('custom')}
                >
                  Custom
                </button>
                <button
                  className={mode === 'compare' ? 'active' : ''}
                  onClick={() => setMode('compare')}
                >
                  Compare
                </button>
              </div>
              <p className="mode-description">
                {mode === 'default' &&
                  `Run the ${LABEL_BASE_SEQUENCING_MODEL} with default parameters`}
                {mode === 'custom' && 'Run model with custom parameters'}
                {mode === 'compare' &&
                  `Run ${LABEL_BASE_SEQUENCING_MODEL} and custom scenarios side-by-side`}
              </p>
            </div>

            <div className="auto-run-section">
              <label className="auto-run-toggle">
                <input
                  type="checkbox"
                  checked={autoRun}
                  onChange={(e) => {
                    setAutoRun(e.target.checked);
                    if (!e.target.checked && autoRunTimeout) {
                      clearTimeout(autoRunTimeout);
                      setAutoRunTimeout(null);
                    }
                  }}
                />
                <span>Auto-run on parameter change</span>
              </label>
              <p className="auto-run-description">
                Automatically runs the model 1.5 seconds after you stop changing parameters
              </p>
            </div>

            {defaults && parameters && (
              <ParameterConfig
                mode={mode}
                parameters={parameters}
                defaults={defaults}
                availableZones={availableZones}
                onParameterChange={handleParameterChange}
              />
            )}

            <div className="run-section">
              <button
                className="run-button"
                onClick={() => handleRunModel()}
                disabled={loading || !parameters}
              >
                {loading ? 'Running Model...' : autoRun ? 'Run Now' : 'Run Model'}
              </button>
              {autoRun && (
                <div className="auto-run-indicator">
                  {loading ? 'Running...' : 'Auto-run enabled'}
                </div>
              )}
              {error && <div className="error-message">{error}</div>}
            </div>
          </div>

          <div className="App-main">
            {results && (
              <ResultsView
                results={results}
                mode={mode}
                zones={parameters?.zones}
              />
            )}
            {!results && !loading && (
              <div className="welcome-message">
                <h2>Welcome to CADENCE</h2>
                <p>Configure your parameters, then click &quot;Run Model&quot; to see results.</p>
              </div>
            )}
            {loading && (
              <div className="loading-message">
                <h2>Running Model...</h2>
                <p>This may take a few moments...</p>
              </div>
            )}
          </div>
        </div>
      )}

      {page === 'gallery' && (
        <div className="App-container App-container-full">
          <div className="App-main App-main-full">
            <ScenarioGallery />
          </div>
        </div>
      )}

      {page === 'about' && (
        <div className="App-container App-container-full">
          <div className="App-main App-main-full">
            <AboutMePage />
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
