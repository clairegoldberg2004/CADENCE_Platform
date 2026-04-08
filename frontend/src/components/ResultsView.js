import React from 'react';
import DeploymentVisualizations from './DeploymentVisualizations';
import SummaryStatistics from './SummaryStatistics';
import { LABEL_BASE_SEQUENCING_MODEL } from '../uiLabels';
import './ResultsView.css';

const ResultsView = ({ results, mode, zones }) => {
  const apiBase = process.env.REACT_APP_API_URL || '';
  
  const getModeLabel = () => {
    switch(mode) {
      case 'default':
        return `${LABEL_BASE_SEQUENCING_MODEL} mode`;
      case 'custom':
        return 'Custom Mode';
      case 'compare':
        return 'Compare Mode';
      default:
        return 'Model Results';
    }
  };

  const handleDownload = async (type, scenario = 'custom') => {
    try {
      const response = await fetch(`${apiBase}/api/download-results`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          type: type,
          scenario: scenario,
          data: results[scenario]?.[type === 'results' ? 'results_df' : 'deployment_df'] || [],
        }),
      });

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `dag_interdeps_${type}_${scenario}.csv`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (error) {
      console.error('Download failed:', error);
      alert('Download failed: ' + error.message);
    }
  };

  return (
    <div className="results-view">
      <div className="results-header">
        <h2>Model Results ({getModeLabel()})</h2>
        <div className="download-buttons">
          {mode === 'compare' ? (
            <>
              <button onClick={() => handleDownload('results', 'default')}>
                Download {LABEL_BASE_SEQUENCING_MODEL} results
              </button>
              <button onClick={() => handleDownload('deployment', 'default')}>
                Download {LABEL_BASE_SEQUENCING_MODEL} deployment
              </button>
              <button onClick={() => handleDownload('results', 'custom')}>
                Download Custom Results
              </button>
              <button onClick={() => handleDownload('deployment', 'custom')}>
                Download Custom Deployment
              </button>
            </>
          ) : (
            <>
              <button onClick={() => handleDownload('results', mode)}>
                Download Results
              </button>
              <button onClick={() => handleDownload('deployment', mode)}>
                Download Deployment
              </button>
            </>
          )}
        </div>
      </div>

      {results.default || results.custom ? (
        <div className="results-section">
          <h3>Deployment outcomes</h3>
          <DeploymentVisualizations results={results} mode={mode} zones={zones} />
        </div>
      ) : null}

      {results.default || results.custom ? (
        <div className="results-section">
          <SummaryStatistics results={results} mode={mode} />
        </div>
      ) : null}
    </div>
  );
};

export default ResultsView;
