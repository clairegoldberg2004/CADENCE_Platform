import React, { useState, useEffect } from 'react';
import { LABEL_BASE_SEQUENCING_MODEL } from '../uiLabels';

const ParameterConfig = ({ mode, parameters, defaults, availableZones, onParameterChange }) => {
  const [localParams, setLocalParams] = useState(parameters);

  useEffect(() => {
    setLocalParams(parameters);
  }, [parameters]);

  const updateParameter = (path, value) => {
    const newParams = JSON.parse(JSON.stringify(localParams));
    const keys = path.split('.');
    let current = newParams;
    
    // Navigate to the parent of the target
    for (let i = 0; i < keys.length - 1; i++) {
      const key = keys[i];
      if (!current[key]) {
        // Check if next key is numeric (array index)
        const nextKey = keys[i + 1];
        current[key] = !isNaN(parseInt(nextKey)) ? [] : {};
      }
      current = current[key];
    }
    
    const lastKey = keys[keys.length - 1];
    // Handle array indices (for ratios like solar_to_transmission_ratio.0)
    if (!isNaN(parseInt(lastKey))) {
      const index = parseInt(lastKey);
      // Ensure it's an array, initialize from defaults if needed
      if (!Array.isArray(current)) {
        const parentPath = keys.slice(0, -1).join('.');
        const defaultVal = getDefaultValue(parentPath);
        current = Array.isArray(defaultVal) ? [...defaultVal] : [0, 0];
        // Update the parent to point to the new array
        let parent = newParams;
        for (let i = 0; i < keys.length - 2; i++) {
          parent = parent[keys[i]];
        }
        parent[keys[keys.length - 2]] = current;
      }
      current[index] = value;
    } else {
      current[lastKey] = value;
    }
    
    setLocalParams(newParams);
    onParameterChange(newParams);
  };

  const getDefaultValue = (path) => {
    const keys = path.split('.');
    let current = defaults;
    for (const key of keys) {
      if (current && typeof current === 'object') {
        // Handle array indices
        if (!isNaN(parseInt(key))) {
          const index = parseInt(key);
          if (Array.isArray(current) && index < current.length) {
            current = current[index];
          } else {
            return null;
          }
        } else if (key in current) {
          current = current[key];
        } else {
          return null;
        }
      } else {
        return null;
      }
    }
    return current;
  };

  const getCurrentValue = (path) => {
    const keys = path.split('.');
    let current = localParams;
    for (const key of keys) {
      if (current && typeof current === 'object') {
        // Handle array indices
        if (!isNaN(parseInt(key))) {
          const index = parseInt(key);
          if (Array.isArray(current) && index < current.length) {
            current = current[index];
          } else {
            return null;
          }
        } else if (key in current) {
          current = current[key];
        } else {
          return null;
        }
      } else {
        return null;
      }
    }
    return current;
  };

  const isDifferent = (path) => {
    const current = getCurrentValue(path);
    const defaultVal = getDefaultValue(path);
    return JSON.stringify(current) !== JSON.stringify(defaultVal);
  };

  const renderParameterInput = (label, path, type = 'number', step = 1) => {
    const value = getCurrentValue(path);
    const defaultValue = getDefaultValue(path);
    const different = isDifferent(path);

    return (
      <div className="parameter-group">
        <label>
          {label}
          {defaultValue !== null && (
            <span className={`parameter-comparison ${different ? 'different' : ''}`}>
              {' '}(default: {typeof defaultValue === 'object' ? JSON.stringify(defaultValue) : defaultValue})
            </span>
          )}
        </label>
        {type === 'number' && (
          <input
            type="number"
            value={value || ''}
            onChange={(e) => updateParameter(path, parseFloat(e.target.value) || 0)}
            step={step}
          />
        )}
        {type === 'text' && (
          <input
            type="text"
            value={value || ''}
            onChange={(e) => updateParameter(path, e.target.value)}
          />
        )}
        {type === 'boolean' && (
          <select
            value={value ? 'true' : 'false'}
            onChange={(e) => updateParameter(path, e.target.value === 'true')}
          >
            <option value="false">False</option>
            <option value="true">True</option>
          </select>
        )}
      </div>
    );
  };

  const technologies = ['Solar', 'Transmission', 'Gas Turbine', 'Battery'];

  return (
    <div className="parameter-config">
      {/* Zones */}
      <div className="parameter-section">
        <h3>Zones</h3>
        <div className="parameter-group">
          <label>
            Select Zones
            <span className={`parameter-comparison ${isDifferent('zones') ? 'different' : ''}`}>
              {' '}(default: {Array.isArray(defaults.zones) ? defaults.zones.join(', ') : defaults.zones})
            </span>
          </label>
          <div className="zone-checkboxes">
            {availableZones.length > 0 ? (
              availableZones.map(zone => {
                const isSelected = Array.isArray(localParams.zones) 
                  ? localParams.zones.includes(zone)
                  : (localParams.zones === zone);
                return (
                  <label key={zone} className="zone-checkbox">
                    <input
                      type="checkbox"
                      checked={isSelected}
                      onChange={(e) => {
                        const currentZones = Array.isArray(localParams.zones) 
                          ? localParams.zones 
                          : (localParams.zones ? [localParams.zones] : []);
                        let newZones;
                        if (e.target.checked) {
                          newZones = [...currentZones, zone];
                        } else {
                          newZones = currentZones.filter(z => z !== zone);
                        }
                        // Ensure at least one zone is selected
                        if (newZones.length > 0) {
                          updateParameter('zones', newZones);
                        }
                      }}
                    />
                    <span>{zone}</span>
                  </label>
                );
              })
            ) : (
              <div className="zone-loading">Loading zones...</div>
            )}
          </div>
          {mode === 'compare' && (
            <p className="zone-note">
              Note: Selected zones apply to both {LABEL_BASE_SEQUENCING_MODEL} and custom scenarios in
              compare mode.
            </p>
          )}
        </div>
      </div>

      {/* Definition Duration Multiplier */}
      <div className="parameter-section">
        <h3>Definition Duration Multiplier</h3>
        {renderParameterInput('Multiplier', 'definition_duration_multiplier', 'number', 0.1)}
        <p style={{ fontSize: '0.85em', color: '#666', marginTop: '5px' }}>
          Multiplies all Definition_months durations proportionally
        </p>
      </div>

      {/* Durations Config */}
      <div className="parameter-section">
        <h3>Stage Durations (months)</h3>
        {technologies.map(tech => (
          <div key={tech} style={{ marginBottom: '15px', padding: '10px', background: '#fff', borderRadius: '4px' }}>
            <strong>{tech}</strong>
            <div className="parameter-row">
              {renderParameterInput('Definition', `durations_config.${tech}.Definition_months`)}
              {renderParameterInput('Approvals', `durations_config.${tech}.Approvals_months`)}
              {renderParameterInput('Construction', `durations_config.${tech}.Construction_months`)}
            </div>
          </div>
        ))}
      </div>

      {/* Interdependency Parameters */}
      <div className="parameter-section">
        <h3>Interdependency Parameters</h3>
        
        <div style={{ marginBottom: '15px' }}>
          <strong>Transmission</strong>
          {renderParameterInput('Solar fraction proceed at FID', 'interdependency_params.transmission.solar_fraction_proceed_at_FID', 'number', 0.01)}
          {renderParameterInput('Solar fraction proceed at completion', 'interdependency_params.transmission.solar_fraction_proceed_at_completion', 'number', 0.01)}
          <div className="parameter-row">
            {renderParameterInput('Solar:Transmission ratio (Solar)', 'interdependency_params.transmission.solar_to_transmission_ratio.0', 'number', 0.1)}
            {renderParameterInput('Solar:Transmission ratio (Transmission)', 'interdependency_params.transmission.solar_to_transmission_ratio.1', 'number', 0.1)}
          </div>
          {renderParameterInput('Rep project size (MW)', 'interdependency_params.transmission.rep_project_size_MW', 'number', 1)}
          {renderParameterInput('Lead time (months)', 'interdependency_params.transmission.lead_time_months')}
        </div>

        <div style={{ marginBottom: '15px' }}>
          <strong>Gas</strong>
          <div className="parameter-row">
            {renderParameterInput('Solar:Gas ratio (Solar)', 'interdependency_params.gas.solar_to_gas_ratio.0', 'number', 0.1)}
            {renderParameterInput('Solar:Gas ratio (Gas)', 'interdependency_params.gas.solar_to_gas_ratio.1', 'number', 0.1)}
          </div>
        </div>

        <div style={{ marginBottom: '15px' }}>
          <strong>Battery</strong>
          <div className="parameter-row">
            {renderParameterInput('Solar:Battery ratio (Solar)', 'interdependency_params.battery.solar_to_battery_ratio.0', 'number', 0.1)}
            {renderParameterInput('Solar:Battery ratio (Battery)', 'interdependency_params.battery.solar_to_battery_ratio.1', 'number', 0.1)}
          </div>
        </div>

        <div style={{ marginBottom: '15px' }}>
          <strong>Attrition</strong>
          {renderParameterInput('Enabled', 'interdependency_params.attrition.enabled', 'boolean')}
          {renderParameterInput('Base rate', 'interdependency_params.attrition.base_rate', 'number', 0.01)}
          {renderParameterInput('Delay threshold (years)', 'interdependency_params.attrition.delay_threshold_years', 'number', 0.1)}
          {renderParameterInput('Max rate', 'interdependency_params.attrition.max_rate', 'number', 0.01)}
        </div>
      </div>

      {/* Initial Queue State */}
      <div className="parameter-section">
        <h3>Initial Queue State (MW)</h3>
        {renderParameterInput('Subtract from targets', 'subtract_initial_queue', 'boolean')}
        {technologies.map(tech => (
          <div key={tech} style={{ marginBottom: '10px', padding: '8px', background: '#fff', borderRadius: '4px' }}>
            <strong>{tech}</strong>
            <div className="parameter-row">
              {renderParameterInput('Definition', `initial_queue_state.${tech}.Definition_MW`)}
              {renderParameterInput('Approvals', `initial_queue_state.${tech}.Approvals_MW`)}
              {renderParameterInput('Construction', `initial_queue_state.${tech}.Construction_MW`)}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default ParameterConfig;
