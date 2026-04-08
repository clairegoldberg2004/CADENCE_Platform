import React, { useEffect, useRef } from 'react';
import Plotly from 'plotly.js';
import './DAGVisualization.css';

const DAGVisualization = ({ graphs, mode }) => {
  const plotRef = useRef(null);

  useEffect(() => {
    if (!graphs || !plotRef.current) return;

    const renderGraph = (graphData, title) => {
      const nodes = graphData.nodes || [];
      const edges = graphData.edges || [];

      // Group nodes by technology for color coding
      const techColors = {
        'Solar': '#FFD700',
        'Transmission': '#FF6B6B',
        'Gas Turbine': '#4ECDC4',
        'Battery': '#95E1D3',
      };

      // Prepare node positions (simple layout for now)
      const nodePositions = {};
      const techGroups = {};
      
      nodes.forEach(node => {
        const tech = node.tech || 'Unknown';
        if (!techGroups[tech]) {
          techGroups[tech] = [];
        }
        techGroups[tech].push(node.id);
      });

      // Simple grid layout
      let yPos = 0;
      Object.keys(techGroups).forEach(tech => {
        const techNodes = techGroups[tech];
        techNodes.forEach((nodeId, idx) => {
          nodePositions[nodeId] = {
            x: idx * 2,
            y: yPos,
          };
        });
        yPos += 3;
      });

      // Create edge traces
      const edgeTraces = [];
      edges.forEach(edge => {
        const sourceNode = nodes.find(n => n.id === edge.source);
        const targetNode = nodes.find(n => n.id === edge.target);
        
        if (sourceNode && targetNode) {
          const sourcePos = nodePositions[edge.source] || { x: 0, y: 0 };
          const targetPos = nodePositions[edge.target] || { x: 0, y: 0 };
          
          edgeTraces.push({
            x: [sourcePos.x, targetPos.x],
            y: [sourcePos.y, targetPos.y],
            mode: 'lines',
            line: { width: 1, color: '#888' },
            hoverinfo: 'none',
            showlegend: false,
          });
        }
      });

      // Create node trace
      const nodeX = nodes.map(n => {
        const pos = nodePositions[n.id] || { x: 0, y: 0 };
        return pos.x;
      });
      const nodeY = nodes.map(n => {
        const pos = nodePositions[n.id] || { x: 0, y: 0 };
        return pos.y;
      });
      const nodeColors = nodes.map(n => techColors[n.tech] || '#999');
      const nodeText = nodes.map(n => 
        `${n.tech} - ${n.stage}<br>${n.project_id}<br>Capacity: ${n.capacity} MW`
      );

      const nodeTrace = {
        x: nodeX,
        y: nodeY,
        mode: 'markers+text',
        type: 'scatter',
        marker: {
          size: 15,
          color: nodeColors,
          line: { width: 2, color: '#333' },
        },
        text: nodes.map(n => n.project_id),
        textposition: 'top center',
        textfont: { size: 8 },
        hovertext: nodeText,
        hoverinfo: 'text',
        name: 'Nodes',
      };

      const data = [...edgeTraces, nodeTrace];

      const layout = {
        title: title,
        showlegend: false,
        xaxis: { showgrid: false, zeroline: false, showticklabels: false },
        yaxis: { showgrid: false, zeroline: false, showticklabels: false },
        hovermode: 'closest',
        margin: { l: 50, r: 50, t: 50, b: 50 },
        height: 600,
      };

      return { data, layout };
    };

    if (mode === 'compare' && graphs.default && graphs.custom) {
      // Render side-by-side comparison
      const defaultPlot = renderGraph(graphs.default, 'Default Scenario DAG');
      const customPlot = renderGraph(graphs.custom, 'Custom Scenario DAG');
      
      const combinedData = [
        ...defaultPlot.data.map(trace => ({ ...trace, xaxis: 'x', yaxis: 'y' })),
        ...customPlot.data.map(trace => ({ ...trace, xaxis: 'x2', yaxis: 'y2' })),
      ];

      const combinedLayout = {
        ...defaultPlot.layout,
        grid: { rows: 1, columns: 2, pattern: 'independent' },
        xaxis: { ...defaultPlot.layout.xaxis, domain: [0, 0.48] },
        yaxis: { ...defaultPlot.layout.yaxis, domain: [0, 1] },
        xaxis2: { ...customPlot.layout.xaxis, domain: [0.52, 1] },
        yaxis2: { ...customPlot.layout.yaxis, domain: [0, 1] },
        height: 600,
      };

      Plotly.newPlot(plotRef.current, combinedData, combinedLayout, { responsive: true });
    } else {
      // Render single graph
      const graphData = graphs.default || graphs.custom;
      const plot = renderGraph(graphData, 'DAG Structure');
      Plotly.newPlot(plotRef.current, plot.data, plot.layout, { responsive: true });
    }

    return () => {
      // Cleanup on unmount
      if (plotRef.current) {
        Plotly.purge(plotRef.current);
      }
    };
  }, [graphs, mode]);

  return (
    <div className="dag-visualization">
      <div ref={plotRef} className="dag-plot"></div>
      <p className="dag-note">
        Interactive DAG visualization. Hover over nodes to see details. 
        This is a simplified view - the full DAG contains many more nodes and edges.
      </p>
    </div>
  );
};

export default DAGVisualization;
