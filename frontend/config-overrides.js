const webpack = require('webpack');

module.exports = function override(config, env) {
  config.resolve.fallback = {
    ...config.resolve.fallback,
    "stream": require.resolve("stream-browserify"),
    "assert": require.resolve("assert"),
    "buffer": require.resolve("buffer"),
    "process": require.resolve("process/browser"),
  };
  
  config.resolve.alias = {
    ...config.resolve.alias,
    "process/browser": require.resolve("process/browser"),
  };
  
  config.plugins = [
    ...config.plugins,
    new webpack.ProvidePlugin({
      Buffer: ['buffer', 'Buffer'],
      process: 'process/browser',
    }),
  ];
  
  // Suppress source map warnings for missing map files
  config.ignoreWarnings = [
    /Failed to parse source map/,
    /ENOENT: no such file or directory/,
  ];
  
  return config;
};
