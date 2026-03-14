module.exports = {
  apps: [{
    name: "loadtest",
    script: "script.js",
    node_args: "--max-old-space-size=3072",
    autorestart: false,
    watch: false,
  }]
};
