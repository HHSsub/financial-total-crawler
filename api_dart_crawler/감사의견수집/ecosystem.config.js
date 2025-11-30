module.exports = {
    apps: [{
      name: 'dart-collector',
      script: '/home/ec2-user/collector/run_collector.sh',
      interpreter: '/bin/bash',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '2G',
      error_file: '/home/ec2-user/collector/logs/error.log',
      out_file: '/home/ec2-user/collector/logs/output.log',
      time: true,
      env: {
        PYTHONUNBUFFERED: '1'
      },
      min_uptime: '10s',
      max_restarts: 50
    }]
  };