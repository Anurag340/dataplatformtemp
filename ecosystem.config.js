module.exports = {
  apps: [
    // ---- API (Uvicorn) ----
    {
      name: "data-platform-api",
      cwd: "/home/ubuntu/apps/data-platform",
      script: "/home/ubuntu/apps/data-platform/venv/bin/python",
      args: [
        "-m", "uvicorn",
        "propel_data_platform.api.main:app",
        "--host", "127.0.0.1",         // use 0.0.0.0 if you need external access
        "--port", "8000",
        "--workers", "10",
        "--log-level", "debug"
      ],
      // IMPORTANT: keep PM2 in fork mode when Uvicorn uses multiple workers
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_restarts: 5,
      restart_delay: 3000,
      out_file: "/home/ubuntu/apps/data-platform/logs/api.out.log",
      error_file: "/home/ubuntu/apps/data-platform/logs/api.err.log",
      merge_logs: true
    },

    // ---- Daily cron job (runner) ----
    {
      name: "data-platform-cron",
      cwd: "/home/ubuntu/apps/data-platform",
      script: "/home/ubuntu/apps/data-platform/venv/bin/python",
      args: ["-m", "src.propel_data_platform.api.runner"],
      // run at 2 AM UTC daily
      cron_restart: "0 2 * * *",
      // job exits when done; don't loop-restart
      autorestart: false,
      watch: false,
      out_file: "/home/ubuntu/apps/data-platform/logs/cron.out.log",
      error_file: "/home/ubuntu/apps/data-platform/logs/cron.err.log",
      merge_logs: true
    }
  ]
};
