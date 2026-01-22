#!/bin/bash
set -e

echo "Installing PostgreSQL 16..."

# Add PostgreSQL APT repository
sudo apt-get install -y wget gnupg2
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

# Update and install
sudo apt-get update
sudo apt-get install -y postgresql-16 postgresql-contrib-16 postgresql-client-16

# Configure PostgreSQL for performance
sudo tee /etc/postgresql/16/main/conf.d/performance.conf > /dev/null <<EOF
# Performance tuning for benchmark
shared_buffers = 2GB
effective_cache_size = 6GB
maintenance_work_mem = 512MB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100
random_page_cost = 1.1
effective_io_concurrency = 200
work_mem = 32MB
min_wal_size = 1GB
max_wal_size = 4GB
max_connections = 200

# Logging for analysis
log_destination = 'csvlog'
logging_collector = on
log_directory = 'log'
log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'
log_min_duration_statement = 1000
log_line_prefix = '%m [%p] %q%u@%d '
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on
EOF

# Restart PostgreSQL
sudo systemctl restart postgresql

# Create benchmark database and user
sudo -u postgres psql -c "CREATE USER bench_user WITH PASSWORD 'bench_pass';"
sudo -u postgres psql -c "CREATE DATABASE bench_db OWNER bench_user;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE bench_db TO bench_user;"

# Allow local connections
echo "host    bench_db        bench_user      127.0.0.1/32            md5" | sudo tee -a /etc/postgresql/16/main/pg_hba.conf
sudo systemctl reload postgresql

# Install monitoring tools
sudo apt-get install -y sysstat htop iotop

echo "PostgreSQL 16 installed successfully!"
echo "Database: bench_db"
echo "User: bench_user"
echo "Password: bench_pass"
echo ""
echo "Connection string: postgresql://bench_user:bench_pass@localhost:5432/bench_db"
