-- Basic Queue Table Schema
-- Uses SELECT FOR UPDATE SKIP LOCKED pattern

\c bench_db

-- Drop existing objects
DROP TABLE IF EXISTS queue_jobs CASCADE;
DROP TABLE IF EXISTS queue_stats CASCADE;
DROP FUNCTION IF EXISTS get_next_job CASCADE;

-- Queue table
CREATE TABLE queue_jobs (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    error_message TEXT,
    worker_id VARCHAR(100),
    CONSTRAINT valid_status CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
);

-- Indexes for performance
CREATE INDEX idx_queue_jobs_status_priority ON queue_jobs(status, priority DESC, created_at) WHERE status = 'pending';
CREATE INDEX idx_queue_jobs_created_at ON queue_jobs(created_at);
CREATE INDEX idx_queue_jobs_worker ON queue_jobs(worker_id) WHERE status = 'processing';

-- Statistics table
CREATE TABLE queue_stats (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    pending_count INTEGER,
    processing_count INTEGER,
    completed_count INTEGER,
    failed_count INTEGER,
    avg_processing_time_ms NUMERIC,
    p50_latency_ms NUMERIC,
    p95_latency_ms NUMERIC,
    p99_latency_ms NUMERIC
);

-- Function to atomically get next job (SKIP LOCKED pattern)
CREATE OR REPLACE FUNCTION get_next_job(p_worker_id VARCHAR)
RETURNS TABLE(
    job_id BIGINT,
    job_payload JSONB,
    job_priority INTEGER,
    job_created_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    UPDATE queue_jobs
    SET 
        status = 'processing',
        started_at = NOW(),
        attempts = attempts + 1,
        worker_id = p_worker_id
    WHERE id = (
        SELECT id
        FROM queue_jobs
        WHERE status = 'pending'
          AND attempts < max_attempts
        ORDER BY priority DESC, created_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    RETURNING id, payload, priority, created_at;
END;
$$ LANGUAGE plpgsql;

-- Function to mark job as completed
CREATE OR REPLACE FUNCTION complete_job(p_job_id BIGINT)
RETURNS BOOLEAN AS $$
BEGIN
    UPDATE queue_jobs
    SET 
        status = 'completed',
        completed_at = NOW()
    WHERE id = p_job_id;
    
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

-- Function to mark job as failed
CREATE OR REPLACE FUNCTION fail_job(p_job_id BIGINT, p_error TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    v_attempts INTEGER;
    v_max_attempts INTEGER;
BEGIN
    SELECT attempts, max_attempts INTO v_attempts, v_max_attempts
    FROM queue_jobs
    WHERE id = p_job_id;
    
    IF v_attempts >= v_max_attempts THEN
        UPDATE queue_jobs
        SET 
            status = 'failed',
            completed_at = NOW(),
            error_message = p_error
        WHERE id = p_job_id;
    ELSE
        UPDATE queue_jobs
        SET 
            status = 'pending',
            started_at = NULL,
            worker_id = NULL,
            error_message = p_error
        WHERE id = p_job_id;
    END IF;
    
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL ON queue_jobs TO bench_user;
GRANT ALL ON queue_stats TO bench_user;
GRANT USAGE ON SEQUENCE queue_jobs_id_seq TO bench_user;
GRANT USAGE ON SEQUENCE queue_stats_id_seq TO bench_user;

-- Create view for monitoring
CREATE VIEW queue_monitoring AS
SELECT 
    status,
    COUNT(*) as count,
    AVG(EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - created_at)) * 1000) as avg_latency_ms,
    MAX(EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - created_at)) * 1000) as max_latency_ms
FROM queue_jobs
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY status;

GRANT SELECT ON queue_monitoring TO bench_user;

\echo 'Basic queue schema created successfully'
