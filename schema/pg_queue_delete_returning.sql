-- Queue Table Schema with DELETE RETURNING
-- More aggressive locking but cleaner semantics

\c bench_db

-- Drop existing objects
DROP TABLE IF EXISTS queue_jobs_dr CASCADE;
DROP TABLE IF EXISTS queue_completed_dr CASCADE;

-- Queue table (pending jobs only)
CREATE TABLE queue_jobs_dr (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3
);

-- Completed/failed jobs archive
CREATE TABLE queue_completed_dr (
    id BIGINT PRIMARY KEY,
    payload JSONB NOT NULL,
    priority INTEGER,
    created_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processing_time_ms NUMERIC,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    worker_id VARCHAR(100),
    CONSTRAINT valid_status_dr CHECK (status IN ('completed', 'failed'))
);

-- Indexes
CREATE INDEX idx_queue_jobs_dr_priority ON queue_jobs_dr(priority DESC, created_at);
CREATE INDEX idx_queue_completed_dr_created ON queue_completed_dr(created_at);
CREATE INDEX idx_queue_completed_dr_status ON queue_completed_dr(status);

-- Function to atomically get and remove next job
CREATE OR REPLACE FUNCTION get_next_job_dr(p_worker_id VARCHAR)
RETURNS TABLE(
    job_id BIGINT,
    job_payload JSONB,
    job_priority INTEGER,
    job_created_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    DELETE FROM queue_jobs_dr
    WHERE id = (
        SELECT id
        FROM queue_jobs_dr
        WHERE attempts < max_attempts
        ORDER BY priority DESC, created_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING id, payload, priority, created_at;
END;
$$ LANGUAGE plpgsql;

-- Function to mark job as completed
CREATE OR REPLACE FUNCTION complete_job_dr(
    p_job_id BIGINT,
    p_payload JSONB,
    p_priority INTEGER,
    p_created_at TIMESTAMP,
    p_worker_id VARCHAR
)
RETURNS VOID AS $$
DECLARE
    v_processing_time_ms NUMERIC;
BEGIN
    v_processing_time_ms := EXTRACT(EPOCH FROM (NOW() - p_created_at)) * 1000;
    
    INSERT INTO queue_completed_dr (
        id, payload, priority, created_at, completed_at, 
        processing_time_ms, status, worker_id
    )
    VALUES (
        p_job_id, p_payload, p_priority, p_created_at, NOW(),
        v_processing_time_ms, 'completed', p_worker_id
    );
END;
$$ LANGUAGE plpgsql;

-- Function to requeue failed job
CREATE OR REPLACE FUNCTION requeue_job_dr(
    p_job_id BIGINT,
    p_payload JSONB,
    p_priority INTEGER,
    p_created_at TIMESTAMP,
    p_attempts INTEGER,
    p_max_attempts INTEGER,
    p_error TEXT
)
RETURNS VOID AS $$
BEGIN
    IF p_attempts >= p_max_attempts THEN
        -- Permanently failed
        INSERT INTO queue_completed_dr (
            id, payload, priority, created_at, completed_at,
            status, error_message
        )
        VALUES (
            p_job_id, p_payload, p_priority, p_created_at, NOW(),
            'failed', p_error
        );
    ELSE
        -- Requeue for retry
        INSERT INTO queue_jobs_dr (id, payload, priority, created_at, attempts, max_attempts)
        VALUES (p_job_id, p_payload, p_priority, p_created_at, p_attempts + 1, p_max_attempts);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL ON queue_jobs_dr TO bench_user;
GRANT ALL ON queue_completed_dr TO bench_user;
GRANT USAGE ON SEQUENCE queue_jobs_dr_id_seq TO bench_user;

-- Monitoring view
CREATE VIEW queue_monitoring_dr AS
SELECT 
    'pending' as status,
    COUNT(*) as count,
    NULL::NUMERIC as avg_latency_ms
FROM queue_jobs_dr
UNION ALL
SELECT 
    status,
    COUNT(*),
    AVG(processing_time_ms) as avg_latency_ms
FROM queue_completed_dr
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY status;

GRANT SELECT ON queue_monitoring_dr TO bench_user;

\echo 'DELETE RETURNING queue schema created successfully'
