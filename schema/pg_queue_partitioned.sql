-- Partitioned Queue Table Schema
-- Reduces contention by distributing jobs across partitions

\c bench_db

-- Drop existing objects
DROP TABLE IF EXISTS queue_jobs_part CASCADE;
DROP FUNCTION IF EXISTS get_next_job_part CASCADE;

-- Parent partitioned table
CREATE TABLE queue_jobs_part (
    id BIGSERIAL,
    partition_key INTEGER NOT NULL,
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
    PRIMARY KEY (partition_key, id),
    CONSTRAINT valid_status_part CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
) PARTITION BY HASH (partition_key);

-- Create 16 partitions for distribution
CREATE TABLE queue_jobs_part_0 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 0);
CREATE TABLE queue_jobs_part_1 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 1);
CREATE TABLE queue_jobs_part_2 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 2);
CREATE TABLE queue_jobs_part_3 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 3);
CREATE TABLE queue_jobs_part_4 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 4);
CREATE TABLE queue_jobs_part_5 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 5);
CREATE TABLE queue_jobs_part_6 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 6);
CREATE TABLE queue_jobs_part_7 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 7);
CREATE TABLE queue_jobs_part_8 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 8);
CREATE TABLE queue_jobs_part_9 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 9);
CREATE TABLE queue_jobs_part_10 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 10);
CREATE TABLE queue_jobs_part_11 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 11);
CREATE TABLE queue_jobs_part_12 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 12);
CREATE TABLE queue_jobs_part_13 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 13);
CREATE TABLE queue_jobs_part_14 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 14);
CREATE TABLE queue_jobs_part_15 PARTITION OF queue_jobs_part FOR VALUES WITH (MODULUS 16, REMAINDER 15);

-- Create indexes on each partition
DO $$
DECLARE
    partition_name TEXT;
BEGIN
    FOR i IN 0..15 LOOP
        partition_name := 'queue_jobs_part_' || i;
        EXECUTE format('CREATE INDEX idx_%I_status_priority ON %I(status, priority DESC, created_at) WHERE status = ''pending''', 
                      partition_name, partition_name);
        EXECUTE format('CREATE INDEX idx_%I_created_at ON %I(created_at)', 
                      partition_name, partition_name);
    END LOOP;
END $$;

-- Function to get next job from specific partition
CREATE OR REPLACE FUNCTION get_next_job_part(p_partition_key INTEGER, p_worker_id VARCHAR)
RETURNS TABLE(
    job_id BIGINT,
    job_payload JSONB,
    job_priority INTEGER,
    job_created_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    UPDATE queue_jobs_part
    SET 
        status = 'processing',
        started_at = NOW(),
        attempts = attempts + 1,
        worker_id = p_worker_id
    WHERE (partition_key, id) = (
        SELECT partition_key, id
        FROM queue_jobs_part
        WHERE partition_key = p_partition_key
          AND status = 'pending'
          AND attempts < max_attempts
        ORDER BY priority DESC, created_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    RETURNING id, payload, priority, created_at;
END;
$$ LANGUAGE plpgsql;

-- Function to complete job
CREATE OR REPLACE FUNCTION complete_job_part(p_partition_key INTEGER, p_job_id BIGINT)
RETURNS BOOLEAN AS $$
BEGIN
    UPDATE queue_jobs_part
    SET 
        status = 'completed',
        completed_at = NOW()
    WHERE partition_key = p_partition_key
      AND id = p_job_id;
    
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

-- Function to fail job
CREATE OR REPLACE FUNCTION fail_job_part(p_partition_key INTEGER, p_job_id BIGINT, p_error TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    v_attempts INTEGER;
    v_max_attempts INTEGER;
BEGIN
    SELECT attempts, max_attempts INTO v_attempts, v_max_attempts
    FROM queue_jobs_part
    WHERE partition_key = p_partition_key
      AND id = p_job_id;
    
    IF v_attempts >= v_max_attempts THEN
        UPDATE queue_jobs_part
        SET 
            status = 'failed',
            completed_at = NOW(),
            error_message = p_error
        WHERE partition_key = p_partition_key
          AND id = p_job_id;
    ELSE
        UPDATE queue_jobs_part
        SET 
            status = 'pending',
            started_at = NULL,
            worker_id = NULL,
            error_message = p_error
        WHERE partition_key = p_partition_key
          AND id = p_job_id;
    END IF;
    
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL ON queue_jobs_part TO bench_user;
GRANT USAGE ON SEQUENCE queue_jobs_part_id_seq TO bench_user;

-- Monitoring view
CREATE VIEW queue_monitoring_part AS
SELECT 
    partition_key,
    status,
    COUNT(*) as count,
    AVG(EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - created_at)) * 1000) as avg_latency_ms
FROM queue_jobs_part
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY partition_key, status;

GRANT SELECT ON queue_monitoring_part TO bench_user;

\echo 'Partitioned queue schema created successfully (16 partitions)'
