-- Batched SKIP LOCKED dequeue functions (reviewer concern #3).
--
-- Shares the queue_jobs table and status enum with pg_queue_basic.sql.
-- Do not run the skip_locked_batch worker concurrently with the
-- skip_locked worker — they will race on the same rows. Choose one
-- per benchmark run.
--
-- The fair comparison point: a PG worker that fetches N rows per
-- SELECT and bulk-completes them, matching the batching behaviour of
-- the Valkey worker (XREADGROUP COUNT=50). Without this variant the
-- PG vs Valkey comparison is batched-vs-unbatched, not PG-vs-Valkey.

\c bench_db

DROP FUNCTION IF EXISTS get_next_jobs_batch CASCADE;
DROP FUNCTION IF EXISTS complete_jobs_batch CASCADE;

-- Atomically claim up to p_batch_size pending jobs.
-- Uses SKIP LOCKED so workers don't contend on the same rows.
CREATE OR REPLACE FUNCTION get_next_jobs_batch(
    p_worker_id VARCHAR,
    p_batch_size INTEGER DEFAULT 100
)
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
    WHERE id IN (
        SELECT id
        FROM queue_jobs
        WHERE status = 'pending'
          AND attempts < max_attempts
        ORDER BY priority DESC, created_at
        FOR UPDATE SKIP LOCKED
        LIMIT p_batch_size
    )
    RETURNING queue_jobs.id, queue_jobs.payload,
              queue_jobs.priority, queue_jobs.created_at;
END;
$$ LANGUAGE plpgsql;

-- Bulk-complete a batch of jobs in a single UPDATE.
CREATE OR REPLACE FUNCTION complete_jobs_batch(p_job_ids BIGINT[])
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    UPDATE queue_jobs
    SET
        status = 'completed',
        completed_at = NOW()
    WHERE id = ANY(p_job_ids);

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION get_next_jobs_batch(VARCHAR, INTEGER) TO bench_user;
GRANT EXECUTE ON FUNCTION complete_jobs_batch(BIGINT[]) TO bench_user;

\echo 'Batched SKIP LOCKED queue functions created successfully'
