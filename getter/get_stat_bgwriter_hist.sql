drop function if exists fv_stats.get_stat_bgwriter_hist;

CREATE OR replace FUNCTION fv_stats.get_stat_bgwriter_hist(g_ts bigint, g_interval interval) RETURNS TABLE 
(
    begin_ts bigint,
    end_ts bigint,
    checkpoints_timed bigint,
    checkpoints_req bigint,
    checkpoint_write_time double precision,
    checkpoint_sync_time double precision,
    buffers_checkpoint bigint,
    buffers_clean bigint,
    maxwritten_clean bigint,
    buffers_backend bigint,
    buffers_backend_fsync bigint,
    buffers_alloc bigint,
    stats_reset timestamp with time zone
)
AS 
$$
BEGIN
    RETURN QUERY 
    select 
      min(sbh.ts), max(sbh.ts), 
      max(sbh.checkpoints_timed) - min(sbh.checkpoints_timed) AS checkpoints_timed,
      max(sbh.checkpoints_req) - min(sbh.checkpoints_req) AS checkpoints_req,
      max(sbh.checkpoint_write_time) - min(sbh.checkpoint_write_time) AS checkpoint_write_time,
      max(sbh.checkpoint_sync_time) - min(sbh.checkpoint_sync_time) AS checkpoint_sync_time,
      max(sbh.buffers_checkpoint) - min(sbh.buffers_checkpoint) AS buffers_checkpoint,
      max(sbh.buffers_clean) - min(sbh.buffers_clean) AS buffers_clean,
      max(sbh.maxwritten_clean) - min(sbh.maxwritten_clean) AS maxwritten_clean,
      max(sbh.buffers_backend) - min(sbh.buffers_backend) AS buffers_backend,
      max(sbh.buffers_backend_fsync) - min(sbh.buffers_backend_fsync) AS buffers_backend_fsync,
      max(sbh.buffers_alloc) - min(sbh.buffers_alloc) AS buffers_alloc,      
      max(sbh.stats_reset)      
    from 
      fv_stats.stat_bgwriter_hist  sbh
    --where sbh.ts in (select * FROM fv_stats.find_between(g_ts) fb)      
    WHERE sbh.ts IN (select ts from fv_stats.find_interval(g_ts, g_interval))
    ;    
END
$$
LANGUAGE plpgsql