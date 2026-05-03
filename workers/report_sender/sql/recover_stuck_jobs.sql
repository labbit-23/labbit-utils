-- Recover stuck auto-dispatch jobs safely.
-- Adjust thresholds below as needed.
with latest_events as (
  select distinct on (e.job_id)
    e.job_id, e.event_type, e.created_at
  from public.report_auto_dispatch_events e
  order by e.job_id, e.created_at desc
),
targets as (
  select j.id, j.reqno, j.reqid, j.phone, j.status, le.event_type, le.created_at as event_at
  from public.report_auto_dispatch_jobs j
  join latest_events le on le.job_id = j.id
  where j.sent_at is null
    and j.status in ('queued','cooling_off','eligible','retrying')
    and le.event_type in ('queued_wait','cooling_off')
    and (
      (le.event_type = 'queued_wait' and le.created_at < now() - interval '6 hour')
      or
      (le.event_type = 'cooling_off' and le.created_at < now() - interval '2 hour')
    )
)
update public.report_auto_dispatch_jobs j
set status = 'queued',
    next_attempt_at = now(),
    updated_at = now()
from targets t
where j.id = t.id;

insert into public.report_auto_dispatch_events
  (job_id, reqno, reqid, phone, event_type, message, payload, created_at)
select
  t.id,
  t.reqno,
  t.reqid,
  t.phone,
  'admin_requeue',
  'Admin SQL recovery requeued stuck job',
  jsonb_build_object('previous_status', t.status, 'latest_event_type', t.event_type, 'latest_event_at', t.event_at),
  now()
from targets t;

