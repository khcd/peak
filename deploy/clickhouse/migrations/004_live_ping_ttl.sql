-- Expire live_ping rows after 2 days, everything else after 180 days.
--
-- live_ping is a liveness heartbeat rather than analytics: every online install emits one every
-- five minutes, so it outgrows every other event type in the table by an order of magnitude, and
-- a ping is worthless once the dashboard's connected-clients window has passed. Two days leaves
-- ample room to debug a liveness problem after the fact without retaining the volume for months.
--
-- The rules are evaluated together: a live_ping row expires under the 2-day rule, and every other
-- event still falls under the unchanged 180-day rule. MODIFY TTL replaces the table's whole TTL
-- expression, so both rules must be restated here even though only the first is new.
--
-- Retention keys off occurred_at, matching the existing rule. Note that occurred_at is the client's
-- wall clock: a client that was offline for days flushes backdated pings that are already past the
-- 2-day mark and will be dropped at the next merge. That is harmless for the dashboard, which
-- measures liveness on received_at and reads those rows long before a TTL merge reaches them.

ALTER TABLE telemetry.events
MODIFY TTL toDateTime(occurred_at) + INTERVAL 2 DAY DELETE WHERE event_name = 'live_ping',
           toDateTime(occurred_at) + INTERVAL 180 DAY;
