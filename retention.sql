-- CREATE A TEMPORARY VIEW OF THE USER TABLE without experimental sessions.
DROP TABLE IF EXISTS first_time_group CASCADE;
CREATE TABLE first_time_group AS
SELECT * 
FROM 
 first_time_londoners AS f
-- first_time_row AS f
WHERE NOT EXISTS(
  SELECT userid, session 
  FROM experimental_sessions
  WHERE userid=f.userid AND session=f.session
);


-- 3.1  RETENTION
-- Number of sessions per day (average) of unique users in London
-- E.g.: 1st day 5 sessions, 2nd day, 8 sessions, 3rd day 6 sessions, 4th day 3 sessions.
-- 3.3 RETENTION
-- Average time spent per session and per day in the app
-- E.g.:  1st day 600 sec, 2nd day, 500 sec , 3rd day 100 sec, 4th day 20 sec….

-- Beginning session time for each user
CREATE OR REPLACE TEMP VIEW  user_session_times AS
SELECT userid, session_started, session_duration,
    dense_rank() OVER (PARTITION BY userid 
                ORDER BY date_trunc('day', session_started)) AS active_day
FROM (
  SELECT 
    userid, session, MIN(created_at) AS session_started, 
    MAX(created_at) - MIN(created_at) AS session_duration
  FROM first_time_group WHERE event != 'MapModeEnd'
  GROUP BY userid, session
  ORDER BY userid, session
) temp
WHERE session_duration < interval '0.5 hour';

-- userid, active_day (1,2,3), total_usage_per_day, avg_usage_per_day
CREATE OR REPLACE TEMP VIEW temp_stats AS
SELECT 
  userid, 
  active_day, 
  SUM(session_duration) AS total_usage_per_day,
  AVG(session_duration) AS avg_usage_per_day,
  COUNT(active_day) AS sessions_per_day
FROM user_session_times
GROUP BY userid, active_day
ORDER BY userid, active_day;

-- FINAL ANSWERS
-- Boxplots for:
--    - number of sessions per day
--    - avg session per day
--    - total usage per day
SELECT 
  active_day, 
  boxplot(sessions_per_day) AS sessions_per_day_boxplot,
  boxplot(EXTRACT(EPOCH FROM avg_usage_per_day)) AS avg_usage_per_day_boxplot,
  boxplot(EXTRACT(EPOCH FROM total_usage_per_day)) AS total_usage_per_day_boxplot,
  COUNT(userid) AS unique_users
FROM temp_stats
GROUP BY active_day
ORDER BY active_day;

------------------------------------------------
-- Internal user id's. No need to exclude them
------------------------------------------------
-- WHERE userid NOT IN 
--  ('FCC006B8-DB05-494B-AE51-27AB67621B76',
--  '113B9F26-578B-4164-B25C-01B5C1E136AC',
--  '4E21C850-F43F-4017-9F5A-8E4DFD318118',
--  'B405E7CC-CFFB-4580-A799-BE565B614338',
--  '7D6D5162-3A5D-4760-9258-780697EFBF54',
-- '2AACDF05-DB29-41A8-B2B2-50DEA6233A0E',
--  '2f5672cb76691b989bbd2022a5349939a2d7b952',
--  'ECDC246D-39F6-4D90-8F75-4C64D8E05B38')



-------------------------------------------------------------------
-- RETENTION 3.2: Number of unique users that come back every day. 
-- NOTE: comes also with a report of users that went offline at some point.
-------------------------------------------------------------------
SELECT SETVAL('serial', 1);
WITH network_groups AS (
  SELECT userid, date_trunc('day', MIN(created_at)) AS offline_moment, COUNT(userid) AS offline_times
  FROM (
    SELECT 
      userid, event, created_at, session, activation_time, network,
      CASE
        WHEN network IS FALSE THEN
          CASE 
            WHEN lag(network) OVER (PARTITION BY userid ORDER BY created_at) IS FALSE THEN currval('serial')-1
            ELSE nextval('serial')-1
          END
        ELSE NULL
      END AS network_group
    FROM first_time_group
    WHERE 
      event NOT IN ('AppStartup', 'MapModeEnd', 'ItineraryAdd2', 
                    'ItineraryRemove2', 'RadarActivated', 'RadarDeactivated',
                    'OnBoardingTutorialDone') 
  ) t
  WHERE network_group IS NOT NULL
  GROUP BY userid, network_group
), retention_days AS (
  SELECT 
    ust.userid, 
    EXTRACT(day FROM ust.session_started - first_day) AS came_back_day,
    CASE 
      WHEN EXISTS (
                    SELECT 1 FROM network_groups 
                    WHERE 
                      userid = ust.userid AND 
                      offline_moment = date_trunc('day', ust.session_started) AND 
                      offline_times >= 4
                  ) THEN TRUE
      ELSE NULL
    END AS went_offline
  FROM user_session_times AS ust
  JOIN (
    SELECT userid, MIN(date_trunc('day', session_started)) AS first_day
    FROM user_session_times
    WHERE active_day = 1
    GROUP BY userid
  ) temp
  ON temp.userid = ust.userid
  GROUP BY 1,2,3
)

SELECT 
  came_back_day, 
  COUNT(came_back_day) AS retention_count, 
  COUNT(went_offline) AS went_offline
FROM retention_days
GROUP BY came_back_day
ORDER BY came_back_day;

