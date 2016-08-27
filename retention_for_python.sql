explain analyze 
  SELECT userid, date_trunc('day', MIN(created_at)) AS offline_moment, COUNT(userid) AS offline_times
  FROM (
    SELECT 
      userid, event, created_at, session, activation_time, network,
      CASE
        WHEN network IS FALSE THEN 1
        ELSE NULL
      END AS network_group
    FROM logs
    WHERE 
      event NOT IN ('AppStartup', 'MapModeEnd', 'ItineraryAdd2', 
                    'ItineraryRemove2', 'RadarActivated', 'RadarDeactivated',
                    'OnBoardingTutorialDone') 
  ) t
  WHERE network_group IS NOT NULL
  GROUP BY userid, network_group

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
    FROM logs
    WHERE 
      event NOT IN ('AppStartup', 'MapModeEnd', 'ItineraryAdd2', 
                    'ItineraryRemove2', 'RadarActivated', 'RadarDeactivated',
                    'OnBoardingTutorialDone') 
  ) t
  WHERE network_group IS NOT NULL
  GROUP BY userid, network_group
), user_session_times AS (
  SELECT userid, session_started, session_duration, 
     dense_rank() OVER (PARTITION BY userid 
            ORDER BY date_trunc('day', session_started)
     ) AS active_day
  FROM (
    SELECT 
      userid, session, MIN(created_at) AS session_started, 
      MAX(created_at) - MIN(created_at) AS session_duration
    FROM logs WHERE event != 'MapModeEnd'
    GROUP BY userid, session
  ) t
  WHERE session_duration < interval '0.5 hour'
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

