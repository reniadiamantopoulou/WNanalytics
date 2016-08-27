DROP SEQUENCE IF EXISTS serial;
CREATE SEQUENCE serial START 1; 
WITH network_groups AS (
  SELECT userid, date_trunc('day', MIN(created_at)) AS offline_moment, COUNT(userid) AS offline_times
  FROM (
    SELECT 
      userid, event, created_at, session, activation_time, network,
      CASE
        WHEN network IS FALSE THEN
          CASE 
            WHEN lag(network) OVER (PARTITION BY userid ORDER BY created_at) IS FALSE THEN currval('serial')
            ELSE nextval('serial')
          END
        ELSE NULL
      END AS network_group
    FROM first_time_group
    WHERE 
      event NOT IN ('AppStartup', 'MapModeEnd', 'ItineraryAdd2', 
                    'ItineraryRemove2', 'RadarActivated', 'RadarDeactivated') 
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
