-- Conversion drop-offs
-- --------------------

-- There have been created 2 views already:
--	1. temp view "first_time_users", which are users with min(session)=1
--	2. materialized view "first_cities", which gives the first recognisable city for each user


-- CREATE A TEMPORARY VIEW OF THE USER TABLE without experimental sessions.
-- DROP TABLE IF EXISTS first_time_group CASCADE;
-- CREATE TABLE first_time_group AS
-- SELECT * 
-- FROM 
--  first_time_londoners AS f
-- first_time_row AS f
-- WHERE NOT EXISTS(
--  SELECT userid, session 
--  FROM experimental_sessions
--  WHERE userid=f.userid AND session=f.session
--);

-- Universe of users
SELECT 'Users' AS description, COUNT(DISTINCT userid)
FROM first_time_group;

-- 1.2. ACQUISITION
-- Press at least one button
SELECT 'acquisition' AS description, COUNT(userid) FROM (
  SELECT userid
  FROM first_time_group
  WHERE event NOT IN ('AppStartup', 'MapModeEnd')
  GROUP BY userid
  HAVING COUNT(event) >= 1
) t;


-- 2.1. ACTIVATION
-- Press plus button or Details View at least 1.
SELECT '2.1 activation' AS description, COUNT(userid) FROM (
  SELECT userid
  FROM first_time_group
  WHERE event IN ('ItineraryAdd', 'DetailsChecked')
  GROUP BY userid
  HAVING COUNT(event) >= 1
) t;

-- 2.2. ACTIVATION
-- Press plus button or Details View at least 2.
SELECT '2.2 activation' AS description, COUNT(userid) FROM (
  SELECT userid
  FROM first_time_group
  WHERE event IN ('ItineraryAdd', 'DetailsChecked')
  GROUP BY userid
  HAVING COUNT(event) >= 2
) t;

-- 2.3. ACTIVATION (only for sanity check for 2.4)
-- Press plus button at least 2 and go to Itinerary. (unordered)
SELECT '2.3 activation' AS description, COUNT(userid) FROM (
  SELECT userid FROM (
    SELECT userid, 
      COUNT(CASE event WHEN 'ItineraryAdd' THEN 1 ELSE NULL END) AS it_plus, 
      COUNT(CASE event WHEN 'ItineraryModeStart' THEN 1 ELSE NULL END) AS it_view
    FROM first_time_group
    GROUP BY userid
  ) AS temp
  WHERE it_plus >= 2 AND it_view >= 1
) t;

-- 2.4. ACTIVATION (also does 2.3)
-- Press plus button at least 2, go to Itinerary and *then* (order) go to Transit View.
SELECT '2.3/2.4 activation' AS description, * FROM (

SELECT 
  SUM(it_plus_plus_view_complete) AS it_plus_plus_view_completed,
  SUM(transit_view_complete) AS transit_view_completed
FROM (
  SELECT 
    t1.userid, 
    GREATEST(min_it_view_time, min_it_two_plus_time) AS it_plus_plus_view_time,
    1 it_plus_plus_view_complete
  FROM (
    SELECT userid, MIN(activation_time) AS min_it_view_time
    FROM first_time_group
    WHERE event='ItineraryModeStart'
    GROUP BY userid
  ) t1
  JOIN (
    SELECT DISTINCT userid, min_it_two_plus_time
    FROM (
      SELECT 
        userid, 
        nth_value(activation_time, 2) 
          OVER (PARTITION BY userid ORDER BY activation_time) AS min_it_two_plus_time
      FROM first_time_group
      WHERE event='ItineraryAdd'
    ) temp
    WHERE min_it_two_plus_time IS NOT NULL
  ) t2
  ON t1.userid = t2.userid
) f1 LEFT JOIN LATERAL (
    SELECT 
      userid, 
      1 AS transit_view_complete
    FROM first_time_group
    WHERE 
      event='TransitModeStart' AND
      userid=f1.userid AND
      activation_time > it_plus_plus_view_time
    LIMIT 1
) f2 ON TRUE

) t;

 
-- 2.5. ACTIVATION
-- User had Map Interactions offline and came back online.
SELECT '2.5 activation' AS description, * FROM (

SELECT 
  SUM(view_map_offline) AS viewed_map_offline,
  SUM(back_online) AS came_back_online
FROM (
  SELECT 
    userid, 
    1 AS view_map_offline,
    MIN(activation_time) AS view_map_offline_time
  FROM first_time_group
  WHERE 
    event='MapIteraction' AND 
    network=FALSE
  GROUP BY userid
) r1 LEFT JOIN LATERAL (
  SELECT
    userid,
    1 AS back_online
  FROM first_time_group
  WHERE
    userid = r1.userid AND
    network = TRUE AND 
    activation_time > view_map_offline_time
  LIMIT 1
) r2 ON TRUE
    
) t;




