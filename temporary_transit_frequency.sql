-- I've calculated this for 3 user groups:

-- 1. All first time London users:
-- 1551 -> 611 (39.4%) -> 388 (63.5%)
-- 1551 -> 611 (39.4%) -> 357 (58.4%) :: {1,1,2,4,21}
-- 1551 -> 611 (39.4%) -> 205 (33.6%) :: {1,1,1,2,9}

-- 2. First time London users, after removing experimental sessions:
-- 1408 -> 274 (19.5%) -> 157 (57.3%)
-- 1408 -> 274 (19.5%) -> 147 (53.6%) :: {1,1,2,5,15}

-- 3. First time London users, that got the value:
-- 221 -> 164 (74.2%) -> 124 (75.6%)
-- 221 -> 164 (74.2%) -> 116 (70.7%)  :: {1,2,4,9,31}
-- 221 -> 164 (74.2%) -> 93  (56.7%)  :: {1,1,2,3,15}



SELECT 
  SUM(it_plus_plus_view_complete) AS it_plus_plus_view_completed,
  SUM(transit_view_complete) AS transit_view_completed,
  percentile_disc(array[0.02,0.25,0.5,0.75,0.98]) WITHIN GROUP (ORDER BY transit_views) AS boxplot
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
      count(event) AS transit_views,
      1 AS transit_view_complete
    FROM first_time_group
    WHERE 
      event='TransitModeFromItinerary' AND
      userid=f1.userid AND
      activation_time > it_plus_plus_view_time
    group by userid
) f2 ON TRUE;
