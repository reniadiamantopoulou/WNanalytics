-- 1. +1 => negative events(transit, it_view, mapview, details)
-- 2. +2 => ...
-- 3. +3 => ...
WITH interesting_events AS ( 
  SELECT userid, event
  FROM first_time_londoners
  WHERE event IN ('ItineraryAdd', 'ItineraryModeStart', 'TransitModeStart',
              'MapIteraction', 'DetailsChecked')
), user_pluses AS (
  SELECT userid, COUNT(event) as it_plus
  FROM interesting_events
  WHERE event='ItineraryAdd'
  GROUP BY userid
)

SELECT 'it_plus >= 1' AS description, COUNT(userid) FROM user_pluses WHERE it_plus >= 1
UNION ALL
  SELECT 
    'it_plus >= 1, no Itinerary View' AS description, 
    COUNT(userid) 
  FROM user_pluses 
  WHERE 
    it_plus >= 1 AND 
    userid NOT IN (SELECT userid FROM interesting_events WHERE event='ItineraryModeStart')
UNION ALL
  SELECT 
    'it_plus >= 1, no Transit mode' AS description, 
    COUNT(userid) 
  FROM user_pluses 
  WHERE 
    it_plus >= 1 AND 
    userid NOT IN (SELECT userid FROM interesting_events WHERE event='TransitModeStart')
UNION ALL
  SELECT 
    'it_plus >= 1, no Map interaction' AS description, 
    COUNT(userid) 
  FROM user_pluses 
  WHERE 
    it_plus >= 1 AND 
    userid NOT IN (SELECT userid FROM interesting_events WHERE event='MapIteraction')
UNION ALL
  SELECT 
    'it_plus >= 1, no DetailsChecked' AS description, 
    COUNT(userid) 
  FROM user_pluses 
  WHERE 
    it_plus >= 1 AND 
    userid NOT IN (SELECT userid FROM interesting_events WHERE event='DetailsChecked')


UNION ALL
SELECT 'it_plus >= 2' AS description, COUNT(userid) FROM user_pluses WHERE it_plus >= 2
UNION ALL
  SELECT 
    'it_plus >= 2, no Itinerary View' AS description, 
    COUNT(userid) 
  FROM user_pluses 
  WHERE 
    it_plus >= 2 AND 
    userid NOT IN (SELECT userid FROM interesting_events WHERE event='ItineraryModeStart')
UNION ALL
  SELECT 
    'it_plus >= 2, no Transit mode' AS description, 
    COUNT(userid) 
  FROM user_pluses 
  WHERE 
    it_plus >= 2 AND 
    userid NOT IN (SELECT userid FROM interesting_events WHERE event='TransitModeStart')
UNION ALL
  SELECT 
    'it_plus >= 2, no Map interaction' AS description, 
    COUNT(userid) 
  FROM user_pluses 
  WHERE 
    it_plus >= 2 AND 
    userid NOT IN (SELECT userid FROM interesting_events WHERE event='MapIteraction')
UNION ALL
  SELECT 
    'it_plus >= 2, no DetailsChecked' AS description, 
    COUNT(userid) 
  FROM user_pluses 
  WHERE 
    it_plus >= 2 AND 
    userid NOT IN (SELECT userid FROM interesting_events WHERE event='DetailsChecked')


UNION ALL
SELECT 'it_plus >= 3' AS description, COUNT(userid) FROM user_pluses WHERE it_plus >= 3
UNION ALL
  SELECT 
    'it_plus >= 3, no Itinerary View' AS description, 
    COUNT(userid) 
  FROM user_pluses 
  WHERE 
    it_plus >= 3 AND 
    userid NOT IN (SELECT userid FROM interesting_events WHERE event='ItineraryModeStart')
UNION ALL
  SELECT 
    'it_plus >= 3, no Transit mode' AS description, 
    COUNT(userid) 
  FROM user_pluses 
  WHERE 
    it_plus >= 3 AND 
    userid NOT IN (SELECT userid FROM interesting_events WHERE event='TransitModeStart')
UNION ALL
  SELECT 
    'it_plus >= 3, no Map interaction' AS description, 
    COUNT(userid) 
  FROM user_pluses 
  WHERE 
    it_plus >= 3 AND 
    userid NOT IN (SELECT userid FROM interesting_events WHERE event='MapIteraction')
UNION ALL
  SELECT 
    'it_plus >= 3, no DetailsChecked' AS description, 
    COUNT(userid) 
  FROM user_pluses 
  WHERE 
    it_plus >= 3 AND 
    userid NOT IN (SELECT userid FROM interesting_events WHERE event='DetailsChecked')
;



---------------
select addfrom, ROUND(100.0 * counts / sum(counts) over (), 2) AS percentage from (
select 
  properties->>'WNItineraryChangeButton' as addfrom, 
  count(properties->>'WNItineraryChangeButton') AS counts
from logs
where event='ItineraryAdd2' 
--and (userid, session) not in (select * from experimental_sessions) 
group by 1) temp
order by addfrom;


