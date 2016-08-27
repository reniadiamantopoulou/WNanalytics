-----------------------------------------------
-- Description:
--
-- Contains setup of useful tables, views 
-- and user-defined aggregations for extracting
-- the fundamental analytics.
------------------------------------------------


------------------------------------------------------------------
-- 1. Create view of first time users within the time interval.
------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW first_time_users AS
SELECT DISTINCT userid
FROM logs
WHERE event <> 'MapModeEnd'
GROUP BY userid
HAVING MIN(session) <= 1;

------------------------------------------------------------------
-- 2. Find first city (i.e. download) for each user.
------------------------------------------------------------------
-- FIRST(): first value that is not NULL
CREATE OR REPLACE FUNCTION public.first_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE sql IMMUTABLE STRICT AS $$
        SELECT $1;
$$;
 
-- And then wrap an aggregate around it
CREATE AGGREGATE public.first (
        sfunc    = public.first_agg,
        basetype = anyelement,
        stype    = anyelement
);

CREATE MATERIALIZED VIEW first_cities AS
SELECT DISTINCT 
  userid, 
  FIRST(city ORDER BY created_at) AS first_city
FROM logs 
WHERE city NOT IN ('UNKNOWN', 'NOLOCATION')
GROUP BY userid;

------------------------------------------------------------------
-- 3. Create table of first time londoners.
------------------------------------------------------------------
CREATE TABLE first_time_londoners AS
SELECT 
  l.userid, l.event, l.created_at, l.session, l.activation_time, 
  CASE l.properties->>'WNNetworkAvailable' 
    WHEN 'true' THEN TRUE
    WHEN 'false' THEN FALSE
    ELSE TRUE -- DEFAULT TO `ONLINE`
  END AS network
FROM first_time_users AS u
JOIN first_cities AS c ON (u.userid=c.userid)
JOIN logs as l ON (u.userid=l.userid)
WHERE LOWER(c.first_city) LIKE 'lond%'
ORDER BY 1, 3, 5;

------------------------------------------------------------------
-- 4. Create table of first time ROW. (same as above)
------------------------------------------------------------------
CREATE TABLE first_time_row AS
SELECT 
  l.userid, l.event, l.created_at, l.session, l.activation_time, 
  CASE l.properties->>'WNNetworkAvailable' 
    WHEN 'true' THEN TRUE
    WHEN 'false' THEN FALSE
    ELSE TRUE -- DEFAULT TO `ONLINE`
  END AS network
FROM first_time_users AS u
JOIN first_cities AS c ON (u.userid=c.userid)
JOIN logs as l ON (u.userid=l.userid)
WHERE LOWER(c.first_city) NOT LIKE 'lond%'
ORDER BY 1, 3, 5;
