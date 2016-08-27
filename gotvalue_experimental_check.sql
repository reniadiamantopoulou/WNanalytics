-- first time londoners that got the value (g)
SELECT DISTINCT g.userid FROM gotvalue AS g
JOIN
(SELECT DISTINCT userid from first_time_londoners) f
ON g.userid=f.userid;

-- first time londoners that had at least 1 experimental session.
SELECT DISTINCT e.userid FROM experimental_sessions AS e
JOIN
(SELECT DISTINCT userid from first_time_londoners) f
ON e.userid=f.userid;

-- e + g
SELECT userid, COUNT(session)
FROM (
  SELECT e.userid, e.session
  FROM experimental_sessions AS e
  JOIN
  gotvalue AS g
  ON (e.userid = g.userid)
  JOIN (
    SELECT DISTINCT userid FROM first_time_londoners 
  ) f
  ON (e.userid = f.userid)
) t
GROUP BY userid;

-- first time londoners that gotvalue but did not experiment.
SELECT DISTINCT g.userid
FROM gotvalue AS g
JOIN (
  SELECT DISTINCT userid FROM first_time_londoners
) f
ON (g.userid = f.userid)
WHERE g.userid NOT IN (
  SELECT DISTINCT userid FROM experimental_sessions
);


first_time_londoners: 1551
experimental: 535
gotvalue: 221
experimental + gotvalue: 133


