SELECT boxplot(detailsmap::float / detailschecks::float) AS ratio
--SELECT userid, (detailsmap::float / detailschecks::float)
FROM (

SELECT userid, 
SUM(CASE WHEN event='DetailsMapOpen' THEN 1 ELSE 0 END) AS detailsmap,
SUM(CASE WHEN event='DetailsChecked' THEN 1 ELSE 0 END) AS detailschecks
FROM logs
WHERE 
    event IN ('DetailsChecked', 'DetailsMapOpen') AND
    EXISTS (
        SELECT userid FROM first_cities 
        WHERE userid=logs.userid AND lower(first_city) LIKE 'lond%'
    ) AND 
    NOT EXISTS (
        SELECT userid FROM experimental_sessions
        WHERE userid=logs.userid AND session = logs.session
    )
    
GROUP BY userid
) t
WHERE detailschecks > 0; -- redundant

-- Stats
-- 2696 londoners
-- 1801 / 2696 = 66.8%  opened details
-- 879  / 2696 = 32.6%  opened details-map
-- 879  / 1801 = 48.8%  of those that open details-> open details-map
-- boxplot: 
--  2%: 0,
-- 25%: 0,
-- 50%: 0,
-- 75%: 0.333
-- 98%: 1.333


-- Removing experimental sessions
-- 2520 londoners
-- 1586 / 2520 = 62.93% opened details
--  772 / 2520 = 30.63% opened details-map
--  772 / 1586 = 48.68% of those that open details-> open details-map

-- boxplot:
--  2%: 0,
-- 25%: 0,
-- 50%: 0,
-- 75%: 0.4,
-- 98%: 1.33


WITH mapinfo AS (
SELECT userid, mapopened - transitmap as mapview, transitmap, detailsmap
FROM (   
    SELECT userid, 
    SUM(CASE WHEN event='MapModeStart' THEN 1 ELSE 0 END) as mapopened,
    SUM(CASE WHEN event='TransitModeFromItinerary' THEN 1 ELSE 0 END) as transitmap,
    SUM(CASE WHEN event='DetailsMapOpen' THEN 1 ELSE 0 END) as detailsmap
    FROM logs
    group by userid
) t
WHERE mapopened >= transitmap
), mapviewers AS (
SELECT DISTINCT userid
FROM mapinfo
WHERE mapview > 0
AND EXISTS (SELECT userid FROM first_cities WHERE userid=mapinfo.userid AND lower(first_city) LIKE '%lond%')
) 

SELECT COUNT(*) FROM mapviewers;

SELECT DISTINCT userid
FROM logs
WHERE 
    event='POIBarSwipe' AND
    EXISTS (SELECT userid FROM mapinfo WHERE userid=logs.userid AND mapview > 0) AND 
    EXISTS (SELECT userid FROM first_cities WHERE userid=logs.userid AND lower(first_city) LIKE '%lond%');
    
-- RESULTS
-- poibarswipe / mapview = 17% (global)
-- 165 / 930 = 17.7% (london)


-- 14820 users
-- (mapview,  transitmap, detailsmap)
-- FFF: 7668 (52%)
-- TFF: 2092 (14%)
-- FTF: 480  (3%)
-- FFT: 1427 (10%)
-- TTF: 648  (4%)
-- TFT: 1137 (8%)
-- FTT: 348  (2%)
-- TTT: 1020 (7%)


