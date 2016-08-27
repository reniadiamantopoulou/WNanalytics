"""
A set of analytics queries for Postgres.
Each query can have:
    - the actual query (required)
    - title (required)
    - description (required)
    - category_id (required) [a unique string identifier of the analytics category e.g. "activation"]
    - list of usergroups for which it will be applied
    - ordered list of returned fields' names
    - labels
    - the type of chart

Each query runs for a usergroup defined in analytics_usergroups.py.
If no the ``usergroups`` field is empty, then the query will 
run for all usergroups. 
"""

# Usergroups:
# ['first_time_row', 
#  'first_time_londoners_noexp', 
#  'first_time_londoners', 
#  'first_time_row_noexp', 
#  'first_time_row_nolnd', 
#  'first_time_londoners_gotvalue', 
#  'first_time_row_gotvalue']


queries = [
    {   "category_id": "universe",
        "title": "#Unique users in user group (universe)",
        "description": "",
        "fields": ["total_count"],
        "usergroups": ["first_time_londoners", "first_time_row", "first_time_row_nolnd", "first_time_londoners_transit",
                       "first_time_londoners_variant_1_branch_0", "first_time_londoners_variant_1_branch_1", 
                       "first_time_londoners_variant_1", "first_time_londoners_variant_0", "first_time_row_lnd"],
        "query": """
                 SELECT COUNT(userid) AS total_count
                 FROM (
                   SELECT DISTINCT userid
                   FROM %(view)s
                 ) t
                 """,
    },
    {   "category_id": "universe",
        "title": "Min/Max date range from data (universe)",
        "description": "",
        "fields": ["min_date", "max_date"],
        "usergroups": ["first_time_londoners", "first_time_row", "first_time_row_nolnd", "first_time_londoners_transit",
                       "first_time_londoners_variant_1_branch_0", "first_time_londoners_variant_1_branch_1", 
                       "first_time_londoners_variant_1", "first_time_londoners_variant_0", "first_time_row_lnd"],
        "query": """
                 SELECT MIN(created_at) AS min_date, MAX(created_at) AS max_date
                 FROM %(view)s
                 """,
    },
    {   "category_id": "acquisition",
        "title": "Acquisition",
        "description": "",
        "usergroups": ["first_time_londoners", "first_time_row", "first_time_row_nolnd", "first_time_londoners_transit",
                       "first_time_londoners_variant_1_branch_0", "first_time_londoners_variant_1_branch_1", 
                       "first_time_londoners_variant_1", "first_time_londoners_variant_0", "first_time_row_lnd"],
        "query": """
                 SELECT COUNT(userid) FROM (
                   SELECT userid
                   FROM %(view)s
                   WHERE event NOT IN ('AppStartup', 'MapModeEnd')
                   GROUP BY userid
                   HAVING COUNT(event) >= 1
                 ) t
                 """,
    },
    {   "category_id": "activation",
        "title": "2.1 activation",
        "description": "",
        "usergroups": ["first_time_londoners", "first_time_row", "first_time_row_nolnd", "first_time_londoners_transit",
                       "first_time_londoners_variant_1_branch_0", "first_time_londoners_variant_1_branch_1", 
                       "first_time_londoners_variant_1", "first_time_londoners_variant_0", "first_time_row_lnd"],
        "query": """
                 SELECT COUNT(userid) FROM (
                   SELECT userid
                   FROM %(view)s
                   WHERE event IN ('ItineraryAdd', 'DetailsChecked')
                   GROUP BY userid
                   HAVING COUNT(event) >= 1
                 ) t
                 """,
    },
    {   "category_id": "activation",
        "title": "2.2 activation",
        "description": "",
        "usergroups": ["first_time_londoners", "first_time_row", "first_time_row_nolnd", "first_time_londoners_transit",
                       "first_time_londoners_variant_1_branch_0", "first_time_londoners_variant_1_branch_1", 
                       "first_time_londoners_variant_1", "first_time_londoners_variant_0", "first_time_row_lnd"],
        "query": """
                 SELECT COUNT(userid) FROM (
                   SELECT userid
                   FROM %(view)s
                   WHERE event IN ('ItineraryAdd', 'DetailsChecked')
                   GROUP BY userid
                   HAVING COUNT(event) >= 2
                 ) t
                 """,
    },
    {   "category_id": "activation",
        "title": "2.3/2.4 activation",
        "description": "",
        "fields": ["it_plus_plus_view_completed", "transit_view_completed"],
        "usergroups": ["first_time_londoners", "first_time_row", "first_time_row_nolnd", "first_time_londoners_transit",
                       "first_time_londoners_variant_1_branch_0", "first_time_londoners_variant_1_branch_1", 
                       "first_time_londoners_variant_1", "first_time_londoners_variant_0", "first_time_row_lnd"],
        "query": """
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
                     FROM %(view)s
                     WHERE event='ButtonPressToItinerary'
                     GROUP BY userid
                   ) t1
                   JOIN (
                     SELECT DISTINCT userid, min_it_two_plus_time
                     FROM (
                       SELECT 
                         userid, 
                         nth_value(activation_time, 2) 
                         OVER (PARTITION BY userid ORDER BY activation_time) AS min_it_two_plus_time
                       FROM %(view)s
                       WHERE event='ItineraryAdd'
                     ) temp
                     WHERE min_it_two_plus_time IS NOT NULL
                   ) t2
                   ON t1.userid = t2.userid
                 ) f1 LEFT JOIN LATERAL (
                     SELECT 
                       userid,
                       1 AS transit_view_complete
                     FROM %(view)s
                     WHERE 
                       event='TransitListInfoOpen' AND
                       userid=f1.userid AND
                       activation_time > it_plus_plus_view_time
                     LIMIT 1
                 ) f2 ON TRUE
                 """,
    },
    {   "category_id": "activation",
        "title": "2.5 activation",
        "description": "",
        "fields": ["viewed_map_offline", "came_back_online"],
        "usergroups": ["first_time_londoners", "first_time_row", "first_time_row_nolnd", "first_time_londoners_transit",
                       "first_time_londoners_variant_1_branch_0", "first_time_londoners_variant_1_branch_1", 
                       "first_time_londoners_variant_1", "first_time_londoners_variant_0", "first_time_row_lnd"],
        "query": """
                 SELECT 
                   SUM(view_map_offline) AS viewed_map_offline,
                   SUM(back_online) AS came_back_online
                 FROM (
                   SELECT 
                     userid, 
                     1 AS view_map_offline,
                     MIN(activation_time) AS view_map_offline_time
                   FROM %(view)s
                   WHERE 
                     event='MapIteraction' AND 
                     network=FALSE
                   GROUP BY userid
                 ) r1 LEFT JOIN LATERAL (
                   SELECT
                     userid,
                     1 AS back_online
                   FROM %(view)s
                   WHERE
                     userid = r1.userid AND
                     network = TRUE AND 
                     activation_time > view_map_offline_time
                   LIMIT 1
                 ) r2 ON TRUE
                 """,
    },
    {   "category_id": "retention",
        "title": "3.1/3.3 Retention",
        "description": "",
        "fields": ["active_day", "sessions_per_day_boxplot", 
                   "avg_usage_per_day_boxplot", "total_usage_per_day_boxplot", 
                   "unique_users"],
        "usergroups": ["first_time_londoners", "first_time_row", "first_time_row_nolnd",
                       "first_time_londoners_gotvalue", "first_time_row_gotvalue", "first_time_londoners_transit",
                       "first_time_londoners_variant_1_branch_0", "first_time_londoners_variant_1_branch_1", 
                       "first_time_londoners_variant_1", "first_time_londoners_variant_0", "first_time_row_lnd"],
        "query": """
                WITH user_session_times AS (
                  SELECT userid, session_started, session_duration, 
                         dense_rank() OVER (PARTITION BY userid 
                                ORDER BY date_trunc('day', session_started)
                         ) AS active_day
                  FROM (
                    SELECT 
                      userid, session, MIN(created_at) AS session_started, 
                      MAX(created_at) - MIN(created_at) AS session_duration
                    FROM %(view)s WHERE event != 'MapModeEnd'
                    GROUP BY userid, session
                    ) temp
                  WHERE session_duration < interval '0.5 hour'
                ), temp_stats AS (
                  SELECT 
                    userid, 
                    active_day, 
                    SUM(session_duration) AS total_usage_per_day,
                    AVG(session_duration) AS avg_usage_per_day,
                    COUNT(active_day) AS sessions_per_day
                  FROM user_session_times
                  GROUP BY userid, active_day
                )

                SELECT 
                  active_day, 
                  percentile_disc(array[0.02,0.25,0.5,0.75,0.98]) 
                    WITHIN GROUP (ORDER BY sessions_per_day) AS sessions_per_day_boxplot,
                  percentile_disc(array[0.02,0.25,0.5,0.75,0.98]) 
                    WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM avg_usage_per_day)) AS avg_usage_per_day_boxplot,
                  percentile_disc(array[0.02,0.25,0.5,0.75,0.98]) 
                    WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM total_usage_per_day)) AS total_usage_per_day_boxplot,
                  COUNT(userid) AS unique_users
                FROM temp_stats
                GROUP BY active_day
                ORDER BY active_day
                """
    },
    {   "category_id": "retention",
        "title": "3.2 Retention",
        "description": "",
        "fields": ["came_back_day", "retention_count", "went_offline"],
        "usergroups": ["first_time_londoners", "first_time_row", "first_time_row_nolnd",
                       "first_time_londoners_gotvalue", "first_time_row_gotvalue", "first_time_londoners_transit",
                       "first_time_londoners_variant_1_branch_0", "first_time_londoners_variant_1_branch_1", 
                       "first_time_londoners_variant_1", "first_time_londoners_variant_0", "first_time_row_lnd"],
        "query": """
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
                      FROM %(view)s
                      WHERE 
                        event NOT IN ('AppStartup', 'MapModeEnd', 'ItineraryAdd2', 
                                      'ItineraryRemove2', 'RadarActivated', 'RadarDeactivated',
                                      'OnboardingTutorialDone') 
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
                      FROM %(view)s WHERE event != 'MapModeEnd'
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
                  ORDER BY came_back_day                 
                 """
    },
    {   "category_id": "general",
        "title": "Distribution of pluses (itinerary add).",
        "description": "",
        "fields": ["it_add_from", "num_of_pluses"],
        "usergroups": ["first_time_londoners", "first_time_londoners_noexp",
                       "first_time_londoners_gotvalue", "first_time_row",
                       "first_time_row_noexp", "first_time_row_gotvalue",
                       "first_time_row_nolnd", "first_time_londoners_transit",
                       "first_time_londoners_variant_1_branch_0", "first_time_londoners_variant_1_branch_1", 
                       "first_time_londoners_variant_1", "first_time_londoners_variant_0", "first_time_row_lnd"],
        "query": """
                 SELECT it_add_from, count(*) AS num_of_pluses
                 FROM %(view)s 
                 WHERE event='ItineraryAdd2' 
                 GROUP BY 1
                 ORDER BY 2
                 """
    },
    {   "category_id": "general",
        "title": "Distribution of events.",
        "description": "",
        "fields": ["event", "median", "average"],
        "usergroups": ["first_time_londoners", "first_time_londoners_noexp",
                       "first_time_londoners_gotvalue", "first_time_row",
                       "first_time_row_noexp", "first_time_row_gotvalue",
                       "first_time_row_nolnd", "first_time_londoners_transit",
                       "first_time_londoners_variant_1_branch_0", "first_time_londoners_variant_1_branch_1", 
                       "first_time_londoners_variant_1", "first_time_londoners_variant_0", "first_time_row_lnd"],
        "query": """
                 SELECT event, percentile_disc(0.5) WITHIN GROUP (ORDER BY counts) AS median,
                    ROUND(avg(counts),1)::float AS average
                 FROM (
                   SELECT event, count(event) AS counts 
                   FROM %(view)s GROUP BY userid, event
                 ) t
                 GROUP BY event ORDER BY 2 DESC;
                 """
    }    

]
