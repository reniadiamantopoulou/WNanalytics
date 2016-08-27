"""
A set of useful usergroups. Each usergroup is defined as a 
temporary view (not efficient, but flexible for now).
"""

# Usergroups are the groups upon which the queries are ran.
# Generally, it is better for them to be temporary views, rather than tables.
# Structure:
#   {
#       view_1: {         // the key is the name of the view.
#           "query": ..., // the query that creates this view.
#           "description": ... // human-readable description of this group.
#       },
#       view_2: { ...}
#   }
#
# All usergroups queries need a "date_start" (inclusive) and 
# "date_end" (exclusive), when string-formatting the query. 
#
usergroups = {
    "first_time_londoners": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_londoners AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network, 
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) LIKE 'lond%%' AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s'
               """,
        "description": "First time users in London.",
    },
    "first_time_londoners_gotvalue": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_londoners_gotvalue AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) LIKE 'lond%%' AND
                 EXISTS (
                   SELECT DISTINCT g.userid FROM gotvalue AS g
                   WHERE
                     g.userid=l.userid AND 
                     g.created_at >= '%(date_start)s' AND 
                     g.created_at < '%(date_end)s'
                 ) AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s'
               """,
        "description": "First time users in London, that got the value."
    },
    "first_time_londoners_noexp": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_londoners_noexp AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) LIKE 'lond%%' AND 
                 NOT EXISTS (
                   SELECT e.userid, e.session
                   FROM experimental_sessions AS e
                   WHERE e.userid=l.userid AND e.session=l.session
                 ) AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s'
               """,
        "description": "First time users in London, excluding experimental sessions."
    },
    "first_time_londoners_transit": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_londoners_transit AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) LIKE 'lond%%' AND 
                 EXISTS (
                   SELECT DISTINCT logs.userid
                   FROM logs
                   WHERE
                        logs.userid=l.userid AND
                        logs.event='TransitListInfoOpen' AND 
                        logs.created_at >= '%(date_start)s' AND
                        logs.created_at < '%(date_end)s'
                 ) AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s'
               """,
        "description": "First time users in London, who have been to transit at least once."
    },
    "first_time_londoners_variant_0": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_londoners_variant_0 AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) LIKE 'lond%%' AND 
                 EXISTS (
                   SELECT DISTINCT logs.userid
                   FROM logs
                   WHERE
                        logs.properties->>'WNAppVariant'='0' AND
                        logs.userid=l.userid AND
                        logs.created_at >= '%(date_start)s' AND
                        logs.created_at < '%(date_end)s'
                 ) AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s'
               """,
        "description": "First time users in London, who were landed on ListView (variant 0)."
    },
    "first_time_londoners_variant_1": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_londoners_variant_1 AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) LIKE 'lond%%' AND 
                 EXISTS (
                   SELECT DISTINCT logs.userid
                   FROM logs
                   WHERE
                        logs.userid=l.userid AND
                        logs.properties->>'WNAppVariant'='1' AND
                        logs.created_at >= '%(date_start)s' AND
                        logs.created_at < '%(date_end)s'
                 ) AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s'
               """,
        "description": "First time users in London, who were landed in MyDayInLondon."
    },
    "first_time_londoners_variant_1_branch_1": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_londoners_variant_1_branch_1 AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) LIKE 'lond%%' AND 
                 EXISTS (
                   SELECT DISTINCT logs.userid
                   FROM logs
                   WHERE
                        logs.userid=l.userid AND
                        logs.properties->>'WNAppVariant'='1' AND
                        logs.properties->>'WNAppVariantBranchFirstAddJumpsToItin'='1' AND
                        logs.created_at >= '%(date_start)s' AND
                        logs.created_at < '%(date_end)s'
                 ) AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s'
               """,
        "description": "First time users in London, who were landed in MyDayInLondon, pressed + and came back to MyDayInLondon."
    },

    "first_time_londoners_variant_1_branch_0": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_londoners_variant_1_branch_0 AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) LIKE 'lond%%' AND 
                 EXISTS (
                   SELECT DISTINCT logs.userid
                   FROM logs
                   WHERE
                        logs.userid=l.userid AND
                        logs.properties->>'WNAppVariant'='1' AND
                        logs.properties->>'WNAppVariantBranchFirstAddJumpsToItin'='0' AND
                        logs.created_at >= '%(date_start)s' AND
                        logs.created_at < '%(date_end)s'
                 ) AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s'
               """,
        "description": "First time users in London, who were landed in MyDayInLondon, and they just continued browsing... no branch."
    },

    "first_time_londoners_variant_1_branch_undefined": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_londoners_variant_1_branch_undefined AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) LIKE 'lond%%' AND 
                 EXISTS (
                   SELECT DISTINCT logs.userid
                   FROM logs
                   WHERE
                        logs.userid=l.userid AND
                        logs.properties->>'WNAppVariant'='1' AND
                        (logs.properties->'WNAppVariantBranchFirstAddJumpsToItin') IS NULL AND
                        logs.created_at >= '%(date_start)s' AND
                        logs.created_at < '%(date_end)s'
                 ) AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s'
               """,
        "description": "First time users in London, who were landed in MyDayInLondon, and did not enter a branch by completing the pattern LV-> press plus."
    },


    "first_time_row": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_row AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) NOT LIKE 'lond%%' AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s'
               """,
        "description": "First time users in ROW."
    },
    # weird case where NOT IN is much better than NOT EXISTS.
    "first_time_row_nolnd": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_row_nolnd AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) NOT LIKE 'lond%%' AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s' AND
                 l.userid NOT IN (  
                    SELECT userid
                    FROM logs 
                    WHERE
                        LOWER(city) LIKE 'lond%%' AND
                        logs.created_at >= '%(date_start)s'
                )
               """,
        "description": "First time users in ROW, who have *not* been in London."
    },
    # weird case where NOT IN is much better than NOT EXISTS.
    "first_time_row_lnd": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_row_lnd AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) NOT LIKE 'lond%%' AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s' AND
                 l.userid IN (  
                    SELECT userid
                    FROM logs 
                    WHERE
                        LOWER(city) LIKE 'lond%%' AND
                        logs.created_at >= '%(date_start)s'
                )
               """,
        "description": "First time users in ROW, who have been in London."
    },
    "first_time_row_gotvalue": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_row_gotvalue AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) NOT LIKE 'lond%%' AND
                 EXISTS (
                   SELECT g.userid FROM gotvalue AS g
                   WHERE 
                     g.userid=l.userid AND
                     g.created_at >= '%(date_start)s' AND 
                     g.created_at < '%(date_end)s'
                 ) AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s'
               """,
        "description": "First time users in ROW, that got the value."
    },
    "first_time_row_noexp": {
        "query": """
               CREATE OR REPLACE TEMP VIEW first_time_row_noexp AS
               SELECT
                 l.userid, l.event, l.created_at, l.session,
                 l.activation_time, l.network,
                 l.properties->>'WNItineraryChangeButton' AS it_add_from
               FROM first_time_users AS u
               JOIN first_cities AS c ON (u.userid=c.userid)
               JOIN logs as l ON (u.userid=l.userid)
               WHERE 
                 LOWER(c.first_city) NOT LIKE 'lond%%' AND
                 NOT EXISTS (
                   SELECT e.userid, e.session
                   FROM experimental_sessions AS e
                   WHERE e.userid=l.userid AND e.session=l.session
                 ) AND
                 l.created_at >= '%(date_start)s' AND 
                 l.created_at < '%(date_end)s' AND
                 u.first_join >= '%(date_start)s' AND 
                 u.first_join < '%(date_end)s'
               """,
        "description": "First time users in ROW, excluding experimental sessions."
    }
}
