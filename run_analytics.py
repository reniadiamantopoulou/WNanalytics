import csv
import json
from time import strptime, time
from datetime import datetime, timedelta, date

import psycopg2
from psycopg2.extras import RealDictCursor

from analytics_queries import queries
from analytics_usergroups import usergroups

CONVERSION_TIME = [timedelta(days=7), timedelta(days=14), 
                   timedelta(days=21), timedelta(days=30), 
                   timedelta(days=60)]

DEBUG = True
HOSTNAME = "sql-event-analytics.ctovz3c435p3.us-east-1.rds.amazonaws.com"

def db_connect(user="cybereye", password="AstroMarseille_3", dbname="analytics", host=HOSTNAME):
    """ Wrapper for connecting to db.

    Returns:
        A tuple of connection and RealDictCursor cursor object.
    """
    # Schema: userid(varchar), event(varchar), created_at(date),
    #         session(int), network(boolean), activation_time(int),
    #         city(varchar), properties(json)
    try:
        con = psycopg2.connect("host=%s dbname=%s user=%s password=%s" %
                               (host, dbname, user, password))
        cur = con.cursor(cursor_factory=RealDictCursor)
        return con, cur
    except Exception as e:
        print "Error connecting to the database: ", e
        raise


def db_close(connection):
    """ Closes the connection to database. """
    try:
        connection.close()
    except Exception as e:
        print "Error closing the connection: ", e
        raise


def run_query(cur, query, usergroup):
    """ Run a query for a specific usergroup.
    """
    # safe, usergroups come from the application.
    sql = query % {"view": usergroup}
    cur.execute(sql)
    results = cur.fetchall()
    return results


def create_usergroup(cur, query, date_start, date_end):
    """ Creates a temporary view of the usergroup.
    The start and end dates are used to restrict this 
    usergroup to a certain date range. 
    `date_start` and `date_end` should be date() objects 
    or strings of the format Y-m-d.
    """
    q = query["query"] % {"date_start": date_start, 
                          "date_end": date_end}
    cur.execute(q)


def day_analytics_csv(cur, date_start):
    """ Run analytics for a specific day.
    ``date_start`` should be a date object.
    
    Outputs the analytics into a CSV file for a specific day.
    """
    output_filename = "report_%s.csv" % date_start

    if DEBUG:
        restrict_groups = set()  
        restrict_time = set([timedelta(days=60)])
        print "[DEBUG MODE ON]"
        print "restrict_groups: ", restrict_groups
        print "restrict_time: ", restrict_time

    with open(output_filename, "wb") as csvfile:
        report = csv.writer(csvfile, delimiter=',')

        # For each date range.
        for delta in CONVERSION_TIME:
            if DEBUG:
                if delta not in restrict_time and restrict_time:
                    continue

            date_end = date_start + delta
            print "Dates: ", date_start, date_end - timedelta(days=1)

            report.writerow(["Dates: %s - %s (%s days)" % 
                            (date_start, date_end - timedelta(days=1), delta.days)])

            # Create the user-groups.
            for uid, uq in usergroups.iteritems():
                create_usergroup(cur, uq, date_start, date_end)

            for q in queries:
                if not q.get("query", "").strip():
                    continue
                query_groups = q.get("usergroups", [])
                for g in query_groups:
                    if DEBUG:
                        if g not in restrict_groups and restrict_groups:
                            continue

                    print "Running query: %s (%s)" % (q["title"], g),
                    t1 = time()
                    results = run_query(cur, q["query"], g)
                    description = usergroups[g].get("description", "")
                    report.writerow(["Group: " + description])
                    report.writerow(["Query: " + q.get("title", "")])
                    report.writerow([""]) # separate group/query from results
                    fields = q.get("fields", [])
                    if fields:
                        report.writerow(fields)
                    for r in results:
                        if fields:
                            report.writerow([r[f] for f in fields])
                        else:
                            report.writerow(r.values())
                    report.writerow([""]) # separate the groups with empty line
                    report.writerow([""]) # separate the groups with empty line
                    print "... %f seconds" % (time() - t1)
                report.writerow([""]) # separate the queries with empty line

def day_analytics_json(cur, date_start):
    """ Run analytics for a specific day.
    ``date_start`` should be a date object.

    Summarizes and returns the analytics into a dictionary.
    The structure is
    {"<usergroup>": 
        {"<date_end>":
            {"<category_id>": 
                [
                    {query_info along with results}, # ideally query id
                ]
            }
        }
    }
    """
    statistics = {}
    if DEBUG:
        restrict_groups = set(["first_time_row", "first_time_londoners", "first_time_row_lnd"])  
        restrict_time = set([timedelta(days=30)])
        print "[DEBUG MODE ON]"
        print "restrict_groups: ", restrict_groups
        print "restrict_time: ", restrict_time

    # For each date range.
    for delta in CONVERSION_TIME:
        if DEBUG:
            if delta not in restrict_time and restrict_time:
                continue

        date_end = date_start + delta
        date_end_str = date_end.isoformat()
        print "Dates: %s - %s (%s days)" % \
                 (date_start, date_end - timedelta(days=1), delta.days)

        # Create the user-groups.
        for gid, gquery in usergroups.iteritems():
            create_usergroup(cur, gquery, date_start, date_end)
            statistics[gid] = {date_end_str: {}}

        for q in queries:
            if not q.get("query", "").strip():
                continue
            query_groups = q.get("usergroups", [])
            for g in query_groups:
                if DEBUG:
                    if g not in restrict_groups and restrict_groups:
                        continue
                print "Running query: %s (%s)" % (q.get("title", "NO_TITLE!"), g),
                t1 = time()
                results = run_query(cur, q["query"], g)
                cat_id = q.get("category_id", "UNKNOWN")
                query_info = {
                    "title": q.get("title", ""),
                    "fields": q.get("fields", []),
                    "result": results
                }

                try:
                    statistics[g][date_end_str][cat_id].append(query_info)
                except KeyError:
                    statistics[g][date_end_str][cat_id] = [query_info]

                print "... %f seconds" % (time() - t1)

    return statistics


if __name__ == "__main__":
    import sys
    if len(sys.argv) == 2:
        format = sys.argv[1]
        valid_formats = ["csv", "json"]
        if format not in valid_formats:
            print "Invalid output file format `%s`. Should be one of (%s)" \
                    % (format, ", ".join(valid_formats))
            sys.exit(-1)
    else:
        format = "json"

    # Connect to postgres database.
    con, cur = db_connect()
    dates = [date(2015, 11, 1)]

    if format == "csv":
        for d in dates:
            day_analytics_csv(cur, d)
    else:
        results = {}
        for d in dates:
            statistics = day_analytics_json(cur, d)
            results[d.isoformat()] = statistics

        # Make datetime and date objects json serializable.
        date_handler = lambda obj: (
            obj.isoformat() 
            if hasattr(obj, 'isoformat') 
            else obj)

        print "Saving results to json..."
        output_filename = "report.json"  # all dates together
        with open(output_filename, "w") as fp:
            json.dump(results, fp, default=date_handler)
        print "Done."
    
    # Close connection.
    db_close(con)
