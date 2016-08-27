import csv
import json
from time import strptime, time
from datetime import datetime, timedelta, date

import psycopg2
from psycopg2.extras import RealDictCursor

from analytics_usergroups import usergroups

HOSTNAME = "sql-event-analytics.ctovz3c435p3.us-east-1.rds.amazonaws.com"

# CSV output fields.
fieldnames = ["event", "string_value", "session", 
              "activation_time", "network", "created_at", 
              "count_value", "userid"]


def sample_users(cur, sample_from, sample_size):
    sampling_query = """SELECT userid FROM %s 
                        ORDER BY random() LIMIT %d""" % \
                        (sample_from, sample_size)
    cur.execute(sampling_query)
    sampled_users = cur.fetchall()
    return sampled_users


def history_to_csv(userid, history, fieldnames, outputdir="."):
    filename = "%s/%s.csv" % (outputdir, userid)
    with open(filename, "wb") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for h in history:
            writer.writerow(h)


def get_user_history(cur, userid):
    """ Returns the history of events for a user.
    Order ascending firstly by Mixpanel's (collection) time and then
    by WNTimeSinceActivation (app's time).
    """
    cur.execute(""" SELECT userid, event, session, created_at,
                activation_time, network,
                properties->>'WNCountValue' as count_value,
                properties->>'WNStringValue' as string_value
                FROM logs
                WHERE userid='%s'
                ORDER BY created_at ASC, activation_time ASC""" % (userid))
    history = cur.fetchall()
    return history


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


if __name__ == "__main__":
    import sys, os, shutil
    if len(sys.argv) != 3:
        print "Usage: %s <usergroup> <size>"
        print "Here is a list of available usergroups to choose from: \n"
        print usergroups.keys()
        sys.exit()

    # Group to sample from and size.
    sample_from = sys.argv[1]
    sample_size = int(sys.argv[2])

    if sample_from not in usergroups:
        print "Usergroup given does not exist!"
        sys.exit()

    # Delete a dictionary if exists and create new one.
    output_dir = sample_from
    print "Creating directory %s" % output_dir,
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)
    print "\tDone."
    
    # Dates of interest.
    start_date = date(2015, 8, 9)
    end_date = date(2015, 8, 23)  # non-inclusive i.e. +1 day

    # Connect to postgres database.
    con, cur = db_connect()

    # Create usergroup.
    print "Creating usergroup %s ..." % sample_from,
    create_usergroup(cur, usergroups[sample_from], start_date, end_date)
    print "\tDone."

    # Sample some users.
    print "Sample some user IDs ...",
    userids = sample_users(cur, sample_from, sample_size)
    print "\tDone."

    # Output each user's history in a directory.
    for n, uid in enumerate(userids, 1):
        uid = uid["userid"]
        print "Sampling user: %d / %d" % (n, len(userids)),
        history = get_user_history(cur, uid)
        history_to_csv(uid, history, fieldnames, outputdir=output_dir)
        print "\tDone."

    # Close connection.
    db_close(con)
