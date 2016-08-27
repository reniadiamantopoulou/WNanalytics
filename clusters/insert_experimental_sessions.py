import datetime
import psycopg2
import json


def bulk_insert(cursor, table, entries):
    if not entries:
        return
    insert = "INSERT INTO %s VALUES " % (table)
    query = cursor.mogrify(insert + ",".join(["%s"] * len(entries)), entries)
    cursor.execute(query)


if __name__ == "__main__":
    user = "analytics"
    password = "analytics"
    dbname = "analytics"
    table = "experimental_sessions"

    # Insert flagged sessions. Schema: userid, session
    filename = "experimental_gotvalue.json"
    with open(filename) as f:
        results = json.load(f)

    # connect to db.
    try:
        con = psycopg2.connect("dbname=%s user=%s password=%s" %
            (dbname, user, password))
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS %s" % (table))
        cur.execute(""" CREATE TABLE %s (
                            userid varchar(128),
                            session INT 
                        )""" % (table))
    except Exception as e:
        print "Error: ", e
        raise 

    # Bulk insert all the flagged sessions.
    session_entries = []
    for userid, val in results.iteritems():
        for session in val["flagged"]:
            session_entries.append((userid, session))
    bulk_insert(cur, table, session_entries)

    # Create indexes on: userid and session
    cur.execute("CREATE INDEX ON %s (userid)" % (table))
    cur.execute("CREATE INDEX ON %s (session)" % (table))

    # Commit changes.
    con.commit()
    con.close()
