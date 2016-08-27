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
    table = "gotvalue"

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
                            created_at timestamptz,
                            session INT,
                            activation_time INT,
                            city varchar(128)
                        )""" % (table))
    except Exception as e:
        print "Error: ", e
        raise 

    # Bulk insert all the flagged sessions.
    gotvalue_entries = []
    for userid, val in results.iteritems():
        g = val["gotvalue"]
        if g:
            gotvalue_entries.append((userid, g[3], g[0], g[1], g[2]))
    bulk_insert(cur, table, gotvalue_entries)

    # Create indexes on: userid and session
    cur.execute("CREATE INDEX ON %s (userid)" % (table))
    cur.execute("CREATE INDEX ON %s (created_at)" % (table))
    cur.execute("CREATE INDEX ON %s (session)" % (table))

    # Commit changes.
    con.commit()
    con.close()
