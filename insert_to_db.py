import datetime
import psycopg2
import json
import glob
from time import time

HOSTNAME = "sql-event-analytics.ctovz3c435p3.us-east-1.rds.amazonaws.com"

def bulk_insert(cursor, table, entries):
    if not entries:
        return
    insert = "INSERT INTO %s VALUES " % (table)
    query = cursor.mogrify(insert + ",".join(["%s"] * len(entries)), entries)
    cursor.execute(query)


def insert_json_file(cursor, file_data, table):
    entries = []
    with open(file_data) as f:
        for line in f:
            data = json.loads(line.strip())

            # Extract fields:
            # userid, event, time, time_since_activation, session, city
            event = data['event']
            properties = data['properties']
            timestamp = datetime.datetime.utcfromtimestamp(
                properties['time']).strftime("%Y-%m-%d %H:%M:%S")
            userid = properties['distinct_id']
            session = int(properties['WNSessionNumber'])
            activation_time = int(properties['WNTimeSinceActivation'])
            network = properties.get('WNNetworkAvailable', True)

            # Extend `city` to be a wider London area. The proper geo-coded
            # value is still stored in the json. If it's not in the wider
            # London area, then just use the geocoded city.
            userlat = properties.get("WNUserLatitude", "NOLOCATION")
            userlon = properties.get("WNUserLongitude", "NOLOCATION")
            try:
                userlat = float(userlat)
                userlon = float(userlon)
                if userlat < 51.951883 and userlat > 51.041394 and \
                   userlon > -0.812988 and userlon < 0.60150:
                    city = "London"
                else:
                    city = properties.get('WNCity', 'NOLOCATION')
            except ValueError:
                city = properties.get('WNCity', 'NOLOCATION')

            entry = (userid, event, timestamp, session, network,
                     activation_time, city, json.dumps(properties))
            entries.append(entry)

            if len(entries) == 5000:
                bulk_insert(cursor, table, entries)
                entries = []

    # Flush whatever is left.
    if entries:
        bulk_insert(cursor, table, entries)


def insert_to_db(user="cybereye", password="AstroMarseille_3", dbname="analytics", table="logs", host=HOSTNAME):

    # Connect to db.
    try:
        con = psycopg2.connect("host=%s dbname=%s user=%s password=%s" %
                               (host, dbname, user, password))
        cur = con.cursor()
        cur.execute(""" CREATE TABLE IF NOT EXISTS logs (
                            userid varchar(128),
                            event varchar(64),
                            created_at timestamptz,
                            session int,
                            network boolean,
                            activation_time int,
                            city varchar(64),
                            properties json
                        )""")

        # Drop indexes before INSERT.
        cur.execute("DROP INDEX IF EXISTS logs_userid_idx")
        cur.execute("DROP INDEX IF EXISTS logs_event_idx")
        cur.execute("DROP INDEX IF EXISTS logs_created_at_idx")
        cur.execute("DROP INDEX IF EXISTS logs_session_idx")
        cur.execute("DROP INDEX IF EXISTS logs_network_idx")
        cur.execute("DROP INDEX IF EXISTS logs_activation_time_idx")
        cur.execute("DROP INDEX IF EXISTS logs_city_idx")
        cur.execute("DROP INDEX IF EXISTS logs_lower_city_idx")

    except Exception as e:
        print "Error: ", e
        raise

    # Open json file and insert data.
    data_dir = "data/*.json"
    for data_file in glob.glob(data_dir):
        print "Inserting %s" % data_file
        start_time = time()
        insert_json_file(cur, data_file, table)
        print "Finished in %f seconds." % (time() - start_time)

    # Create indexes on all exposed fields.
    print "Creating indexes..."
    create_index_time = time()
    cur.execute("CREATE INDEX logs_userid_idx ON logs (userid)")
    cur.execute("CREATE INDEX logs_event_idx ON logs (event)")
    cur.execute("CREATE INDEX logs_created_at_idx ON logs (created_at)")
    cur.execute("CREATE INDEX logs_session_idx ON logs (session)")
    cur.execute("CREATE INDEX logs_network_idx ON logs (network)")
    cur.execute("CREATE INDEX logs_activation_time_idx ON logs (activation_time)")
    cur.execute("CREATE INDEX logs_city_idx ON logs (city)")
    cur.execute("CREATE INDEX logs_lower_city_idx ON logs (LOWER(city) text_pattern_ops)")
    print "Finished in %f seconds." % (time() - create_index_time)

    # Refresh materialized views: first_cities, first_time_users
    print "Refreshing materialized views ..."
    refreshing_views_time = time()
    cur.execute("REFRESH MATERIALIZED VIEW first_cities")
    cur.execute("REFRESH MATERIALIZED VIEW first_time_users")
    print "Finished in %f seconds." % (time() - refreshing_views_time)

    # Commit changes.
    con.commit()
    con.close()


if __name__ == '__main__':
    insert_to_db()
