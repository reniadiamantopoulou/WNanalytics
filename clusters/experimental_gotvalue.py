import json
import os.path
import pickle

from time import strptime
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import NamedTupleCursor

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from geopy import Location

geolocator = Nominatim(timeout=10)

EVENTS_TABLE = "logs"  # events logs postgres table
GEOHITS = 0
GEOQUERIES = 0
GEOTIMEOUTS = 0

GEOHASH = {}
GEOFILE = "./geohash.p"
if os.path.exists(GEOFILE):
    GEOHASH = pickle.load(open(GEOFILE))

EXCLUDE_EVENTS = set(["MapModeEnd",  # temporary, to avoid app bug on startup
                      "AppStartup",
                      "ItineraryAdd2", "ItineraryRemove2",
                      "SetStartingPointClose",  # (Pending)
                      "SetStartingPointSearchClose",  # (Pending)
                      "RadarDeactivated", "RadarActivated",
                      "OnboardingTutorialDone"
                      ])


def get_date(date_string):
    """ Returns a time struct from a string representation of a date. """
    return strptime(date_string, "%Y-%m-%d %H:%M:%S")


def string_date(dateobj):
    """ `dateobj` a datetime object """
    return dateobj.strftime("%Y-%m-%d %H:%M:%S")


def estimate_location(city):
    """ Queries OpenStreetMaps and looks for `london` or `united kingdom`
    in the ``address`` field. In case of a non-UK address, then "ROW" is
    returned. In case the city can not be resolved, "UNKNOWN" is returned.
    In case of geocoding timeout the ``city`` is returned.

    Returns:
        One of "London", "United Kingdom", "ROW", "UNKNOWN" or `city`.
    """
    global GEOHASH, GEOHITS, GEOQUERIES, GEOTIMEOUTS
    try:
        GEOHITS += 1
        return GEOHASH[city]
    except KeyError:
        GEOHITS -= 1
        GEOQUERIES += 1
        pass

    try:
        location = geolocator.geocode(city)
    except GeocoderTimedOut as e:
        GEOTIMEOUTS += 1
        return city

    if not location:
        GEOHASH[city] = "UNKNOWN"
        return GEOHASH[city]

    address = location.address.lower()
    if "london" in address:
        GEOHASH[city] = "London"
    elif "united kingdom" in address:
        GEOHASH[city] = "United Kingdom"
    else:
        GEOHASH[city] = "ROW"
    return GEOHASH[city]


def get_closest_location(history, event_index):
    """ Returns the closest, in terms of time, known city or country,
    from the current event, which is defined by the `event_index`.
    By "known" we mean a location other than "UNKNOWN" or "NOLOCATION".
    """
    unknown = ("UNKNOWN", "NOLOCATION")

    # Check if current is known.
    if history[event_index].location not in unknown:
        location = history[event_index].location
        return estimate_location(location)

    # Find previous known.
    prev_location = None
    for i in xrange(event_index-1, -1, -1):
        if history[i].location not in unknown:
            prev_location = history[i].location
            prev_time = history[i].activation_time
            break

    # Find next known.
    next_location = None
    for i in xrange(event_index+1, len(history)):
        if history[i].location not in unknown:
            next_location = history[i].location
            next_time = history[i].activation_time
            break

    if not prev_location and not next_location:
        return "UNKNOWN"

    if not next_location:
        location = prev_location
    elif not prev_location:
        location = next_location
    else:
        event_time = history[event_index].activation_time
        if abs(event_time - prev_time) > abs(next_time - event_time):
            location = prev_location
        else:
            location = next_location
    return estimate_location(location)


def find_location(user_city):
    """ Sanitizes user's location.

    Returns:
        A string which is one of "London", "ROW", "NOLOCATION", "UNKNOWN".
    """
    if user_city == "NOLOCATION" or not user_city:
        return "NOLOCATION"
    if user_city == "UNKNOWN":
        return "UNKNOWN"
    if "lond" in user_city.lower():
        return "London"
    return "ROW"


def get_user_history(cur, userid, start, end, table=EVENTS_TABLE):
    """ Returns the history of events for a user within a time period.
    Order ascending firstly by Mixpanel's (collection) time and then
    by WNTimeSinceActivation (app's time).
    """
    cur.execute(""" SELECT userid, event, session, created_at,
                activation_time,
                COALESCE(city, 'UNKNOWN') as location,
                properties->>'WNCountValue' as count_value,
                properties->>'WNNetworkAvailable' as network
                FROM %s
                WHERE created_at >= %s and created_at < %s and userid=%s
                ORDER BY created_at, activation_time """ %
                (table, "%s", "%s", "%s"), (start, end, userid))
    history = cur.fetchall()
    return history


def get_unique_users(cur, start, end, table=EVENTS_TABLE):
    """ `cur` postgres cursor
        `table` table in database
        `start` and `end` are datetime objects
    """
    try:
        # Unique users in this period.
        cur.execute(""" SELECT DISTINCT(userid) FROM %s
                    WHERE created_at >= %s and created_at < %s""" %
                    (table, "%s", "%s"), (start, end))
        unique_users = cur.fetchall()
        return unique_users
    except Exception as e:
        print "Error on getting unique users: ", e
        raise


def user_got_value(history, interval=600, userid=None):
    """ Detects if a user got the offline value of the app.

    The criterion is if he has generated more than 5 continuous offline
    events in at least a time threshold (default: 600 seconds = 10 minutes).

    NOTE: Given that most events have an *End/*Closed event that follows them,
    we ignore them, since they're not actual user actions.
    Exception to this rule is the "MapIteraction" event which comes in batches
    of 10 events. Therefore, "MapIteraction" is automatically promoted as a
    successful sequence.

    Returns:
        A tuple (session, activation_time, location), of the last event, of the
        first satisfying offline sequence or None if there was not any.
    """
    gotvalue = None
    state = 0
    event_cnt = 0  # counter of events excluding ones in EXCLUDE_EVENTS
    for i, e in enumerate(history):
        if e.event in EXCLUDE_EVENTS:
            continue
        else:
            event_cnt += 1

        if e.network == "false":
            if state == 0:
                activation_start = e.activation_time
                activation_event = event_cnt
                state = 1
                continue

            # True if we have a satisfying offline sequence.
            if ((e.activation_time - activation_start >= interval) and
               (event_cnt - activation_event + 1 >= 5 or
               e.event == "MapIteraction")):

                # Assign the location of the closest known one.
                location = get_closest_location(history, i)
                gotvalue = (e.session, e.activation_time, location, string_date(e.created_at))
                break
        else:
            state = 0

    return gotvalue


def experimental_sessions(history):
    """ Flags which user sessions are experimental.

    Criteria for flagging a session as experimental are:
        - itinerary add / details ratio < threshold (default: 3)
        - time / itinerary modifications < threshold (default: 10sec)
        - unrealistic itineraries: >= threshold (default: 10 POI)
    Returns:
        A list of sessions that are flagged as experimental.
    """
    it_add_details_ratio = 3
    time_it_modifications_ratio = 10
    unrealistic_it = 10

    flagged = set([])
    prev_session = -1
    for i, e in enumerate(history):
        if e.session in flagged:
            continue

        # Reset counters for the new session.
        if e.session != prev_session:
            session_start = e.activation_time
            it_add = 0
            it_rm = 0
            details = 0

        # Compute sufficient quantities.
        event_time = e.activation_time - session_start
        currentPOI = 0
        if e.event == "ItineraryAdd":
            it_add += 1
            try:
                currentPOI = int(e.count_value)
            except:
                pass
        if e.event == "ItineraryRemove":
            it_rm += 1
        if e.event == "DetailsChecked":
            details += 1

        it_modifications = it_add + it_rm  # itinerary modifications

        # Apply flagging criteria.
        if ((it_modifications >= 3 and
                event_time < it_modifications * time_it_modifications_ratio)
                or (it_add >= 3 and it_add > details * it_add_details_ratio)
                or (currentPOI > unrealistic_it)):
            flagged.add(e.session)

        prev_session = e.session

    return flagged


def flag_sessions(cur, start, end, table=EVENTS_TABLE):
    """ Driver of sessions flagging. """
    results = {}
    unique_users = get_unique_users(cur, start, end, table=table)
    for i, u in enumerate(unique_users, 1):
        if i % 500 == 0:
            print "Processing user: %d / %d" % (i, len(unique_users))
        userid = u[0]
        history = get_user_history(cur, userid, start, end, table=table)
        results[userid] = {"gotvalue": user_got_value(history, userid=userid),
                           "flagged": list(experimental_sessions(history))}
    return results


def db_connect(
        user="analytics", password="analytics",
        dbname="analytics", table=EVENTS_TABLE):
    """ Wrapper for connecting to db.

    Returns:
        A tuple of connection and NamedTupleCursor cursor object.
    """
    # Schema: userid(varchar), event(varchar), created_at(date),
    #         session(int), properties(json)
    try:
        con = psycopg2.connect("dbname=%s user=%s password=%s" %
                               (dbname, user, password))
        cur = con.cursor(cursor_factory=NamedTupleCursor)
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


if __name__ == "__main__":
    # Connect to postgres database.
    con, cur = db_connect()

    delta = {
        "daily": timedelta(days=1),
        "weekly": timedelta(days=7),
        "monthly": timedelta(days=30)
    }

    start = datetime(2015, 1, 15, 0)
    final = datetime(2015, 11, 25, 0)  # exclusive! should be +1 from db last date
    mode = "monthly"
    results = {}
    while start < final:
        print "Processing date range from...", start
        end = start + delta[mode]
        batch_results = flag_sessions(cur, start, end)
        for u, v in batch_results.iteritems():
            try:
                results[u]["flagged"].extend(v["flagged"])
                if not results[u]["gotvalue"]:
                    results[u]["gotvalue"] = v["gotvalue"]
            except KeyError:
                results[u] = v
        start = end

    # Close connection.
    db_close(con)

    # Save json file with results.
    print "Saving results to json..."
    with open("experimental_gotvalue.json", "w+") as f:
        json.dump(results, f)

    # Save geohash for checking.
    import pickle
    print "Saving geohash table..."
    pickle.dump(GEOHASH, open("geohash.p", "wb"))
    print "Geohits: ", GEOHITS
    print "Geoqueries: ", GEOQUERIES
    print "Geotimeouts: ", GEOTIMEOUTS
