from mixpanel_raw import Mixpanel

import json
from collections import defaultdict

import os
import time
import ConfigParser
from datetime import date
from datetime import timedelta
from datetime import datetime


def next_range(start, end, dayrange=15):
    """ Generates pairs of (from, to) date objects given a starting and
    ending date.  The pairs are ranges of `dayrange` days.
    """
    oneday = timedelta(days=1)
    delta = timedelta(days=dayrange - 1)
    from_date = start

    while from_date <= end:
        to_date = min(from_date + delta, end)
        yield from_date, to_date
        from_date = to_date + oneday


def download_mixpanel(start, end, output_dir="data"):
    """ Accepts the `date` objects `start` and `end`, and downloads
    the raw data from Mixpanel. The final json files are stored by
    default in the `./data` directory.
    """
    api_key = "8fafecaae9f01db61646fe963b313919"
    api_secret = "e6b841e008f313ae3b585068cf506dff"
    api = Mixpanel(api_key=api_key, api_secret=api_secret)

    # If output directory exists, rename it and create an empty one.
    try:
        if os.path.isdir(output_dir):
            filename = "backup_%s_%s" % (output_dir, datetime.utcnow().date())
            os.rename(output_dir, filename)
        os.makedirs(output_dir)
    except OSError as e:
        raise "Error when creating download directory for Mixpanel data.", e

    for batch, (from_date, to_date) in enumerate(next_range(start, end)):
        # Download data. 
        # Returns string. Each row = json entry = an event.
        print "[DL] dates: %s to %s ..." % (from_date, to_date)
        dl_start = time.time()
        data = api.request(['export'], {
            'from_date' : from_date, # in UTC
            'to_date' : to_date,
        })
        print "[DL] finished in: %d seconds." % (time.time() - dl_start)

        # Write data to file.
        filename = "%s/%s_%s.json" % (output_dir, from_date, to_date)
        print "[WR] to file: %s" % filename
        write_start = time.time()
        with open(filename, 'w+') as f:
            f.write(data)
        print "[WR] finished in: %d seconds." % (time.time() - write_start)

        # Sleep for 2 seconds.
        time.sleep(2)


def auto_download(config_file="mixpanel_download.ini"):
    """ Opens the ``config_file`` and reads the last updated date.
    Then sets the date range for downloading, which is +1 day after 
    the last update and 2 days before in UTC time.

    NOTE: The 2 days is only for safety reasons, due to delays in 
    data collection by Mixpanel. They say you can download till the 
    previous day, but keep it as 2 just to be safe.
    """
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    last_update = config.get("Dates", "Last update")
    last_update = datetime.strptime(last_update, "%Y-%m-%d").date()
    start = last_update + timedelta(days=1)
    end = (datetime.utcnow() - timedelta(days=2)).date()
    download_mixpanel(start, end)

    # On success store the last updated date back to the .ini file.
    with open(config_file, "w") as f:
        config.set("Dates", "Last update", end)
        config.write(f)

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print "Usage: %s <from> <to>"  % (sys.argv[0])
        print "from: initial day for download inclusive. (format: y-m-d)"
        print "to: ending day (inclusive). (format: y-m-d)"
        print "e.g. %s 2015-2-1 2015-2-16" % (sys.argv[0])
        print "Will download from 1st till 16th of February 2015." 
        sys.exit(-1)

    start_day, end_day = sys.argv[1:]
    start = datetime.strptime(start_day, "%Y-%m-%d").date()
    end = datetime.strptime(end_day, "%Y-%m-%d").date()
    download_mixpanel(start, end)
