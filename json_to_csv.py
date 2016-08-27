import sys
import json
import csv

from analytics_usergroups import usergroups

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: %s <json_file>" % sys.argv[0]
        sys.exit()
    jsonfile = sys.argv[1]
    
    with open(jsonfile) as jfp:
        statistics = json.load(jfp)

    for date_start in statistics:
        output_filename = "report_%s.csv" % date_start
        with open(output_filename, "wb") as csvfile:
            report = csv.writer(csvfile, delimiter=',')
            for g in statistics[date_start]:
                for date_end, queries in statistics[date_start][g].iteritems():
                    if not queries:
                        continue

                    description = usergroups[g].get("description", "")
                    report.writerow(["Group: " + description])

                    for cat_id, cat_query_list in queries.iteritems():
                        for q in cat_query_list:
                            report.writerow(["Query: " + q.get("title", "")])
                            report.writerow([""]) # separate group/query from results
                            fields = q.get("fields", [])
                            result = q.get("result", [])
                            if fields:
                                report.writerow(fields)
                            for r in result:
                                if fields:
                                    report.writerow([r[f] for f in fields])
                                else:
                                    report.writerow(r.values())
                            report.writerow([""]) # separate the queries with empty line
                report.writerow([""]) # separate the groups with empty line
                report.writerow([""]) # separate the groups with empty line

    print "Output saved to: %s" % output_filename
