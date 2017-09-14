import os
import sys
from datetime import datetime
import json
import csv

def date(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f")

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("USAGE: %s measurement\n" % sys.argv[0])
        sys.exit(1)
    path = sys.argv[1]

    with open(os.path.join(path, "metadata.json")) as f:
        metadata = json.load(f)
        start = date(metadata["start"][:-1])
        end = date(metadata["start"][:-1])
        print('open measurement "%s" from "%s" to "%s"', metadata["name"], start, end)
        for service in metadata["services"]:
            print('open service "%s"' % service["name"])
            with open(os.path.join(path, service["filename"])) as csvfile:
                r = csv.DictReader(csvfile, dialect=csv.excel_tab)
                for row in r:
                    print(row["time"])

if __name__ == '__main__':
    main()
