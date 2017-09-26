#!/usr/bin/env python3
import os
import bz2
import re
import datetime
import argparse
import time
from sys import stdout
from influxdb import client as influxdb

# Constants
TZ_OFFSET = 3  # Moscow
REGEX_SNAPTIME = r'\d{2}/\d{2}/\d{2}\ \d{2}:\d{2}:\d{2}'
REGEX_CELLNAME = r'.*\((.*)\)'
REGEX_TRASH = r'#|zzz|avg-cpu|Device|\ '
BATCH_SIZE = 2000

def parse_args():
    '''
    Parse arguments
    '''

    parser = argparse.ArgumentParser(description="Parse iostat output and import it into InfluxDB")
    parser.add_argument('-dir',
                        default=os.getcwd(),
                        metavar='directory',
                        help='Directory with iostat dumps')
    parser.add_argument('-dbhost',
                        default='localhost',
                        help='Database host')
    parser.add_argument('-dbport',
                        default=8086,
                        help='Database port')
    parser.add_argument('-dbname',
                        default='exa',
                        help='Database name')
    parser.add_argument('-dbuser',
                        help='Database user name')
    parser.add_argument('-dbpass',
                        help='Database user password')
    parsed_args = parser.parse_args()
    return vars(parsed_args)

def process_bz2(filename, dbconn):
    '''
    Parse bz2 file, construct JSON, and write it to the database
    '''

    json_body = []
    import_start = time.time()

    print("Processing:", filename)

    for line in bz2.open(filename, 'rt'):
        line = line.rstrip('\r\n')
        if not line:
            continue
        if re.match(REGEX_TRASH, line):
            continue
        if re.match(REGEX_CELLNAME, line):
            cellname = re.search(r'\((.*?)\.', line).group(1)
            continue
        if re.match(REGEX_SNAPTIME, line):
            snaptime_local = datetime.datetime.strptime(line, "%m/%d/%y %H:%M:%S")
            snaptime_utc = snaptime_local - datetime.timedelta(hours=TZ_OFFSET)
            continue

        json_body = json_body + [
            {
                "measurement": "iostat",
                "tags": {
                    "cell": cellname,
                    "disk": line.split()[0]
                },
                "time": snaptime_utc,
                "fields": {
                    "rrqm/s"  : float(line.split()[1]),
                    "wrqm/s"  : float(line.split()[2]),
                    "r/s"     : float(line.split()[3]),
                    "w/s"     : float(line.split()[4]),
                    "rsec/s"  : float(line.split()[5]),
                    "wsec/s"  : float(line.split()[6]),
                    "avgrq-sz": float(line.split()[7]),
                    "avgqu-sz": float(line.split()[8]),
                    "await"   : float(line.split()[9]),
                    "r_await" : float(line.split()[10]),
                    "w_await" : float(line.split()[11]),
                    "svctm"   : float(line.split()[12]),
                    "%util"   : float(line.split()[13])
                }
            }
        ]
        if len(json_body) >= BATCH_SIZE:
            dbconn.write_points(json_body, time_precision='s')
            json_body = []
            stdout.write(".")
            stdout.flush()

    dbconn.write_points(json_body, time_precision='s')
    stdout.write("\n")
    stdout.flush()

    import_end = time.time()

    print('Done in', round(import_end - import_start), 'seconds' )

def main():
    '''
    MAIN
    '''

    args = parse_args()
    iostat_dir = args['dir']
    dbhost = args['dbhost']
    dbport = args['dbport']
    dbuser = args['dbuser']
    dbpass = args['dbpass']
    dbname = args['dbname']

    dbconn = influxdb.InfluxDBClient(dbhost, dbport, dbuser, dbpass, dbname)

    for fname in sorted(os.listdir(iostat_dir)):
        fname_full = os.path.join(iostat_dir, fname)
        fname_ext = os.path.splitext(fname_full)[1]
        if fname_ext == ".bz2":
            process_bz2(fname_full, dbconn)

if __name__ == "__main__":
    main()
