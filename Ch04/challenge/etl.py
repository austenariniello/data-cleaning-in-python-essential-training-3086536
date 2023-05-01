"""
Load traffic.csv into "traffic" table in sqlite3 database.

Drop and report invalid rows.
- ip should be valid IP (see ipaddress)
- time must not be in the future
- path can't be empty
- status code must be a valid HTTP status code (see http.HTTPStatus)
- size can't be negative or empty

Report the percentage of bad rows. Fail the ETL if there are more than 5% bad rows
"""

# %%

import sqlite3
from contextlib import closing
from http import HTTPStatus
from ipaddress import ip_address

import pandas as pd

http_status_codes = set(HTTPStatus)


def is_valid_row(row):

    # ip should be valid IP
    # returns an error if invalid ip address format
    # error is caught by except
    try:
        ip_address(row['ip'])
    except ValueError:
        return False

    # time must not be in the future
    # time is a string when it should be a timestamp
    now = pd.Timestamp.now()
    if row['time'] > now:
        return False

    # path can't be empty
    if pd.isnull(row['path']) or not row['path'].strip():
        return False

    # status code must be a valid HTTP status code
    if row['status'] not in http_status_codes:
        return False

    # size can't be negative or empty
    if pd.isnull(row['size']) or row['size'] < 0:
        return False

    return True


def etl(csv_file, db_file):
    df = pd.read_csv(csv_file, parse_dates=['time'])
    df.info()

    bad_df = df[~df.apply(is_valid_row, axis=1)]
    percent_bad = (len(bad_df)/len(df)) * 100
    print(f'{percent_bad:.2f}% bad rows')

    if percent_bad > 5:
        raise Exception("Too many invlid data rows")
    else:
        with closing(sqlite3.connect(db_file)) as conn:
            conn.execute('BEGIN')
            with conn:
                df.to_sql('traffic', conn, if_exists='append', index=False)

# %%


if __name__ == '__main__':
    etl('traffic.csv', 'traffic.db')

# %%
