import sqlite3 as sl  # Database
import fetch

con = sl.connect('ARK.db')                                  # connect to local database
queries = [
    "DELETE FROM %FUND WHERE pos_date = '12/24/2020'",      # custom SQL queries to execute - use "%FUND" in place of fund tickers
    "SELECT * FROM %FUND WHERE pos_date = '12/24/2020'"
]
for query in queries:                                       # for each query
    for stock in fetch.stocks:                              # for each fund
        queryR = query.replace("%FUND", stock[0])           # replace
        cur = con.cursor()                                  # create database cursor
        cur.execute(queryR)                                 # execute current command
        rows = cur.fetchall()                               # get the query results
        print("\n" + queryR)                                # print the actual query
        for row in rows:                                    # for each line of the query response
            print(row)                                      # print the line
