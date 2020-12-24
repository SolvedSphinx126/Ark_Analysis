if __name__ == '__main__':

    import requests         # HTTP Requests
    import csv              # CSV
    import re               # RegEx
    import sqlite3 as sl    # Database

    con = sl.connect('ARK.db') # connect to local database

    ARKK = "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv"
    ARKQ = "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_AUTONOMOUS_TECHNOLOGY_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv"
    ARKG = "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_GENOMIC_REVOLUTION_MULTISECTOR_ETF_ARKG_HOLDINGS.csv"
    ARKW = "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv"
    ARKF = "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv"
    PRNT = "https://ark-funds.com/wp-content/fundsiteliterature/csv/THE_3D_PRINTING_ETF_PRNT_HOLDINGS.csv"

    stocks = [ # list of ETF information tuples
        ("ARKK", "arkk.csv", ARKK),
        ("ARKQ", "arkq.csv", ARKQ),
        ("ARKG", "arkg.csv", ARKG),
        ("ARKW", "arkw.csv", ARKW),
        ("ARKF", "arkf.csv", ARKF),
        ("PRNT", "prnt.csv", PRNT)
    ]

    for stock in stocks:    #for each ETF

        j = requests.get(stock[2], allow_redirects=True)                    # get CSV file
        open(stock[1], "wb").write(j.content)                               # write local CSV copy
        sql = 'INSERT INTO ' + stock[0] + ' values(?, ?, ?, ?, ?, ?, ?)'    # setup SQL add Command
        data = [                                                            # declare list for ARK ETFs

        ]
        with open(stock[1], newline="") as file:                                                        # open local CSV file
            reader = csv.reader(file)                                                                   # define file reader
            for row in reader:                                                                          # for each line in the csv file
                if re.search("\d+/\d+/\d+", str(row)):                                                  # if data starts with a date
                    info = str(row).strip('[\']').replace(" ", "_").split("\',_\'")                     # split data into list
                    data.append((info[0], info[2], info[3], info[4], info[5], info[6], info[7]))        # add ETF data list to ARK data list
                    date = info[0]                                                                      # check dates to prevent duplicates
        cur = con.cursor()                                                                              # create a cursor on the connection
        cur.execute('SELECT DISTINCT pos_date FROM ARKK')                                               # execute an SQL command that queries the database for a list of unique dates already on the database
        rows = cur.fetchall()                                                                           # assign the list of dates to rows
        flag = True                                                                                     # set a sentinel flag to check for any repeated dates
    for row in rows:                                                                                    # for each date on the database already
            flag = flag & (str(row).strip("()',") != date)                                              # set flag to false if there is a repeated date
            #print(str(row).strip("()',") + " " + str(flag))                                            # DEBUG: prints a list of dates found on the database and the flag status (False = repeated dates)
        if (flag):                                                                                      # if the date in the CSV isn't in the database
            with con:                                                                                   # open local database
                con.executemany(sql, data)                                                              # Execute SQL add command

