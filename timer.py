import fetch
import schedule
import time

schedule.every().day.at("19:05").do(fetch.update)   # fetch an update every day at 7:05 PM CST
schedule.every().day.at("03:00").do(fetch.update)   # and 3:00 AM CST
while True:
    schedule.run_pending()
    time.sleep(1)
