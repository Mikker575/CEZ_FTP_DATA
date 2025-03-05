import datetime
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from lib import LOGS_DIR, INTERVAL
from lib.csv_reader import last_interval_date
from lib.sftp_conn import read_last_interval, sftp_write_jsons

log = logging.getLogger(__name__)

logging_filename = os.path.splitext(os.path.basename(__file__))[0]
logging_file = os.path.join(LOGS_DIR, f'log_{logging_filename}.log')


def main():
    date = last_interval_date()
    data = read_last_interval(date=date)
    sftp_write_jsons(date=date, data_dict=data)


if __name__ == '__main__':
    # logger configuration
    def log_unhandled_exceptions(exc_type, exc_value, exc_traceback):
        log.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] --- %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(), RotatingFileHandler(logging_file, mode='a', maxBytes=100000, backupCount=10)]
    )
    log = logging.getLogger(__name__)
    sys.excepthook = log_unhandled_exceptions

    scheduler = BackgroundScheduler()
    trigger = CronTrigger(minute=f'*/{INTERVAL}')
    scheduler.add_job(main, trigger=trigger, misfire_grace_time=10)
    scheduler.start()
    try:
        while True:
            time.sleep(0.2)
    except (KeyboardInterrupt, SystemExit):
        log.info("Exiting")
        scheduler.shutdown()
