import os

APP_PATH = os.path.dirname(os.path.dirname(__file__))
DATA_PATH = os.path.join(os.path.dirname(APP_PATH), 'data')
CONFIG_PATH = os.path.join(DATA_PATH, "config")
SFTP_CONFIG = os.path.join(CONFIG_PATH, "sftp.json")
FTP_CONFIG = os.path.join(CONFIG_PATH, "ftp.json")
LOGS_DIR = os.path.join(DATA_PATH, "logs")
TEST_DATA = os.path.join(DATA_PATH, "test_data")
SSH_KEY_PATH = os.path.join(DATA_PATH, ".ssh", "known_hosts.txt")
TIMEZONE = "Europe/Budapest"
INTERVAL = 5
LOGGER_DT_FMT = "%Y%m%d"
LOGGER_DT_FMT_2 = "%y%m%d"
HUB_DT_FMT = "%Y%m%d %H%M%S"
HUB_CSV_DT_FMT = "%Y-%m-%dT%H:%M:%S"
LOGGER_CSV_DT_FORMAT = "%Y-%m-%d %H:%M:%S"
LOGGER_CSV_DT_FORMAT_2 = "%y-%m-%d %H:%M:%S"
