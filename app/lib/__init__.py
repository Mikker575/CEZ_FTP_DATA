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
