import io
import json
import logging
import warnings
import ftplib

import pandas as pd
import pysftp
from pydantic import BaseModel, SecretStr

from lib import SSH_KEY_PATH, SFTP_CONFIG, LOGGER_DT_FMT, FTP_CONFIG
from lib.csv_reader import huawei_datalogger_csv_parser, replacement_data, handle_missing_intervals
from lib.json_writer import production_to_json_bytes

log = logging.getLogger(__name__)


class FTPConfig(BaseModel):
    host: str
    port: int
    username: str
    password: SecretStr


class FtpConn(ftplib.FTP):
    """
    FTP connection class
    """
    def __init__(self, ftp_name: str):
        with open(FTP_CONFIG) as f:
            data = json.load(f)

        config = data[ftp_name]
        config = FTPConfig(**config)

        self.host = config.host
        self.port = config.port
        self.username = config.username
        self.__password = config.password.get_secret_value()

        super().__init__()

    def start_connection(self):
        """
        Start connection and login with configured credentials
        """
        try:
            super().connect(host=self.host, port=self.port)
            super().login(user=self.username, passwd=self.__password)
        except Exception as e:
            log.warning(f"Cannot start connection to FTP - {e}")

    def write_file(self, filename: str, binary_data: io.BytesIO):
        """
        Start connection, login, write file and quit connection
        """
        self.start_connection()
        try:
            self.storbinary(f"STOR {filename}", binary_data)
            log.info(f"Successfully created file {filename}")
        except Exception as e:
            log.warning(f"Cannot write file {filename} to FTP - {e}")
        self.quit()


class SftpConn(pysftp.Connection):
    """
    SFTP connection class
    """
    def __init__(self):
        """
        Connect to specific sftp host
        """
        # setting _sftp_live and _transport only to avoid warning in tests
        self._sftp_live = False
        self._transport = None
        # ignore pysftp sshkey warning due to fail in pysftp module
        warnings.simplefilter("ignore", UserWarning)
        self._cnopts = pysftp.CnOpts()
        self._cnopts.hostkeys.load(SSH_KEY_PATH)
        warnings.resetwarnings()

        # load config from json
        with open(SFTP_CONFIG) as f:
            data = json.load(f)

        config = FTPConfig(**data)

        self.host = config.host
        self.port = config.port
        self.username = config.username
        self.__password = config.password.get_secret_value()

        super().__init__(host=self.host, port=self.port, username=self.username,
                         password=self.__password, cnopts=self._cnopts)


def read_last_interval(date: pd.Timestamp) -> dict:
    """
    Read all directories on source sftp and in each folder look for file based on timestamp

    If no pod_id exists, then raises ValueError
    If there is no file in some pod_id, it generates replacement dataset

    :param date: Timestamp
    :return: dictionary with folder name (=POD of pvp) as key and DataFrame as value for all directories
    """
    project_data = {}
    with SftpConn() as sftp:
        dirs = [s for s in sftp.listdir() if sftp.isdir(s)]
        if not dirs:
            raise ValueError(f"No directories found on sftp {sftp.host} - cannot process and send any data")
        
        for pod_id in dirs:
            with sftp.cd(pod_id):
                files_attrs = sftp.listdir_attr()
                if not files_attrs:
                    log.warning(f"No matching files for pod_id {pod_id} - using replacement data")
                    project_data[pod_id] = replacement_data(date)
                else:
                    latest_filenames = [s.filename for s in files_attrs if date.strftime(LOGGER_DT_FMT) in s.filename]
                    if len(latest_filenames) == 1:
                        log.info(f"File for pod_id {pod_id} is correct")
                        df = sftp_read_and_process_csv(sftp=sftp, filename=latest_filenames[0],
                                                       date=date)
                        project_data[pod_id] = df
                    elif not latest_filenames:
                        project_data[pod_id] = replacement_data(date)
                        log.warning(
                            f"No data for {pod_id} - using replacement data")
                    else:
                        log.warning(f"Multiple matching files for {pod_id} - using file with latest time of modification")
                        files_attr = [s for s in files_attrs if s.filename in latest_filenames]
                        latest_file = max(files_attr, key=lambda s: s.st_mtime)
                        df = sftp_read_and_process_csv(sftp=sftp, filename=latest_file.filename,
                                                       date=date)
                        project_data[pod_id] = df
    return project_data


def sftp_read_and_process_csv(sftp: pysftp.Connection, filename: str, date: pd.Timestamp) -> pd.DataFrame:
    """
    Read file from sftp and convert it to DataFrame
    """
    with sftp.open(filename, 'r') as file_handle:
        file_buffer = io.BytesIO(file_handle.read())
        file_buffer.seek(0)
        decoded_file = io.StringIO(file_buffer.read().decode('utf-8'))
        df = huawei_datalogger_csv_parser(decoded_file, date=date)
        df = handle_missing_intervals(df, date=date)
    return df


def sftp_write_jsons(date: pd.Timestamp, data_dict: dict):
    """
    Go through data dict (key is POD number and value is DataFrame with interval data - convert it to CEZ json format
    and write all files to target ftp
    """
    for key, value in data_dict.items():
        json_io = production_to_json_bytes(value)
        filename = f"{key}-{date.date()}.json"
        json_io.seek(0)

        ftp = FtpConn(key)
        ftp.write_file(filename=filename, binary_data=json_io)
