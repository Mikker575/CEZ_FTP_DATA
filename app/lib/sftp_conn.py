import io
import json
import logging
import warnings
from typing import Literal

import pandas as pd
import pysftp
from pydantic import BaseModel, SecretStr

from lib import SSH_KEY_PATH, DATA_SOURCES_CONFIG, LOGGER_DT_FMT
from lib.csv_reader import huawei_datalogger_csv_parser, replacement_data, handle_missing_intervals
from lib.json_writer import production_to_json_bytes

log = logging.getLogger(__name__)


class FTPConfig(BaseModel):
    host: str
    port: int
    username: str
    password: SecretStr


class DataSourcesConfig(BaseModel):
    source_ftp: FTPConfig
    target_ftp: FTPConfig


class SftpConn(pysftp.Connection):
    """
    SFTP connection class
    """
    def __init__(self, sftp_source: Literal["source_ftp", "target_ftp"]):
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
        with open(DATA_SOURCES_CONFIG) as f:
            data = json.load(f)

        if sftp_source not in data.keys():
            raise ValueError(f"Invalid sftp_source: {sftp_source}")

        config = DataSourcesConfig(**data)
        source = getattr(config, sftp_source)

        self.host = source.host
        self.port = source.port
        self.username = source.username
        self.__password = source.password.get_secret_value()

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
    with SftpConn("source_ftp") as sftp:
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
    with SftpConn("target_ftp") as sftp:

        for key, value in data_dict.items():
            json_io = production_to_json_bytes(value)
            filename = f"{key}-{date.date()}.json"
            remote_path = f"./{filename}"
            json_io.seek(0)

            with sftp.open(remote_path, "wb") as remote_file:
                remote_file.write(json_io.getvalue())
                log.info(f"Successfully created file {remote_path}")
