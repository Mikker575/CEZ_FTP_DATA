import json
import logging
import warnings
from typing import Literal

import pysftp
from pydantic import BaseModel, SecretStr

from lib import SSH_KEY_PATH, DATA_SOURCES_CONFIG

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
