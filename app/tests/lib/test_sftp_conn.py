import pytest
from pydantic import ValidationError

from lib.sftp_conn import DataSourcesConfig, SftpConn


def test_data_sources_config():
    config_data = {
        "source_ftp": {
            "host": "example.com",
            "port": 22,
            "username": "user",
            "password": "pass123"
        },
        "target_ftp": {
            "host": "example2.com",
            "port": 2222,
            "username": "user2",
            "password": "pass456"
        }
    }

    config = DataSourcesConfig(**config_data)

    assert config.source_ftp.host == "example.com"
    assert config.source_ftp.port == 22
    assert config.source_ftp.username == "user"
    assert config.source_ftp.password.get_secret_value() == "pass123"
    assert str(config.source_ftp.password) == "**********"


def test_invalid_data_sources_config():
    invalid_data = {
        "source_ftp": {
            "host": "example.com",
            "port": 22,
            "username": "user"
        }
    }

    invalid_source_name = {
        "ftp_source": {
            "host": "example.com",
            "port": 22,
            "username": "user",
            "password": "pass123"
        }
    }

    with pytest.raises(ValidationError):
        DataSourcesConfig(**invalid_data)

    with pytest.raises(ValidationError):
        DataSourcesConfig(**invalid_source_name)


def test_invalid_sftp_source():
    with pytest.raises(ValueError, match="wrong_ftp_source"):
        SftpConn("wrong_ftp_source")
