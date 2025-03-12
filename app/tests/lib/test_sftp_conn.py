import io
from unittest.mock import MagicMock, patch
import unittest

import pandas as pd
import pytest
from pydantic import ValidationError

from lib import LOGGER_DT_FMT
from lib.csv_reader import replacement_data, startDate, quantity, status
from lib.sftp_conn import SftpConn, read_last_interval, sftp_write_jsons, FTPConfig, FtpConn


def test_data_sources_config():
    config_data = {
            "host": "example.com",
            "port": 22,
            "username": "user",
            "password": "pass123"
    }

    config = FTPConfig(**config_data)

    assert config.host == "example.com"
    assert config.port == 22
    assert config.username == "user"
    assert config.password.get_secret_value() == "pass123"
    assert str(config.password) == "**********"


def test_invalid_data_sources_config():
    invalid_data = {
            "host": "example.com",
            "port": 22,
            "username": "user"
    }

    with pytest.raises(ValidationError):
        FTPConfig(**invalid_data)


@pytest.fixture
def mock_sftp():
    with patch("lib.sftp_conn.SftpConn") as MockSftp:
        sftp = MockSftp.return_value.__enter__.return_value
        yield sftp


def test_no_directories(mock_sftp):
    mock_sftp.listdir.return_value = []

    with pytest.raises(ValueError, match="No directories found on sftp"):
        read_last_interval(pd.Timestamp("2024-03-04 00:00"))


def test_read_last_interval_empty_dir(mock_sftp):
    mock_sftp.listdir.return_value = ["pod_123"]
    mock_sftp.isdir.return_value = True
    mock_sftp.listdir_attr.return_value = []

    date = pd.Timestamp("2024-03-04 00:00")
    replacement_df = replacement_data(date)
    with patch("lib.sftp_conn.replacement_data", return_value=replacement_df) as mock_replacement:
        result = read_last_interval(date)

    assert "pod_123" in result
    mock_replacement.assert_called_once_with(date)


def test_read_last_interval_single_file(mock_sftp):
    date = pd.Timestamp("2024-03-04")
    mock_sftp.listdir.return_value = ["pod_123"]
    mock_sftp.isdir.return_value = True
    mock_sftp.listdir_attr.return_value = [
        MagicMock(filename=f"{date.strftime(LOGGER_DT_FMT)}.csv", st_mtime=12345)
    ]

    sftp_processed_data = pd.DataFrame([
        {startDate: pd.Timestamp('2025-03-03 23:00:00+0000', tz='UTC'), status: 'w', quantity: 0.0},
        {startDate: pd.Timestamp('2025-03-03 23:05:00+0000', tz='UTC'), status: 'f', quantity: 0.0}
    ])
    sftp_processed_data = sftp_processed_data.set_index(startDate)
    with patch("lib.sftp_conn.sftp_read_and_process_csv", return_value=sftp_processed_data) as mock_csv_reader:
        result = read_last_interval(date)

    assert "pod_123" in result
    assert not result["pod_123"].empty
    assert isinstance(result["pod_123"], pd.DataFrame)
    mock_csv_reader.assert_called_once_with(sftp=mock_sftp, filename=f"{date.strftime(LOGGER_DT_FMT)}.csv", date=date)


def test_read_last_interval_multiple_files(mock_sftp):
    date = pd.Timestamp("2024-03-04")
    mock_sftp.listdir.return_value = ["pod_123"]
    mock_sftp.isdir.return_value = True
    mock_sftp.listdir_attr.return_value = [
        MagicMock(filename=f"{date.strftime(LOGGER_DT_FMT)}.csv", st_mtime=12345),
        MagicMock(filename=f"{date.strftime(LOGGER_DT_FMT)}.csv", st_mtime=23456),
    ]

    sftp_processed_data = pd.DataFrame([
        {startDate: pd.Timestamp('2025-03-03 23:00:00+0000', tz='UTC'), status: 'w', quantity: 0.0},
        {startDate: pd.Timestamp('2025-03-03 23:05:00+0000', tz='UTC'), status: 'f', quantity: 0.0}
    ])
    sftp_processed_data = sftp_processed_data.set_index(startDate)
    with patch("lib.sftp_conn.sftp_read_and_process_csv", return_value=sftp_processed_data) as mock_csv_reader:
        result = read_last_interval(date)

    assert "pod_123" in result
    assert len(result) == 1
    assert not result["pod_123"].empty
    mock_csv_reader.assert_called_once_with(sftp=mock_sftp, filename=f"{date.strftime(LOGGER_DT_FMT)}.csv", date=date)


@pytest.fixture
def mock_open():
    mock_data = '{"ftp_name": {"host": "localhost", "port": 21, "username": "ftp_name", "password": "password"}}'
    with patch('builtins.open', unittest.mock.mock_open(read_data=mock_data)):
        yield


@patch('lib.sftp_conn.FtpConn')
def test_init(mock_ftp_class, mock_open):
    mock_ftp_class.return_value = MagicMock()

    ftp_conn = FtpConn('ftp_name')

    assert ftp_conn.host == "localhost"
    assert ftp_conn.port == 21
    assert ftp_conn.username == "ftp_name"
    assert ftp_conn._FtpConn__password == "password"


@patch('ftplib.FTP.connect')
@patch('ftplib.FTP.login')
def test_start_connection(mock_login, mock_connect, mock_open):
    ftp_conn = FtpConn('ftp_name')

    ftp_conn.start_connection()

    mock_connect.assert_called_once_with(host=ftp_conn.host, port=ftp_conn.port)
    mock_login.assert_called_once_with(user=ftp_conn.username, passwd=ftp_conn._FtpConn__password)


@patch('ftplib.FTP.storbinary')
@patch('ftplib.FTP.quit')
def test_write_file(mock_quit, mock_storbinary, mock_open):
    ftp_conn = FtpConn('ftp_name')

    filename = "test.txt"
    binary_data = io.BytesIO(b"Some binary content")

    ftp_conn.write_file(filename, binary_data)

    mock_storbinary.assert_called_once_with(f"STOR {filename}", binary_data)

    mock_quit.assert_called_once()
