import io
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pydantic import ValidationError

from lib import LOGGER_DT_FMT
from lib.csv_reader import replacement_data, startDate, quantity, status
from lib.sftp_conn import DataSourcesConfig, SftpConn, read_last_interval, sftp_write_jsons


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


sample_date = pd.Timestamp("2024-03-04 00:00")
sample_df = pd.DataFrame({
    'timestamp': [sample_date, sample_date + pd.Timedelta(minutes=15)],
    'production': [10, 20]
})
data_dict = {"pod_123": sample_df}


def mock_production_to_json_bytes(df):
    fake_json = '{"production": [10, 20], "timestamp": ["2024-03-04T00:00:00", "2024-03-04T00:15:00"]}'
    return io.BytesIO(fake_json.encode("utf-8"))


@patch("lib.sftp_conn.SftpConn")
@patch("lib.sftp_conn.production_to_json_bytes", side_effect=mock_production_to_json_bytes)
@patch("lib.sftp_conn.log.info")
def test_sftp_write_jsons(mock_log_info, mock_production_to_json_bytes, mock_sftp_conn):
    mock_sftp_instance = MagicMock()
    mock_sftp_conn.return_value.__enter__.return_value = mock_sftp_instance
    mock_sftp_instance.open.return_value.__enter__.return_value = MagicMock()

    sftp_write_jsons(sample_date, data_dict)

    mock_production_to_json_bytes.assert_called_once_with(sample_df)
    mock_sftp_instance.open.assert_called_once_with("./pod_123-2024-03-04.json", "wb")
    mock_log_info.assert_called_once_with("Successfully created file ./pod_123-2024-03-04.json")
    mock_sftp_instance.open.return_value.__enter__.return_value.write.assert_called_once_with(
        b'{"production": [10, 20], "timestamp": ["2024-03-04T00:00:00", "2024-03-04T00:15:00"]}')
