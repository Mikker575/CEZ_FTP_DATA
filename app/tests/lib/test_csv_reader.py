import io
import os

import pandas as pd
import pytest

from lib import INTERVAL, TIMEZONE, TEST_DATA
from lib.csv_reader import (last_interval_date, huawei_datalogger_csv_parser,
                            startDate, quantity, status, replacement_data, handle_missing_intervals)
from lib.json_writer import DataValidity


@pytest.fixture
def mock_current_time():
    return pd.Timestamp('2023-03-03 12:37:00', tz=TIMEZONE)


def test_last_interval_date(mock_current_time, mocker):
    mocker.patch('pandas.Timestamp.now', return_value=mock_current_time)
    expected_result = (mock_current_time.floor(f"{INTERVAL}min") - pd.Timedelta(minutes=INTERVAL))

    result = last_interval_date()

    assert result == expected_result, f"Expected {expected_result}, but got {result}"
    assert result.tz is not None, "Timestamp is not timezone-aware"
    assert result.tz == expected_result.tz, f"Timezones do not match - expected {expected_result.tz}, but it is {result.tz}"


def test_last_interval_date_at_exact_interval(mock_current_time, mocker):
    mock_current_time = pd.Timestamp('2023-03-03 12:30:00', tz=TIMEZONE)
    mocker.patch('pandas.Timestamp.now', return_value=mock_current_time)

    expected_result = (mock_current_time.floor(f"{INTERVAL}min") - pd.Timedelta(minutes=INTERVAL))
    result = last_interval_date()

    assert result == expected_result, f"Expected {expected_result}, but got {result}"
    assert result.tz is not None, "Timestamp is not timezone-aware"
    assert result.tz == expected_result.tz, f"Timezones do not match - expected {expected_result.tz}, but it is {result.tz}"


@pytest.mark.parametrize("csv_file", ['huawei_datalogger_csv_parser_valid.csv'])
def test_huawei_datalogger_csv_parser_valid(csv_file):
    with open(os.path.join(TEST_DATA, csv_file), 'rb') as f:
        file_buffer = io.BytesIO(f.read())
        file_buffer.seek(0)
        csv_data = io.StringIO(file_buffer.read().decode('utf-8'))

    date = (pd.Timestamp('2023-01-28 12:30:00', tz=TIMEZONE).floor(f"{INTERVAL}min")) - pd.Timedelta(minutes=INTERVAL)

    result_df = huawei_datalogger_csv_parser(csv_data, date)

    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty
    assert all(col in result_df.columns for col in [quantity])
    assert result_df.index.name == startDate
    assert result_df[quantity].sum() == 42
    assert str(result_df.index.tz) == "UTC"


@pytest.mark.parametrize("csv_file", ['huawei_datalogger_csv_parser_invalid.csv'])
def test_huawei_datalogger_csv_parser_invalid(csv_file):
    with open(os.path.join(TEST_DATA, csv_file), 'rb') as f:
        file_buffer = io.BytesIO(f.read())
        file_buffer.seek(0)
        csv_data = io.StringIO(file_buffer.read().decode('utf-8'))

    date = (pd.Timestamp('2023-01-28 12:30:00', tz=TIMEZONE).floor(f"{INTERVAL}min")) - pd.Timedelta(minutes=INTERVAL)

    result_df = huawei_datalogger_csv_parser(csv_data, date)

    assert isinstance(result_df, pd.DataFrame)
    assert result_df.empty
    assert all(col in result_df.columns for col in [quantity])
    assert result_df.index.name == startDate


@pytest.mark.parametrize("csv_file", ['huawei_datalogger_csv_parser_empty.csv'])
def test_huawei_datalogger_csv_parser_empty(csv_file):
    with open(os.path.join(TEST_DATA, csv_file), 'rb') as f:
        file_buffer = io.BytesIO(f.read())
        file_buffer.seek(0)
        csv_data = io.StringIO(file_buffer.read().decode('utf-8'))

    date = (pd.Timestamp('2023-01-28 12:30:00', tz=TIMEZONE).floor(f"{INTERVAL}min")) - pd.Timedelta(minutes=INTERVAL)
    start_of_day_utc = (date + pd.Timedelta(minutes=INTERVAL)).floor("D").tz_convert("UTC")

    result_df = huawei_datalogger_csv_parser(csv_data, date)

    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty
    assert all(col in result_df.columns for col in [quantity])
    assert result_df.index.name == startDate
    assert result_df[quantity].sum() == 0
    assert str(result_df.index.tz) == "UTC"
    assert len(result_df) == 1
    assert result_df.index[0] == start_of_day_utc


def test_replacement_data():
    date_tz = pd.Timestamp("2024-03-03 10:45", tz=TIMEZONE)
    expected_start_tz = date_tz.floor("D").tz_convert("UTC")
    expected_end_tz = date_tz.tz_convert("UTC")
    df_tz = replacement_data(date_tz)

    assert quantity in df_tz.columns
    assert status in df_tz.columns
    assert df_tz[quantity].eq(0).all()
    assert df_tz[status].eq(DataValidity.f.value).all()
    assert df_tz.index[0] == expected_start_tz
    assert df_tz.index[-1] == expected_end_tz
    assert str(df_tz.index.tz) == "UTC"

    date = pd.Timestamp("2024-03-03 10:45")
    expected_start = date.tz_localize(TIMEZONE).floor("D").tz_convert("UTC")
    expected_end = date.tz_localize(TIMEZONE).tz_convert("UTC")
    df = replacement_data(date)

    assert str(df.index.tz) == "UTC"
    assert df_tz.index[0] == expected_start
    assert df_tz.index[-1] == expected_end


def test_handle_missing_intervals():
    date = pd.Timestamp("2024-03-03 01:00", tz=TIMEZONE)

    datalogger_index = [
        pd.Timestamp("2024-03-03 00:00", tz=TIMEZONE),
        pd.Timestamp("2024-03-03 00:05", tz=TIMEZONE),
        pd.Timestamp("2024-03-03 00:10", tz=TIMEZONE),
        pd.Timestamp("2024-03-03 00:15", tz=TIMEZONE),
        pd.Timestamp("2024-03-03 00:20", tz=TIMEZONE),
        pd.Timestamp("2024-03-03 00:25", tz=TIMEZONE),
        pd.Timestamp("2024-03-03 00:35", tz=TIMEZONE),
        pd.Timestamp("2024-03-03 00:40", tz=TIMEZONE)
    ]

    datalogger_data = {
        quantity: [10] * len(datalogger_index),
        status: [DataValidity.f.value] * len(datalogger_index)
    }
    datalogger_df = pd.DataFrame(datalogger_data, index=datalogger_index)
    datalogger_df.index.name = startDate
    datalogger_df.index = datalogger_df.index.tz_convert("UTC")

    df = handle_missing_intervals(datalogger_df, date)

    replacement_df = replacement_data(date).drop(columns=["quantity"])
    expected_index = replacement_df.index[replacement_df.index <= date.tz_convert("UTC")]

    assert df.index.equals(expected_index)
    assert df[quantity].isna().sum() == 0
    assert df[quantity].sum() == sum([10] * len(datalogger_index))
    assert df[status].isin([e.value for e in DataValidity]).all()
