import io
import json

import pandas as pd
import pytest
from pydantic import ValidationError

from lib import TIMEZONE
from lib.csv_reader import quantity, status, startDate
from lib.json_writer import (DataValidity, TimeSeries, JsonDataCEZ, dump_df, production_to_json_bytes)


def test_parse_timestamp_validator():
    timestamp = "2024-03-04T12:00:00"
    data = {startDate: timestamp, quantity: 0, status: DataValidity.f.value}
    model = TimeSeries(**data)
    assert isinstance(model.startDate, pd.Timestamp)
    assert model.startDate == pd.Timestamp(timestamp)

    timestamp = pd.Timestamp("2024-03-04T12:00:00", tz=TIMEZONE)
    data = {startDate: timestamp, quantity: 0, status: DataValidity.f.value}
    model = TimeSeries(**data)
    assert model.startDate is timestamp

    timestamp = "1.1.2024 00:00"
    data = {startDate: timestamp, quantity: 0, status: DataValidity.f.value}
    model = TimeSeries(**data)
    assert isinstance(model.startDate, pd.Timestamp)
    assert model.startDate == pd.Timestamp(timestamp)

    timestamp = "random"
    data = {startDate: timestamp, quantity: 0, status: DataValidity.f.value}
    with pytest.raises(ValidationError):
        TimeSeries(**data)

    timestamp = ""
    data = {startDate: timestamp, quantity: 0, status: DataValidity.f.value}
    with pytest.raises(ValidationError):
        TimeSeries(**data)

    timestamp = 10
    data = {startDate: timestamp, quantity: 0, status: DataValidity.f.value}
    with pytest.raises(ValidationError):
        TimeSeries(**data)


def test_dump_df_valid():
    df_index = [
        pd.Timestamp("2024-03-03 00:00", tz=TIMEZONE),
        pd.Timestamp("2024-03-03 00:05", tz=TIMEZONE),
        pd.Timestamp("2024-03-03 00:10", tz=TIMEZONE),
    ]
    df_data = {
        quantity: [10] * len(df_index),
        status: [DataValidity.f.value] * len(df_index)
    }
    df = pd.DataFrame(df_data, index=df_index)
    df.index.name = startDate
    df.index = df.index.tz_convert("UTC")

    result = dump_df(df, TimeSeries)

    expected = [
        {startDate: pd.Timestamp("2024-03-03 00:00", tz=TIMEZONE).tz_convert("UTC"), quantity: 10, status: DataValidity.f.value},
        {startDate: pd.Timestamp("2024-03-03 00:05", tz=TIMEZONE).tz_convert("UTC"), quantity: 10, status: DataValidity.f.value},
        {startDate: pd.Timestamp("2024-03-03 00:10", tz=TIMEZONE).tz_convert("UTC"), quantity: 10, status: DataValidity.f.value}
    ]

    assert result == expected


def test_dump_df_invalid():
    df1 = pd.DataFrame(data={quantity: [-1], status: ["f"]}, index=[pd.Timestamp("2024-03-03 00:00", tz=TIMEZONE)])
    df1.index.name = startDate
    df1.index = df1.index.tz_convert("UTC")

    with pytest.raises(ValidationError):
        dump_df(df1, TimeSeries)

    df2 = pd.DataFrame(data={quantity: [100], status: ["h"]}, index=[pd.Timestamp("2024-03-03 00:00", tz=TIMEZONE)])
    df2.index.name = startDate
    df2.index = df2.index.tz_convert("UTC")

    with pytest.raises(ValidationError):
        dump_df(df2, TimeSeries)

    df3 = pd.DataFrame(data={quantity: [100], status: ["w"]}, index=["2024-03-03 00:00"])
    df3.index.name = startDate

    with pytest.raises(ValidationError):
        dump_df(df3, TimeSeries)


def test_production_to_json_bytes_valid():
    df = pd.DataFrame(
        data={quantity: [100, 0], status: ["w", "f"]},
        index=[pd.Timestamp("2024-03-03 00:00", tz=TIMEZONE), pd.Timestamp("2024-03-03 00:05", tz=TIMEZONE)])
    df.index.name = startDate
    df.index = df.index.tz_convert("UTC")

    json_bytes_io = production_to_json_bytes(df)
    assert isinstance(json_bytes_io, io.BytesIO)

    json_dict = json.loads(json_bytes_io.getvalue().decode("utf-8"))
    class_json = JsonDataCEZ(**json_dict)
    assert class_json.production[0].quantity == 100
