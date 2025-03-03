import csv
import io
import logging

import pandas as pd

from lib import TIMEZONE, INTERVAL
from lib.json_writer import DataValidity, startDate, quantity, status

log = logging.getLogger(__name__)


TIMESTAMP_COL = "#Time"
E_DAY_COL = "E-Day"
UTC_TIMESTAMP = "timestamp_utc"
E_INTERVAL = "E-Increment"


def last_interval_date() -> pd.Timestamp:
    """
    Get localized current time and offset it by -INTERVAL
    """
    return (pd.Timestamp.now(tz=TIMEZONE).floor(f"{INTERVAL}min")) - pd.Timedelta(minutes=INTERVAL)


def huawei_datalogger_csv_parser(data: io.StringIO, date: pd.Timestamp) -> pd.DataFrame:
    """
    Parse csv from huawei datalogger with inverter data
    """
    parsed_csv = csv.reader(data, delimiter=';')
    all_inverters = []  # data from all devices
    inverter_data = []
    for row in parsed_csv:
        if any([TIMESTAMP_COL in element for element in row]):  # if strings match table header
            if inverter_data:
                all_inverters.append(inverter_data)  # before creating new list for inv, append previous inv data to all
            inverter_data = [row]  # start new list for inv
        elif not any(["#" in element for element in row]):
            inverter_data.append(row)
    all_inverters.append(inverter_data)  # append last inv data because there wont be additional header

    dfs = []
    for item in all_inverters:
        if item:
            df = pd.DataFrame(item)
            new_header = df.iloc[0]
            df = df[1:]
            df.columns = new_header
            df = df[[TIMESTAMP_COL, E_DAY_COL]].rename(columns={TIMESTAMP_COL: startDate, E_DAY_COL: quantity})
            dfs.append(df)
    print(len(dfs))
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        df[startDate] = pd.to_datetime(df[startDate], yearfirst=True).dt.tz_localize(TIMEZONE, ambiguous="infer")
        df[quantity] = df[quantity].astype(float)
        df = df.groupby(by=startDate, as_index=False).agg({quantity: "sum"}).set_index(startDate)
        df = df.diff().fillna(0).clip(lower=0)
        df[quantity] = df[quantity].round(3)
        df.columns.name = None
        df.index = df.index.tz_convert("UTC")
    else:  # if no data to concat, return first row from replacement data
        df = replacement_data(date=date).drop(columns=[status]).iloc[:1]
    return df


def replacement_data(date: pd.Timestamp) -> pd.DataFrame:
    """
    Generate fake data with 0 values and F status for the whole day until interval date (inclusive) - csv data from
    Huawei smartloggers are not sending values during night but CEZ wants data - these replacement data will be used
    for these periods, also if some intervals of actual production are missing, replacement data will be used as well
    """
    if date.tz is None:
        date = date.tz_localize(TIMEZONE, ambiguous=True)
    start = date.floor('D')
    end = start + pd.Timedelta(days=1)
    date_range = pd.date_range(start, end, freq=f"{INTERVAL}min", tz=TIMEZONE, inclusive="left")
    df = pd.DataFrame({
        startDate: date_range,
        quantity: [0] * len(date_range)
    })
    df = df.set_index(startDate)
    df = df[df.index <= date]
    df.index = df.index.tz_convert("UTC")
    df[status] = DataValidity.f.value
    return df


def handle_missing_intervals(datalogger_df: pd.DataFrame, date: pd.Timestamp) -> pd.DataFrame:
    """
    Merge df with data from datalogger with replacement df to create continuous timeseries - missing intervals in
    datalogger data will be replaced by date (value 0 and status 0) from replacement data
    """
    replacement = replacement_data(date).drop(columns=[quantity])
    df = pd.merge(replacement, datalogger_df, left_index=True, right_index=True, how="left")
    df[status] = [DataValidity.f.value if pd.isna(x) else DataValidity.w.value for x in df[quantity]]
    df[quantity] = df[quantity].fillna(0)
    return df
