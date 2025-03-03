import io
import logging
from enum import Enum
from typing import List

import pandas as pd
from pydantic import BaseModel, Field, ConfigDict

from lib import INTERVAL

log = logging.getLogger(__name__)

# df column names - must match Field of pydantic model below
startDate = "startDate"
quantity = "quantity"
status = "status"

unitType = "unitType"
intervalMinutes = "intervalMinutes"
production = "production"


# requested json model: https://megujulo-cez.hu/files/json-data-structure.json


class DataValidity(Enum):
    w = 'w'
    f = 'f'


class TimeSeries(BaseModel):
    model_config = ConfigDict(
        use_enum_values=True,
        json_encoders={pd.Timestamp: lambda v: v.isoformat(timespec='microseconds').replace("+00:00", "Z")},
        arbitrary_types_allowed=True,
        extra='forbid')

    startDate: pd.Timestamp
    quantity: float = Field(ge=0)
    status: DataValidity


class JsonDataCEZ(BaseModel):
    unitType: str = "kWh"
    intervalMinutes: int = INTERVAL
    production: list[TimeSeries]


def dump_df(df: pd.DataFrame, model: BaseModel) -> list:
    """
    Validate dataframe and return it as list of model instances
    """
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index(drop=False)
    data = df.to_dict(orient="records")
    validated_data = [model.model_validate(s).model_dump() for s in data]
    return validated_data


def generate_json_data(production_data: List[TimeSeries]) -> JsonDataCEZ:
    """
    Convert data to required json schema
    """
    json_data = JsonDataCEZ(production=production_data)
    return json_data


def production_to_json_bytes(production_data: pd.DataFrame) -> io.BytesIO:
    """
    Write json to bytes io - prepare for sftp write
    """
    data = dump_df(production_data, TimeSeries)
    json_data = generate_json_data(data)
    json_str = json_data.model_dump_json(indent=4)
    json_bytes_io = io.BytesIO(json_str.encode("utf-8"))
    return json_bytes_io
