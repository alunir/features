from datetime import datetime
from dataclasses import dataclass


@dataclass
class OHLCV:
    Instrument: int
    Epoch: datetime
    Open: float
    High: float
    Low: float
    Close: float
    Volume: float
    Number: int
