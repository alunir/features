from dataclasses import dataclass


@dataclass
class OHLCV:
    Instrument: int
    Epoch: str
    Open: float
    High: float
    Low: float
    Close: float
    Volume: float
    Number: int
