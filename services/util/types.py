from datetime import datetime
from dataclasses import dataclass


@dataclass
class Data:
    Instrument: int
    Epoch: datetime


@dataclass
class OHLCV(Data):
    Instrument: int
    Epoch: datetime
    Open: float
    High: float
    Low: float
    Close: float
    Volume: float
    Number: int


@dataclass
class VpinOHLCV(Data):
    Instrument: int
    VPIN: int
    Epoch: datetime
    Open: float
    High: float
    Low: float
    Close: float
    Volume: float
    BuyVolume: float
    SellVolume: float
    Number: float


@dataclass
class Features202406(Data):
    Instrument: int
    VPIN: int
    Epoch: datetime
    Volume: float
    Number: float
    imf_imf_0: float
    imf_imf_1: float
    imf_imf_2: float
    imf_imf_3: float
    imf_imf_4: float
    imf_imf_5: float
    imf_imf_6: float
    imf_imf_7: float
    imf_imf_8: float
    imf_ia_0: float
    imf_ia_1: float
    imf_ia_2: float
    imf_ia_3: float
    imf_ia_4: float
    imf_ia_5: float
    imf_ia_6: float
    imf_ia_7: float
    imf_ia_8: float
    imf_ip_0: float
    imf_ip_1: float
    imf_ip_2: float
    imf_ip_3: float
    imf_ip_4: float
    imf_ip_5: float
    imf_ip_6: float
    imf_ip_7: float
    imf_ip_8: float
    open: float
    high: float
    low: float
    close: float
    Ask: float
    Bid: float
    Last: float
