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
    Trades: int


@dataclass
class FFD:
    Instrument: int
    Resolution: str
    Fdim: float
    Epoch: datetime
    Open: float
    High: float
    Low: float
    Close: float
    Volume: float
    Trades: int


@dataclass
class EMD:
    Instrument: int
    Resolution: str
    Fdim: float
    Epoch: datetime
    If_0: float
    If_1: float
    If_2: float
    If_3: float
    If_4: float
    If_5: float
    If_6: float
    If_7: float
    If_8: float
    If_9: float
    If_10: float
    If_11: float
    If_12: float
    If_13: float
    If_14: float
    If_15: float
    Ia_0: float
    Ia_1: float
    Ia_2: float
    Ia_3: float
    Ia_4: float
    Ia_5: float
    Ia_6: float
    Ia_7: float
    Ia_8: float
    Ia_9: float
    Ia_10: float
    Ia_11: float
    Ia_12: float
    Ia_13: float
    Ia_14: float
    Ia_15: float
    Ip_0: float
    Ip_1: float
    Ip_2: float
    Ip_3: float
    Ip_4: float
    Ip_5: float
    Ip_6: float
    Ip_7: float
    Ip_8: float
    Ip_9: float
    Ip_10: float
    Ip_11: float
    Ip_12: float
    Ip_13: float
    Ip_14: float
    Ip_15: float


@dataclass
class PremiumIndex:
    SpotInstrument: int
    FuturesInstrument: int
    Resolution: str
    Epoch: datetime
    PremiumIndex: float


@dataclass
class VpinOHLCV:
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
    Trades: float


@dataclass
class Features202406:
    Instrument: int
    VPIN: int
    Epoch: datetime
    Volume: float
    Trades: float
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
