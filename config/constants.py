# Nombre de columnas para binance/klines

COLS = {
    0:"open_time",
    1:"open",
    2:"high",
    3:"low",
    4:"close",
    5:"volume",
    6:"close_time",
    7:"quote_asset_volume",
    8:"num_trades",
    9:"tb_base_asset_volume",
    10:"tb_quote_asset_volume",
    11:"ignore"}

CONVERSION_MAPPING = {
    "open":"float32",
    "high":"float32",
    "low":"float32",
    "close":"float32",
    "volume":"float32",
    "quote_asset_volume":"float64",
    "num_trades":"int32",
    "tb_base_asset_volume":"float32",
    "tb_quote_asset_volume":"float64"
    }
