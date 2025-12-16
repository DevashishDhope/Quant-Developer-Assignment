import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller

def calculate_ohlcv(df, interval='1min'):
    """
    Resample tick data to OHLCV bars.
    df: DataFrame with 'timestamp', 'price', 'quantity'
    """
    if df.empty:
        return pd.DataFrame()
        
    df = df.set_index('timestamp')
    ohlcv = df['price'].resample(interval).ohlc()
    ohlcv['volume'] = df['quantity'].resample(interval).sum()
    ohlcv = ohlcv.dropna()
    return ohlcv

def calculate_hedge_ratio(series_y, series_x):
    """
    Calculate Hedge Ratio using OLS (Y = beta * X + alpha).
    Returns beta (hedge ratio).
    """
    if len(series_y) != len(series_x) or len(series_y) < 2:
        return 0, 0
        
    X = sm.add_constant(series_x)
    model = sm.OLS(series_y, X).fit()
    return model.params[1] # beta

def calculate_spread(series_y, series_x, hedge_ratio):
    """
    Calculate Spread = Y - (Hedge Ratio * X)
    """
    return series_y - (hedge_ratio * series_x)

def calculate_zscore(series, window=20):
    """
    Calculate Rolling Z-Score.
    Z = (Value - RollingMean) / RollingStd
    """
    r_mean = series.rolling(window=window).mean()
    r_std = series.rolling(window=window).std()
    z_score = (series - r_mean) / r_std
    return z_score

def calculate_rolling_correlation(series_y, series_x, window=20):
    """
    Calculate Rolling Correlation.
    """
    return series_y.rolling(window=window).corr(series_x)

def perform_adf_test(series):
    """
    Perform Augmented Dickey-Fuller test.
    Returns dict with stats.
    """
    if len(series) < 10:
        return None
        
    result = adfuller(series.dropna())
    return {
        'adf_statistic': result[0],
        'p_value': result[1],
        'critical_values': result[4]
    }
