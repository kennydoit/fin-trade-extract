"""
Time Series Daily Adjusted Transformer

This module transforms raw OHLCV time series data into comprehensive ML-ready features
for swing trading applications. It creates technical indicators with OHLCV_ prefixes
and forward-looking target variables with TARGET_ prefixes.

Features Generated:
- Trend: SMA, EMA, crossovers, ratios
- Momentum: RSI, MACD, ROC, Williams %R  
- Volatility: ATR, Bollinger Bands
- Volume: OBV, CMF, AD, volume ratios
- Targets: Returns, directions, classifications for multiple horizons

Author: Financial Trading Craft
Date: September 2025
"""

import logging
import sys
import os
from pathlib import Path
import numpy as np
import pandas as pd
import pandas_ta as ta
from sqlalchemy import create_engine
from dotenv import load_dotenv
import yaml

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from db.postgres_database_manager import PostgresDatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


class TimeSeriesDailyAdjustedTransformer:
    """
    Transforms OHLCV time series data into comprehensive ML features for swing trading.
    
    Creates technical indicators with OHLCV_ prefix and target variables with TARGET_ prefix
    following swing trading best practices for multiple time horizons.
    """
    
    def __init__(self, universe_id=None, config_path=None):
        """
        Initialize the transformer.
        
        Args:
            universe_id (str, optional): Specific universe to process
            config_path (str, optional): Path to configuration file
        """
        self.db = PostgresDatabaseManager()
        self.universe_id = universe_id
        
        # Database connection setup
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DATABASE', 'fin_trade_craft')
        
        db_url = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        self.engine = create_engine(db_url)
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Feature generation parameters (optimized for swing trading)
        self.rolling_window = self.config.get('rolling_window', 8)
        self.ma_periods = self.config.get('ma_periods', [5, 10, 20, 50])
        self.ema_periods = self.config.get('ema_periods', [8, 21, 34, 55])
        self.rsi_periods = self.config.get('rsi_periods', [7, 14])
        self.atr_periods = self.config.get('atr_periods', [10, 14])
        self.target_horizons = self.config.get('target_horizons', [5, 10, 20, 30, 40])
        
    def _load_config(self, config_path):
        """Load configuration from YAML file."""
        if config_path and Path(config_path).exists():
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        
        # Try default config locations
        default_configs = [
            Path(project_root) / 'features' / 'config.yaml',
            Path(project_root) / 'config.yaml'
        ]
        
        for config_file in default_configs:
            if config_file.exists():
                with open(config_file, 'r') as f:
                    config = yaml.safe_load(f)
                    logger.info(f"Loaded configuration from {config_file}")
                    return config
        
        logger.warning("No configuration file found, using defaults")
        return {}
    
    def safe_divide(self, a, b, fillvalue=0.0):
        """
        Safely divide two series, handling division by zero and NaN values.
        
        Args:
            a (pd.Series): Numerator
            b (pd.Series): Denominator
            fillvalue (float): Value to use for division by zero
            
        Returns:
            pd.Series: Result of division with safe handling
        """
        a_clean = a.fillna(0)
        b_clean = b.fillna(1)
        with np.errstate(divide='ignore', invalid='ignore'):
            result = np.divide(
                a_clean, b_clean, 
                out=np.full_like(a_clean, fillvalue, dtype=float), 
                where=(b_clean != 0)
            )
        return pd.Series(result, index=a.index)

    def create_trend_features(self, df):
        """
        Create trend-based technical indicators.
        
        Features:
        - Simple Moving Averages (SMA) and ratios
        - Exponential Moving Averages (EMA) and ratios  
        - EMA crossover signals
        
        Args:
            df (pd.DataFrame): Input dataframe with OHLCV data
            
        Returns:
            pd.DataFrame: DataFrame with added trend features
        """
        logger.info('Creating trend features...')
        
        for symbol in df['symbol'].unique():
            mask = df['symbol'] == symbol
            symbol_data = df.loc[mask].copy().sort_values('date')
            
            # Simple Moving Averages
            for period in self.ma_periods:
                sma = symbol_data['close'].rolling(period).mean()
                df.loc[mask, f'ohlcv_sma_{period}'] = sma
                df.loc[mask, f'ohlcv_sma_{period}_ratio'] = self.safe_divide(
                    symbol_data['close'], sma
                )
            
            # Exponential Moving Averages
            for period in self.ema_periods:
                ema = symbol_data['close'].ewm(span=period).mean()
                df.loc[mask, f'ohlcv_ema_{period}'] = ema
                df.loc[mask, f'ohlcv_ema_{period}_ratio'] = self.safe_divide(
                    symbol_data['close'], ema
                )
            
            # EMA crossovers (8/21 popular for swing trading)
            ema8 = symbol_data['close'].ewm(span=8).mean()
            ema21 = symbol_data['close'].ewm(span=21).mean()
            df.loc[mask, 'ohlcv_ema_8_21_cross'] = (ema8 > ema21).astype(int)
            df.loc[mask, 'ohlcv_ema_8_21_ratio'] = self.safe_divide(ema8, ema21)
            
        return df

    def create_momentum_features(self, df):
        """
        Create momentum-based technical indicators.
        
        Features:
        - Relative Strength Index (RSI) with overbought/oversold signals
        - MACD with signal line and histogram
        - Rate of Change (ROC)
        - Williams %R
        
        Args:
            df (pd.DataFrame): Input dataframe with OHLCV data
            
        Returns:
            pd.DataFrame: DataFrame with added momentum features
        """
        logger.info('Creating momentum features...')
        
        for symbol in df['symbol'].unique():
            mask = df['symbol'] == symbol
            symbol_data = df.loc[mask].copy().sort_values('date')
            
            # RSI with overbought/oversold signals
            for period in self.rsi_periods:
                rsi_values = ta.rsi(symbol_data['close'], length=period)
                df.loc[mask, f'ohlcv_rsi_{period}'] = rsi_values
                df.loc[mask, f'ohlcv_rsi_{period}_oversold'] = (rsi_values < 30).astype(int)
                df.loc[mask, f'ohlcv_rsi_{period}_overbought'] = (rsi_values > 70).astype(int)
            
            # MACD (12,26,9 - standard parameters)
            try:
                macd = ta.macd(symbol_data['close'], fast=12, slow=26, signal=9)
                if not macd.empty:
                    df.loc[mask, 'ohlcv_macd'] = macd['MACD_12_26_9']
                    df.loc[mask, 'ohlcv_macd_signal'] = macd['MACDs_12_26_9']
                    df.loc[mask, 'ohlcv_macd_histogram'] = macd['MACDh_12_26_9']
                    df.loc[mask, 'ohlcv_macd_bullish'] = (
                        macd['MACD_12_26_9'] > macd['MACDs_12_26_9']
                    ).astype(int)
            except Exception as e:
                logger.warning(f"MACD calculation failed for {symbol}: {e}")
            
            # Rate of Change
            df.loc[mask, 'ohlcv_roc_10'] = ta.roc(symbol_data['close'], length=10)
            df.loc[mask, 'ohlcv_roc_20'] = ta.roc(symbol_data['close'], length=20)
            
            # Williams %R
            df.loc[mask, 'ohlcv_willr_14'] = ta.willr(
                symbol_data['high'], symbol_data['low'], 
                symbol_data['close'], length=14
            )
            
        return df

    def create_volatility_features(self, df):
        """
        Create volatility-based technical indicators.
        
        Features:
        - Average True Range (ATR) and percentage
        - Bollinger Bands with position and width
        
        Args:
            df (pd.DataFrame): Input dataframe with OHLCV data
            
        Returns:
            pd.DataFrame: DataFrame with added volatility features
        """
        logger.info('Creating volatility features...')
        
        for symbol in df['symbol'].unique():
            mask = df['symbol'] == symbol
            symbol_data = df.loc[mask].copy().sort_values('date')
            
            # Average True Range
            for period in self.atr_periods:
                try:
                    atr = ta.atr(
                        symbol_data['high'], symbol_data['low'], 
                        symbol_data['close'], length=period
                    )
                    df.loc[mask, f'ohlcv_atr_{period}'] = atr
                    df.loc[mask, f'ohlcv_atr_{period}_pct'] = self.safe_divide(
                        atr, symbol_data['close']
                    ) * 100
                except Exception as e:
                    logger.warning(f"ATR calculation failed for {symbol}: {e}")
                    
            # Bollinger Bands (20,2 - standard parameters)
            try:
                bb = ta.bbands(symbol_data['close'], length=20, std=2)
                if not bb.empty:
                    df.loc[mask, 'ohlcv_bb_upper'] = bb['BBU_20_2.0']
                    df.loc[mask, 'ohlcv_bb_middle'] = bb['BBM_20_2.0']
                    df.loc[mask, 'ohlcv_bb_lower'] = bb['BBL_20_2.0']
                    df.loc[mask, 'ohlcv_bb_width'] = (
                        bb['BBU_20_2.0'] - bb['BBL_20_2.0']
                    ) / bb['BBM_20_2.0']
                    df.loc[mask, 'ohlcv_bb_position'] = (
                        symbol_data['close'] - bb['BBL_20_2.0']
                    ) / (bb['BBU_20_2.0'] - bb['BBL_20_2.0'])
            except Exception as e:
                logger.warning(f"Bollinger Bands calculation failed for {symbol}: {e}")
                
        return df

    def create_volume_features(self, df):
        """
        Create volume-based technical indicators.
        
        Features:
        - On-Balance Volume (OBV)
        - Chaikin Money Flow (CMF)
        - Accumulation/Distribution Line (AD)
        - Volume moving averages and ratios
        
        Args:
            df (pd.DataFrame): Input dataframe with OHLCV data
            
        Returns:
            pd.DataFrame: DataFrame with added volume features
        """
        logger.info('Creating volume features...')
        
        for symbol in df['symbol'].unique():
            mask = df['symbol'] == symbol
            symbol_data = df.loc[mask].copy().sort_values('date')
            
            # On-Balance Volume
            df.loc[mask, 'ohlcv_obv'] = ta.obv(symbol_data['close'], symbol_data['volume'])
            
            # Chaikin Money Flow
            df.loc[mask, 'ohlcv_cmf'] = ta.cmf(
                symbol_data['high'], symbol_data['low'], 
                symbol_data['close'], symbol_data['volume'], length=20
            )
            
            # Accumulation/Distribution Line
            df.loc[mask, 'ohlcv_ad'] = ta.ad(
                symbol_data['high'], symbol_data['low'], 
                symbol_data['close'], symbol_data['volume']
            )
            
            # Volume Moving Averages
            df.loc[mask, 'ohlcv_volume_sma_20'] = symbol_data['volume'].rolling(20).mean()
            df.loc[mask, 'ohlcv_volume_sma_50'] = symbol_data['volume'].rolling(50).mean()
            df.loc[mask, 'ohlcv_volume_ratio'] = self.safe_divide(
                symbol_data['volume'], 
                symbol_data['volume'].rolling(20).mean()
            )
            
        return df

    def create_target_variables(self, df):
        """
        Create forward-looking target variables for multiple time horizons.
        
        Features:
        - Percentage returns
        - Log returns
        - Binary direction (up/down)
        - Ternary classification (Up >2%, Flat ±2%, Down <-2%)
        
        Args:
            df (pd.DataFrame): Input dataframe with OHLCV data
            
        Returns:
            pd.DataFrame: DataFrame with added target variables
        """
        logger.info('Creating target variables...')
        
        for symbol in df['symbol'].unique():
            mask = df['symbol'] == symbol
            symbol_data = df.loc[mask].copy().sort_values('date')
            
            # Forward returns for different horizons
            for horizon in self.target_horizons:
                # Percentage returns
                future_close = symbol_data['close'].shift(-horizon)
                pct_return = self.safe_divide(
                    future_close - symbol_data['close'], 
                    symbol_data['close']
                )
                df.loc[mask, f'target_return_{horizon}d'] = pct_return
                
                # Log returns (handle zero/negative prices safely)
                log_return = np.log(self.safe_divide(
                    future_close, symbol_data['close'], fillvalue=1
                ))
                df.loc[mask, f'target_log_return_{horizon}d'] = log_return
                
                # Directional targets (binary classification)
                df.loc[mask, f'target_direction_{horizon}d'] = (pct_return > 0).astype(int)
                
                # Ternary classification (Down <-2%, Flat ±2%, Up >2%)
                df.loc[mask, f'target_ternary_{horizon}d'] = pd.cut(
                    pct_return, 
                    bins=[-np.inf, -0.02, 0.02, np.inf], 
                    labels=[0, 1, 2]  # Down, Flat, Up
                )
                
        return df

    def get_symbol_universe(self, universe_id):
        """
        Get symbols for a specific universe from the database.
        
        Args:
            universe_id (str): Universe identifier
            
        Returns:
            list: List of symbol IDs
        """
        if not universe_id:
            return None
            
        try:
            self.db.connect()
            query = """
                SELECT DISTINCT symbol_id 
                FROM transformed.symbol_universes 
                WHERE universe_id = %s
            """
            result = self.db.fetch_all(query, (universe_id,))
            symbol_ids = [row[0] for row in result]
            logger.info(f"Found {len(symbol_ids)} symbols for universe {universe_id}")
            return symbol_ids
        except Exception as e:
            logger.error(f"Error fetching symbol universe: {e}")
            return None
        finally:
            self.db.close()

    def process(self, universe_id=None, limit_symbols=None):
        """
        Main processing method to transform time series data.
        
        Args:
            universe_id (str, optional): Specific universe to process
            limit_symbols (int, optional): Limit number of symbols for testing
            
        Returns:
            bool: Success status
        """
        try:
            self.db.connect()
            universe_filter = universe_id or self.universe_id
            
            logger.info(f"TimeSeriesDailyAdjustedTransformer: Processing universe_id = {universe_filter}")
            
            # Base query for time series data
            base_query = """
                SELECT ts.symbol_id, ts.symbol, ts.date, ts.open, ts.high, ts.low, 
                       ts.close, ts.adjusted_close, ts.volume
                FROM source.time_series_daily_adjusted ts
            """
            
            # Apply universe filter or symbol limit
            if universe_filter:
                query = base_query + f"""
                    WHERE ts.symbol_id IN (
                        SELECT DISTINCT symbol_id 
                        FROM transformed.symbol_universes 
                        WHERE universe_id = '{universe_filter}'
                    )
                    AND ts.api_response_status = 'pass'
                    ORDER BY ts.symbol, ts.date
                """
            elif limit_symbols:
                query = base_query + f"""
                    WHERE ts.api_response_status = 'pass'
                    AND ts.symbol IN (
                        SELECT DISTINCT symbol 
                        FROM source.time_series_daily_adjusted 
                        LIMIT {limit_symbols}
                    )
                    ORDER BY ts.symbol, ts.date
                """
            else:
                # Process all symbols with good data
                query = base_query + """
                    WHERE ts.api_response_status = 'pass'
                    ORDER BY ts.symbol, ts.date
                """
            
            # Fetch data
            df = self.db.fetch_dataframe(query)
            symbols_count = df['symbol'].nunique() if not df.empty else 0
            logger.info(f"Fetched {len(df)} time series records for {symbols_count} symbols")
            
            if df.empty:
                logger.warning('No time series data found')
                return False
            
            # Ensure date column is datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Create comprehensive features
            df = self.create_trend_features(df)
            df = self.create_momentum_features(df)
            df = self.create_volatility_features(df)
            df = self.create_volume_features(df)
            df = self.create_target_variables(df)
            
            # Prepare final dataframe
            feature_columns = [col for col in df.columns if col.startswith(('ohlcv_', 'target_'))]
            final_df = df[['symbol_id', 'symbol', 'date'] + feature_columns].copy()
            final_df['created_at'] = pd.Timestamp.now()
            final_df['updated_at'] = pd.Timestamp.now()
            
            # Log feature summary
            ohlcv_features = [col for col in feature_columns if col.startswith('ohlcv_')]
            target_features = [col for col in feature_columns if col.startswith('target_')]
            
            logger.info(f"Generated {len(feature_columns)} total features:")
            logger.info(f"  - {len(ohlcv_features)} OHLCV features")
            logger.info(f"  - {len(target_features)} TARGET features")
            
            symbols_list = final_df['symbol'].unique().tolist()
            logger.info(f"Symbols processed: {symbols_list}")
            
            date_min = final_df['date'].min()
            date_max = final_df['date'].max()
            logger.info(f"Date range: {date_min} to {date_max}")
            
            # Save to database using SQLAlchemy engine
            final_df.to_sql(
                'time_series_features', self.engine, schema='transformed', 
                if_exists='replace', index=False, method='multi', chunksize=1000
            )
            
            logger.info(f"Successfully inserted {len(final_df)} records with {len(feature_columns)} features")
            logger.info("Time series transformation completed successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in time series transformation: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self.db.close()


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Transform time series data to ML features')
    parser.add_argument('--universe-id', type=str, help='Specific universe to process')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--limit', type=int, help='Limit number of symbols (for testing)')
    
    args = parser.parse_args()
    
    # Initialize transformer
    transformer = TimeSeriesDailyAdjustedTransformer(
        universe_id=args.universe_id,
        config_path=args.config
    )
    
    # Execute transformation
    success = transformer.process(
        universe_id=args.universe_id,
        limit_symbols=args.limit
    )
    
    if success:
        print("\n🎉 TIME SERIES TRANSFORMATION COMPLETED SUCCESSFULLY! 🎉")
        print("Generated comprehensive OHLCV features and TARGET variables for swing trading!")
        print("\n✅ Features created with proper prefixes:")
        print("   - OHLCV_ prefix: Technical indicators and price/volume features") 
        print("   - TARGET_ prefix: Forward-looking outcomes for model training")
        print("\n📊 Ready for machine learning model training!")
    else:
        print("\n❌ Time series transformation failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
