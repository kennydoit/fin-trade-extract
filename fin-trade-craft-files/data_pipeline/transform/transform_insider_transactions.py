#!/usr/bin/env python3#!/usr/bin/env python3#!/usr/bin/env python3#!/usr/bin/env python3

"""Transform Insider Transactions data into analytical features.

"""Transform Insider Transactions data into analytical features.

Creates table ``transformed.insider_transactions_features`` containing insider trading

activity features such as insider buy/sell ratios, executive seniority analysis,"""Transform Insider Transactions data into analytical features."""Transform Insider Transactions data into analytical features.

trading volume patterns, and sentiment indicators.

"""Creates table ``transformed.insider_transactions_features`` containing insider trading



from __future__ import annotationsactivity features such as insider buy/sell ratios, executive seniority analysis,



import systrading volume patterns, and sentiment indicators.

from pathlib import Path

import re"""Creates table ``transformed.insider_transactions_features`` containing insider tradingCreates table ``transformed.insider_transactions_features`` containing insider trading



import numpy as np

import pandas as pd

from __future__ import annotationsactivity features such as insider buy/sell ratios, executive seniority analysis,activity features such as insider buy/sell ratios, executive seniority analysis,

sys.path.append(str(Path(__file__).parent.parent.parent))

from db.postgres_database_manager import PostgresDatabaseManager



import systrading volume patterns, and sentiment indicators.trading volume patterns, and sentiment indicators.

class InsiderTransactionsTransformer:

    """Create analytical features from insider transactions information."""from pathlib import Path



    def __init__(self, universe_id: str | None = None) -> None:import re""""""

        self.db = PostgresDatabaseManager()

        self.universe_id = universe_id

        self.rolling_window = 12  # Monthly aggregation over 12 months

        self.epsilon = 1e-6  # For safe divisionimport numpy as np



    def _fetch_insider_transactions(self) -> pd.DataFrame:import pandas as pd

        """Fetch insider transactions data."""

        if self.universe_id:from __future__ import annotationsfrom __future__ import annotations

            query = """

                SELECTsys.path.append(str(Path(__file__).parent.parent.parent))

                    it.symbol_id,

                    it.symbol,from db.postgres_database_manager import PostgresDatabaseManager

                    it.transaction_date,

                    it.executive,

                    it.executive_title,

                    it.security_type,import sysimport sys

                    it.acquisition_or_disposal,

                    it.shares,class InsiderTransactionsTransformer:

                    it.share_price,

                    it.api_response_status    """Create analytical features from insider transactions information."""from pathlib import Pathfrom pathlib import Path

                FROM source.insider_transactions it

                WHERE it.api_response_status = 'pass'

                AND it.shares IS NOT NULL

                AND it.transaction_date IS NOT NULL    def __init__(self, universe_id: str | None = None) -> None:import reimport re

                AND it.symbol_id IN (

                    SELECT DISTINCT symbol_id         self.db = PostgresDatabaseManager()

                    FROM transformed.symbol_universes 

                    WHERE universe_id = %s        self.universe_id = universe_idfrom typing import Dict, Any

                )

                ORDER BY it.symbol, it.transaction_date        self.rolling_window = 12  # Monthly aggregation over 12 months

            """

            params = (self.universe_id,)        self.epsilon = 1e-6  # For safe divisionimport numpy as np

        else:

            query = """

                SELECT

                    it.symbol_id,    def _fetch_insider_transactions(self) -> pd.DataFrame:import pandas as pdimport numpy as np

                    it.symbol,

                    it.transaction_date,        """Fetch insider transactions data."""

                    it.executive,

                    it.executive_title,        base_query = """import pandas as pd

                    it.security_type,

                    it.acquisition_or_disposal,            SELECT

                    it.shares,

                    it.share_price,                it.symbol_id,sys.path.append(str(Path(__file__).parent.parent.parent))

                    it.api_response_status

                FROM source.insider_transactions it                it.symbol,

                WHERE it.api_response_status = 'pass'

                AND it.shares IS NOT NULL                it.transaction_date,from db.postgres_database_manager import PostgresDatabaseManagersys.path.append(str(Path(__file__).parent.parent.parent))

                AND it.transaction_date IS NOT NULL

                ORDER BY it.symbol, it.transaction_date                it.executive,

            """

            params = None                it.executive_title,from db.postgres_database_manager import PostgresDatabaseManager



        return pd.read_sql_query(query, self.db.connection, params=params)                it.security_type,



    def _get_executive_seniority_tier(self, title: str) -> int:                it.acquisition_or_disposal,

        """Classify executive title into seniority tiers (0-3)."""

        if not title or pd.isna(title):                it.shares,

            return 0

                            it.share_price,class InsiderTransactionsTransformer:

        title_upper = str(title).upper()

                        it.api_response_status

        # Tier 3 (highest): CEO / President / Chair / Chief Executive Officer

        tier3_patterns = [            FROM source.insider_transactions it    """Create analytical features from insider transactions information."""class InsiderTransactionsTransformer:

            r'\bCEO\b', r'CHIEF\s+EXECUTIVE\b', r'\bPRESIDENT\b', 

            r'\bCHAIR\b', r'EXECUTIVE\s+CHAIR'            WHERE it.api_response_status = 'pass'

        ]

                    AND it.shares IS NOT NULL    """Create analytical features from insider transactions information."""

        # Tier 2 (high): CFO / COO / EVP / SVP / Chief Officers

        tier2_patterns = [            AND it.transaction_date IS NOT NULL

            r'\bCFO\b', r'\bCOO\b', r'\bCTO\b', r'\bCMO\b',

            r'CHIEF\s+\w+\s+OFFICER', r'\bEVP\b', r'\bSVP\b'        """    def __init__(self, universe_id: str | None = None) -> None:

        ]

                

        # Tier 1 (moderate): Director / Vice President / Secretary / Treasurer

        tier1_patterns = [        if self.universe_id:        self.db = PostgresDatabaseManager()    def __init__(self, universe_id: str | None = None) -> None:

            r'\bDIRECTOR\b', r'VICE\s+PRESIDENT\b', r'\bVP\b',

            r'\bSECRETARY\b', r'\bTREASURER\b'            query = f"""

        ]

                    {base_query}        self.universe_id = universe_id        self.db = PostgresDatabaseManager()

        for pattern in tier3_patterns:

            if re.search(pattern, title_upper):            AND it.symbol_id IN (

                return 3

                                SELECT DISTINCT symbol_id         self.rolling_window = 12  # Monthly aggregation over 12 months        self.universe_id = universe_id

        for pattern in tier2_patterns:

            if re.search(pattern, title_upper):                FROM transformed.symbol_universes 

                return 2

                                WHERE universe_id = %s        self.epsilon = 1e-6  # For safe division        self.rolling_window = 12  # Monthly aggregation over 12 months

        for pattern in tier1_patterns:

            if re.search(pattern, title_upper):            )

                return 1

                            ORDER BY it.symbol, it.transaction_date        self.epsilon = 1e-6  # For safe division

        return 0

            """

    def _calculate_trading_activity_features(self, df: pd.DataFrame) -> pd.DataFrame:

        """Calculate insider trading activity features."""            params = (self.universe_id,)    def _fetch_insider_transactions(self) -> pd.DataFrame:

        # Convert shares to numeric and handle nulls

        df['shares'] = pd.to_numeric(df['shares'], errors='coerce').fillna(0)        else:

        df['share_price'] = pd.to_numeric(df['share_price'], errors='coerce')

                    query = f"{base_query} ORDER BY it.symbol, it.transaction_date"        """Fetch insider transactions data."""# ------------------------- Patterns & Helpers -------------------------

        # Calculate transaction value

        df['transaction_value'] = df['shares'] * df['share_price'].fillna(df.groupby('symbol')['share_price'].transform('median'))            params = None

        

        # Classify transactions        base_query = """

        df['is_buy'] = df['acquisition_or_disposal'].str.upper().eq('A')  # Acquisition

        df['is_sell'] = df['acquisition_or_disposal'].str.upper().eq('D')  # Disposal        return pd.read_sql_query(query, self.db.connection, params=params)

        

        # Add executive features            SELECT# Tier patterns (order matters for clarity only; tier resolution uses MAX across matches)

        df['executive_tier'] = df['executive_title'].apply(self._get_executive_seniority_tier)

        df['is_10pct_owner'] = df['executive_title'].str.contains('10%', case=False, na=False)    def _get_executive_seniority_tier(self, title: str) -> int:

        

        # Group by symbol and transaction_date        """Classify executive title into seniority tiers (0-3)."""                it.symbol_id,TIER3_PATTERNS = [

        features = df.groupby(['symbol_id', 'symbol', 'transaction_date']).agg({

            'shares': ['count', 'sum', 'mean'],        if not title or pd.isna(title):

            'transaction_value': ['sum', 'mean'],

            'is_buy': ['sum', 'mean'],            return 0                it.symbol,    r"\bCEO\b",

            'is_sell': ['sum', 'mean'],

            'executive_tier': ['max', 'mean'],            

            'is_10pct_owner': ['sum', 'mean'],

        }).reset_index()        title_upper = str(title).upper()                it.transaction_date,    r"Chief\s+Executive\b",

        

        # Flatten column names        

        features.columns = [

            'symbol_id', 'symbol', 'transaction_date',        # Tier 3 (highest): CEO / President / Chair / Chief Executive Officer                it.executive,    r"\bPresident\b",

            'fit_transaction_count', 'fit_total_shares', 'fit_avg_shares_per_transaction',

            'fit_total_transaction_value', 'fit_avg_transaction_value',        tier3_patterns = [

            'fit_buy_count', 'fit_buy_ratio',

            'fit_sell_count', 'fit_sell_ratio',            r'\bCEO\b', r'CHIEF\s+EXECUTIVE\b', r'\bPRESIDENT\b',                 it.executive_title,    r"\bChair\b",

            'fit_max_executive_tier', 'fit_avg_executive_tier',

            'fit_owner_transaction_count', 'fit_owner_ratio'            r'\bCHAIR\b', r'EXECUTIVE\s+CHAIR'

        ]

                ]                it.security_type,    r"Executive\s+Chair"

        return features

        

    def run(self) -> None:

        """Execute the insider transactions transformation pipeline."""        # Tier 2 (high): CFO / COO / EVP / SVP / Chief Officers                it.acquisition_or_disposal,]

        try:

            print("🔍 Starting insider transactions transformation...")        tier2_patterns = [

            self.db.connect()

            r'\bCFO\b', r'\bCOO\b', r'\bCTO\b', r'\bCMO\b',                it.shares,

            # Create features table

            create_table_sql = """            r'CHIEF\s+\w+\s+OFFICER', r'\bEVP\b', r'\bSVP\b'

            CREATE TABLE IF NOT EXISTS transformed.insider_transactions_features (

                symbol_id BIGINT,        ]                it.share_price,TIER2_PATTERNS = [

                symbol VARCHAR(10),

                transaction_date DATE,        

                

                -- Basic trading activity        # Tier 1 (moderate): Director / Vice President / Secretary / Treasurer                it.api_response_status    r"\bCFO\b",

                fit_transaction_count INTEGER,

                fit_total_shares DECIMAL(20,4),        tier1_patterns = [

                fit_avg_shares_per_transaction DECIMAL(20,4),

                fit_total_transaction_value DECIMAL(20,4),            r'\bDIRECTOR\b', r'VICE\s+PRESIDENT\b', r'\bVP\b',            FROM source.insider_transactions it    r"\bCOO\b",

                fit_avg_transaction_value DECIMAL(20,4),

                            r'\bSECRETARY\b', r'\bTREASURER\b'

                -- Buy/Sell activity

                fit_buy_count INTEGER,        ]            WHERE it.api_response_status = 'pass'    r"\bEVP\b",

                fit_buy_ratio DECIMAL(10,4),

                fit_sell_count INTEGER,        

                fit_sell_ratio DECIMAL(10,4),

                        for pattern in tier3_patterns:            AND it.shares IS NOT NULL    r"\bSVP\b",

                -- Executive analysis

                fit_max_executive_tier INTEGER,            if re.search(pattern, title_upper):

                fit_avg_executive_tier DECIMAL(10,4),

                fit_owner_transaction_count INTEGER,                return 3            AND it.transaction_date IS NOT NULL    r"Chief\s+\w+\s+Officer",   # any Chief <X> Officer (CIO, CMO, CHRO, etc.)

                fit_owner_ratio DECIMAL(10,4),

                                

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,        for pattern in tier2_patterns:        """    r"Executive\s+Vice\s+President",

                

                PRIMARY KEY (symbol_id, transaction_date)            if re.search(pattern, title_upper):

            );

            """                return 2            r"Senior\s+Vice\s+President"

            

            self.db.execute_query(create_table_sql)                



            # Clear existing data for this universe if specified        for pattern in tier1_patterns:        if self.universe_id:]

            if self.universe_id:

                delete_sql = """            if re.search(pattern, title_upper):

                DELETE FROM transformed.insider_transactions_features 

                WHERE symbol_id IN (                return 1            query = f"""

                    SELECT DISTINCT symbol_id 

                    FROM transformed.symbol_universes                 

                    WHERE universe_id = %s

                )        return 0            {base_query}TIER1_PATTERNS = [

                """

                self.db.execute_query(delete_sql, (self.universe_id,))



            # Fetch raw data    def _is_10pct_owner(self, title: str) -> bool:            AND it.symbol_id IN (    r"\bDirector\b",

            print("📊 Fetching insider transactions data...")

            raw_data = self._fetch_insider_transactions()        """Check if title indicates 10% owner."""

            

            if raw_data.empty:        if not title or pd.isna(title):                SELECT DISTINCT symbol_id     r"Vice\s+President\b",      # non-exec VPs (if 'Executive Vice President' hit earlier → tier 2)

                print("⚠️  No insider transactions data found for the specified universe.")

                return            return False



            print(f"✅ Loaded {len(raw_data):,} insider transaction records")        return '10%' in str(title).upper() or 'OWNER' in str(title).upper()                FROM transformed.symbol_universes     r"\bSecretary\b",



            # Calculate basic trading activity features

            print("📈 Calculating trading activity features...")

            activity_features = self._calculate_trading_activity_features(raw_data)    def _calculate_trading_activity_features(self, df: pd.DataFrame) -> pd.DataFrame:                WHERE universe_id = %s    r"\bTreasurer\b",



            if activity_features.empty:        """Calculate insider trading activity features."""

                print("⚠️  No features calculated from insider transactions data.")

                return        # Convert shares to numeric and handle nulls            )    r"Assistant\s+Secretary",



            # Insert into database        df['shares'] = pd.to_numeric(df['shares'], errors='coerce').fillna(0)

            print("💾 Inserting features into database...")

                    df['share_price'] = pd.to_numeric(df['share_price'], errors='coerce')            ORDER BY it.symbol, it.transaction_date    r"Associate\s+VP"

            # Prepare data for insertion

            feature_columns = [col for col in activity_features.columns         

                             if col not in ['created_at', 'updated_at']]

                    # Calculate transaction value            """]

            insert_data = activity_features[feature_columns].replace([np.inf, -np.inf], np.nan)

                    df['transaction_value'] = df['shares'] * df['share_price'].fillna(df.groupby('symbol')['share_price'].transform('median'))

            # Insert in chunks

            chunk_size = 1000                    params = (self.universe_id,)

            total_inserted = 0

                    # Classify transactions

            for i in range(0, len(insert_data), chunk_size):

                chunk = insert_data.iloc[i:i + chunk_size]        df['is_buy'] = df['acquisition_or_disposal'].str.upper().eq('A')  # Acquisition        else:OWNER_PATTERNS = [

                

                # Create insert statement        df['is_sell'] = df['acquisition_or_disposal'].str.upper().eq('D')  # Disposal

                columns_str = ', '.join(feature_columns)

                placeholders = ', '.join(['%s'] * len(feature_columns))                    query = f"{base_query} ORDER BY it.symbol, it.transaction_date"    r"10%\s*Owner",

                insert_sql = f"""

                INSERT INTO transformed.insider_transactions_features ({columns_str})        # Add executive features

                VALUES ({placeholders})

                ON CONFLICT (symbol_id, transaction_date) DO UPDATE SET        df['executive_tier'] = df['executive_title'].apply(self._get_executive_seniority_tier)            params = None    r"Ten\s*Percent\s*Owner"

                    fit_transaction_count = EXCLUDED.fit_transaction_count,

                    fit_total_shares = EXCLUDED.fit_total_shares,        df['is_10pct_owner'] = df['executive_title'].apply(self._is_10pct_owner)

                    fit_avg_shares_per_transaction = EXCLUDED.fit_avg_shares_per_transaction,

                    fit_total_transaction_value = EXCLUDED.fit_total_transaction_value,        ]

                    fit_avg_transaction_value = EXCLUDED.fit_avg_transaction_value,

                    fit_buy_count = EXCLUDED.fit_buy_count,        # Group by symbol and transaction_date

                    fit_buy_ratio = EXCLUDED.fit_buy_ratio,

                    fit_sell_count = EXCLUDED.fit_sell_count,        features = df.groupby(['symbol_id', 'symbol', 'transaction_date']).agg({        return pd.read_sql_query(query, self.db.connection, params=params)

                    fit_sell_ratio = EXCLUDED.fit_sell_ratio,

                    fit_max_executive_tier = EXCLUDED.fit_max_executive_tier,            'shares': ['count', 'sum', 'mean'],

                    fit_avg_executive_tier = EXCLUDED.fit_avg_executive_tier,

                    fit_owner_transaction_count = EXCLUDED.fit_owner_transaction_count,            'transaction_value': ['sum', 'mean'],# For canonical role list (clean labels for modeling)

                    fit_owner_ratio = EXCLUDED.fit_owner_ratio,

                    updated_at = CURRENT_TIMESTAMP            'is_buy': ['sum', 'mean'],

                """

                            'is_sell': ['sum', 'mean'],    def _get_executive_seniority_tier(self, title: str) -> int:ROLE_LABELS = [

                # Convert chunk to list of tuples

                values = [tuple(row) for row in chunk.to_numpy()]            'executive_tier': ['max', 'mean'],

                

                # Execute batch insert            'is_10pct_owner': ['sum', 'mean'],        """Classify executive title into seniority tiers (0-3)."""    ("CEO",           [r"\bCEO\b", r"Chief\s+Executive\b"]),

                cursor = self.db.connection.cursor()

                cursor.executemany(insert_sql, values)        }).reset_index()

                self.db.connection.commit()

                                if not title or pd.isna(title):    ("President",     [r"\bPresident\b"]),

                total_inserted += len(chunk)

                if i % (chunk_size * 10) == 0:        # Flatten column names

                    print(f"  📝 Inserted {total_inserted:,} / {len(insert_data):,} records...")

        features.columns = [            return 0    ("Chair",         [r"\bChair\b", r"Executive\s+Chair"]),

            print(f"✅ Successfully inserted {total_inserted:,} insider transaction features")

            print("🎉 Insider transactions transformation completed!")            'symbol_id', 'symbol', 'transaction_date',



        except Exception as e:            'fit_transaction_count', 'fit_total_shares', 'fit_avg_shares_per_transaction',                ("CFO",           [r"\bCFO\b", r"Chief\s+Financial\b"]),

            print(f"❌ Error in insider transactions transformation: {e}")

            raise            'fit_total_transaction_value', 'fit_avg_transaction_value',

        finally:

            if hasattr(self, 'db') and self.db.connection:            'fit_buy_count', 'fit_buy_ratio',        title_upper = str(title).upper()    ("COO",           [r"\bCOO\b", r"Chief\s+Operating\b"]),

                self.db.close()

            'fit_sell_count', 'fit_sell_ratio',



def main() -> None:            'fit_max_executive_tier', 'fit_avg_executive_tier',            ("EVP",           [r"\bEVP\b", r"Executive\s+Vice\s+President"]),

    """CLI entry point for testing."""

    import argparse            'fit_owner_transaction_count', 'fit_owner_ratio'

    

    parser = argparse.ArgumentParser(description="Transform insider transactions data")        ]        # Tier 3 (highest): CEO / President / Chair / Chief Executive Officer    ("SVP",           [r"\bSVP\b", r"Senior\s+Vice\s+President"]),

    parser.add_argument("--universe-id", help="Universe ID to filter data")

    args = parser.parse_args()        

    

    transformer = InsiderTransactionsTransformer(universe_id=args.universe_id)        return features        tier3_patterns = [    ("Director",      [r"\bDirector\b"]),

    transformer.run()





if __name__ == "__main__":    def run(self) -> None:            r'\bCEO\b', r'CHIEF\s+EXECUTIVE\b', r'\bPRESIDENT\b',     ("VP",            [r"Vice\s+President\b"]),

    main()
        """Execute the insider transactions transformation pipeline."""

        try:            r'\bCHAIR\b', r'EXECUTIVE\s+CHAIR'    ("Secretary",     [r"\bSecretary\b"]),

            print("🔍 Starting insider transactions transformation...")

            self.db.connect()        ]    ("Treasurer",     [r"\bTreasurer\b"]),



            # Create features table        ]

            create_table_sql = """

            CREATE TABLE IF NOT EXISTS transformed.insider_transactions_features (        # Tier 2 (high): CFO / COO / EVP / SVP / Chief Officers

                symbol_id BIGINT,

                symbol VARCHAR(10),        tier2_patterns = [SEP_PATTERN = re.compile(r"\s*(,|/|&| and )\s*", flags=re.IGNORECASE)

                transaction_date DATE,

                            r'\bCFO\b', r'\bCOO\b', r'\bCTO\b', r'\bCMO\b',

                -- Basic trading activity

                fit_transaction_count INTEGER,            r'CHIEF\s+\w+\s+OFFICER', r'\bEVP\b', r'\bSVP\b'def _regex_any(patterns: list[str], text: str) -> bool:

                fit_total_shares DECIMAL(20,4),

                fit_avg_shares_per_transaction DECIMAL(20,4),        ]    return any(re.search(p, text, flags=re.IGNORECASE) for p in patterns)

                fit_total_transaction_value DECIMAL(20,4),

                fit_avg_transaction_value DECIMAL(20,4),        

                

                -- Buy/Sell activity        # Tier 1 (moderate): Director / Vice President / Secretary / Treasurerdef _find_roles(text: str) -> list[str]:

                fit_buy_count INTEGER,

                fit_buy_ratio DECIMAL(10,4),        tier1_patterns = [    roles: list[str] = []

                fit_sell_count INTEGER,

                fit_sell_ratio DECIMAL(10,4),            r'\bDIRECTOR\b', r'VICE\s+PRESIDENT\b', r'\bVP\b',    for label, patterns in ROLE_LABELS:

                

                -- Executive analysis            r'\bSECRETARY\b', r'\bTREASURER\b'        if _regex_any(patterns, text):

                fit_max_executive_tier INTEGER,

                fit_avg_executive_tier DECIMAL(10,4),        ]            roles.append(label)

                fit_owner_transaction_count INTEGER,

                fit_owner_ratio DECIMAL(10,4),            # Ensure uniqueness and stable order from ROLE_LABELS

                

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,        for pattern in tier3_patterns:    seen = set()

                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                            if re.search(pattern, title_upper):    deduped = []

                PRIMARY KEY (symbol_id, transaction_date)

            );                return 3    for r in roles:

            """

                                    if r not in seen:

            self.db.execute_query(create_table_sql)

        for pattern in tier2_patterns:            seen.add(r)

            # Clear existing data for this universe if specified

            if self.universe_id:            if re.search(pattern, title_upper):            deduped.append(r)

                delete_sql = """

                DELETE FROM transformed.insider_transactions_features                 return 2    return deduped

                WHERE symbol_id IN (

                    SELECT DISTINCT symbol_id                 

                    FROM transformed.symbol_universes 

                    WHERE universe_id = %s        for pattern in tier1_patterns:def _tier_from_text(text: str) -> int:

                )

                """            if re.search(pattern, title_upper):    tier = 0

                self.db.execute_query(delete_sql, (self.universe_id,))

                return 1    if _regex_any(TIER3_PATTERNS, text):

            # Fetch raw data

            print("📊 Fetching insider transactions data...")                        tier = max(tier, 3)

            raw_data = self._fetch_insider_transactions()

                    return 0    if _regex_any(TIER2_PATTERNS, text):

            if raw_data.empty:

                print("⚠️  No insider transactions data found for the specified universe.")        tier = max(tier, 2)

                return

    def _is_10pct_owner(self, title: str) -> bool:    if _regex_any(TIER1_PATTERNS, text):

            print(f"✅ Loaded {len(raw_data):,} insider transaction records")

        """Check if title indicates 10% owner."""        tier = max(tier, 1)

            # Calculate basic trading activity features

            print("📈 Calculating trading activity features...")        if not title or pd.isna(title):    return tier

            activity_features = self._calculate_trading_activity_features(raw_data)

            return False

            # Insert into database

            print("💾 Inserting features into database...")        return '10%' in str(title).upper() or 'OWNER' in str(title).upper()def _owner_flag(text: str) -> int:

            

            # Prepare data for insertion    return 1 if _regex_any(OWNER_PATTERNS, text) else 0

            feature_columns = [col for col in activity_features.columns 

                             if col not in ['created_at', 'updated_at']]    def _calculate_trading_activity_features(self, df: pd.DataFrame) -> pd.DataFrame:

            

            insert_data = activity_features[feature_columns].replace([np.inf, -np.inf], np.nan)        """Calculate insider trading activity features."""def _flags(text: str, roles: list[str], owner: int) -> list[str]:

            

            # Insert in chunks        # Convert shares to numeric and handle nulls    flags = []

            chunk_size = 1000

            total_inserted = 0        df['shares'] = pd.to_numeric(df['shares'], errors='coerce').fillna(0)    if SEP_PATTERN.search(text):

            

            for i in range(0, len(insert_data), chunk_size):        df['share_price'] = pd.to_numeric(df['share_price'], errors='coerce')        flags.append("multiple_roles")

                chunk = insert_data.iloc[i:i + chunk_size]

                            if re.search(r"\d+%|\bpercent\b", text, flags=re.IGNORECASE):

                # Create insert statement

                columns_str = ', '.join(feature_columns)        # Calculate transaction value        flags.append("contains_percent")

                placeholders = ', '.join(['%s'] * len(feature_columns))

                insert_sql = f"""        df['transaction_value'] = df['shares'] * df['share_price'].fillna(df.groupby('symbol')['share_price'].transform('median'))    if owner == 1:

                INSERT INTO transformed.insider_transactions_features ({columns_str})

                VALUES ({placeholders})                flags.append("owner_detected")

                ON CONFLICT (symbol_id, transaction_date) DO UPDATE SET

                    fit_transaction_count = EXCLUDED.fit_transaction_count,        # Classify transactions    if not roles:

                    fit_total_shares = EXCLUDED.fit_total_shares,

                    fit_avg_shares_per_transaction = EXCLUDED.fit_avg_shares_per_transaction,        df['is_buy'] = df['acquisition_or_disposal'].str.upper().eq('A')  # Acquisition        flags.append("ambiguous")

                    fit_total_transaction_value = EXCLUDED.fit_total_transaction_value,

                    fit_avg_transaction_value = EXCLUDED.fit_avg_transaction_value,        df['is_sell'] = df['acquisition_or_disposal'].str.upper().eq('D')  # Disposal    return sorted(set(flags))

                    fit_buy_count = EXCLUDED.fit_buy_count,

                    fit_buy_ratio = EXCLUDED.fit_buy_ratio,        

                    fit_sell_count = EXCLUDED.fit_sell_count,

                    fit_sell_ratio = EXCLUDED.fit_sell_ratio,        # Add executive featuresdef _clean_title(raw: str | None) -> str:

                    fit_max_executive_tier = EXCLUDED.fit_max_executive_tier,

                    fit_avg_executive_tier = EXCLUDED.fit_avg_executive_tier,        df['executive_tier'] = df['executive_title'].apply(self._get_executive_seniority_tier)    if raw is None:

                    fit_owner_transaction_count = EXCLUDED.fit_owner_transaction_count,

                    fit_owner_ratio = EXCLUDED.fit_owner_ratio,        df['is_10pct_owner'] = df['executive_title'].apply(self._is_10pct_owner)        return ""

                    updated_at = CURRENT_TIMESTAMP

                """            t = raw.strip()

                

                # Convert chunk to list of tuples        # Group by symbol and transaction_date    # Fix repeated spaces, normalize commas/ands etc. (light-touch; keep original for output)

                values = [tuple(row) for row in chunk.to_numpy()]

                        features = df.groupby(['symbol_id', 'symbol', 'transaction_date']).agg({    return re.sub(r"\s+", " ", t)

                # Execute batch insert

                cursor = self.db.connection.cursor()            'shares': ['count', 'sum', 'mean'],

                cursor.executemany(insert_sql, values)

                self.db.connection.commit()            'transaction_value': ['sum', 'mean'],# ------------------------- Core Normalization -------------------------

                

                total_inserted += len(chunk)            'is_buy': ['sum', 'mean'],

                if i % (chunk_size * 10) == 0:

                    print(f"  📝 Inserted {total_inserted:,} / {len(insert_data):,} records...")            'is_sell': ['sum', 'mean'],def normalize_title(raw_title: str | None) -> dict[str, Any]:



            print(f"✅ Successfully inserted {total_inserted:,} insider transaction features")            'executive_tier': ['max', 'mean'],    """

            print("🎉 Insider transactions transformation completed!")

            'is_10pct_owner': ['sum', 'mean'],    Returns:

        except Exception as e:

            print(f"❌ Error in insider transactions transformation: {e}")        }).reset_index()      {

            raise

        finally:                "executive_title_raw": <original>,

            if hasattr(self, 'db') and self.db.connection:

                self.db.close()        # Flatten column names        "executive_title_clean": <cleaned>,



        features.columns = [        "standardized_roles": ["CEO","Director",...],

def main() -> None:

    """CLI entry point for testing."""            'symbol_id', 'symbol', 'transaction_date',        "seniority_tier": 0|1|2|3,

    import argparse

                'fit_transaction_count', 'fit_total_shares', 'fit_avg_shares_per_transaction',        "is_owner_10pct": 0|1,

    parser = argparse.ArgumentParser(description="Transform insider transactions data")

    parser.add_argument("--universe-id", help="Universe ID to filter data")            'fit_total_transaction_value', 'fit_avg_transaction_value',        "flags": ["multiple_roles", ...]

    args = parser.parse_args()

                'fit_buy_count', 'fit_buy_ratio',      }

    transformer = InsiderTransactionsTransformer(universe_id=args.universe_id)

    transformer.run()            'fit_sell_count', 'fit_sell_ratio',    """



            'fit_max_executive_tier', 'fit_avg_executive_tier',    original = "" if raw_title is None else str(raw_title)

if __name__ == "__main__":

    main()            'fit_owner_transaction_count', 'fit_owner_ratio'    clean = _clean_title(original)

        ]

            roles = _find_roles(clean)

        return features    tier = _tier_from_text(clean)

    owner = _owner_flag(clean)

    def run(self) -> None:    flgs = _flags(clean, roles, owner)

        """Execute the insider transactions transformation pipeline."""

        try:    return {

            print("🔍 Starting insider transactions transformation...")        "executive_title_raw": original,

            self.db.connect()        "executive_title_clean": clean,

        "standardized_roles": roles,

            # Create features table        "seniority_tier": int(tier),

            create_table_sql = """        "is_owner_10pct": int(owner),

            CREATE TABLE IF NOT EXISTS transformed.insider_transactions_features (        "flags": flgs

                symbol_id BIGINT,    }

                symbol VARCHAR(10),

                transaction_date DATE,# ------------------------- Database I/O -------------------------

                

                -- Basic trading activitydef read_titles_from_database() -> list[dict[str, Any]]:

                fit_transaction_count INTEGER,    """

                fit_total_shares DECIMAL(20,4),    Reads executive_title data from the insider_transactions table.

                fit_avg_shares_per_transaction DECIMAL(20,4),    Creates an aggregate with title and count.

                fit_total_transaction_value DECIMAL(20,4),    Returns list of dicts: [{"title": "...", "count": <int>}]

                fit_avg_transaction_value DECIMAL(20,4),    """

                    db = PostgresDatabaseManager()

                -- Buy/Sell activity    db.connect()

                fit_buy_count INTEGER,

                fit_buy_ratio DECIMAL(10,4),    try:

                fit_sell_count INTEGER,        query = """

                fit_sell_ratio DECIMAL(10,4),            SELECT executive_title, COUNT(*) as count

                            FROM extracted.insider_transactions

                -- Executive analysis            WHERE executive_title IS NOT NULL

                fit_max_executive_tier INTEGER,                AND executive_title != ''

                fit_avg_executive_tier DECIMAL(10,4),            GROUP BY executive_title

                fit_owner_transaction_count INTEGER,            ORDER BY count DESC

                fit_owner_ratio DECIMAL(10,4),        """

                

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,        result = db.fetch_query(query)

                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,        rows = []

                        for row in result:

                PRIMARY KEY (symbol_id, transaction_date)            rows.append({"title": row[0], "count": row[1]})

            );

            """        return rows

                finally:

            self.db.execute_query(create_table_sql)        db.close()



            # Clear existing data for this universe if specifieddef read_titles_csv(path: str, title_col: str, count_col: str | None) -> list[dict[str, Any]]:

            if self.universe_id:    """

                delete_sql = """    Reads a CSV with at least a title column. Count column is optional.

                DELETE FROM transformed.insider_transactions_features     Returns list of dicts: [{"title": "...", "count": <int or None>}]

                WHERE symbol_id IN (    """

                    SELECT DISTINCT symbol_id     rows = []

                    FROM transformed.symbol_universes     with Path(path).open(newline="", encoding="utf-8-sig") as f:

                    WHERE universe_id = %s        reader = csv.DictReader(f)

                )        # Try to auto-detect if not provided

                """        if title_col not in reader.fieldnames:

                self.db.execute_query(delete_sql, (self.universe_id,))            # heuristics

            candidates = [c for c in reader.fieldnames if "title" in c.lower()]

            # Fetch raw data            if not candidates:

            print("📊 Fetching insider transactions data...")                raise ValueError(f"Could not find title column. Available: {reader.fieldnames}")

            raw_data = self._fetch_insider_transactions()            title_col = candidates[0]

            

            if raw_data.empty:        for r in reader:

                print("⚠️  No insider transactions data found for the specified universe.")            title = r.get(title_col, "")

                return            cnt = None

            if count_col:

            print(f"✅ Loaded {len(raw_data):,} insider transaction records")                try:

                    cnt = int(r.get(count_col, "") or 0)

            # Calculate basic trading activity features                except (ValueError, TypeError):

            print("📈 Calculating trading activity features...")                    cnt = None

            activity_features = self._calculate_trading_activity_features(raw_data)            rows.append({"title": title, "count": cnt})

    return rows

            # Insert into database

            print("💾 Inserting features into database...")def write_mapping_csv(path: str, mapped: list[dict[str, Any]]) -> None:

                fields = [

            # Prepare data for insertion        "executive_title_raw",

            feature_columns = [col for col in activity_features.columns         "executive_title_clean",

                             if col not in ['created_at', 'updated_at']]        "standardized_roles_json",

                    "seniority_tier",

            insert_data = activity_features[feature_columns].replace([np.inf, -np.inf], np.nan)        "is_owner_10pct",

                    "flags_json",

            # Insert in chunks        "count"

            chunk_size = 1000    ]

            total_inserted = 0    with Path(path).open("w", newline="", encoding="utf-8") as f:

                    w = csv.DictWriter(f, fieldnames=fields)

            for i in range(0, len(insert_data), chunk_size):        w.writeheader()

                chunk = insert_data.iloc[i:i + chunk_size]        for m in mapped:

                            w.writerow({

                # Create insert statement                "executive_title_raw": m["executive_title_raw"],

                columns_str = ', '.join(feature_columns)                "executive_title_clean": m["executive_title_clean"],

                placeholders = ', '.join(['%s'] * len(feature_columns))                "standardized_roles_json": json.dumps(m["standardized_roles"], ensure_ascii=False),

                insert_sql = f"""                "seniority_tier": m["seniority_tier"],

                INSERT INTO transformed.insider_transactions_features ({columns_str})                "is_owner_10pct": m["is_owner_10pct"],

                VALUES ({placeholders})                "flags_json": json.dumps(m["flags"], ensure_ascii=False),

                ON CONFLICT (symbol_id, transaction_date) DO UPDATE SET                "count": m.get("count")

                    fit_transaction_count = EXCLUDED.fit_transaction_count,            })

                    fit_total_shares = EXCLUDED.fit_total_shares,

                    fit_avg_shares_per_transaction = EXCLUDED.fit_avg_shares_per_transaction,# ------------------------- Public API (reusable) -------------------------

                    fit_total_transaction_value = EXCLUDED.fit_total_transaction_value,

                    fit_avg_transaction_value = EXCLUDED.fit_avg_transaction_value,def map_titles_dataframe(df, title_col: str = "executive_title"):

                    fit_buy_count = EXCLUDED.fit_buy_count,    """

                    fit_buy_ratio = EXCLUDED.fit_buy_ratio,    If you're using pandas elsewhere, you can import this function and do:

                    fit_sell_count = EXCLUDED.fit_sell_count,        df = map_titles_dataframe(df, title_col="executive_title")

                    fit_sell_ratio = EXCLUDED.fit_sell_ratio,    It will add the following columns:

                    fit_max_executive_tier = EXCLUDED.fit_max_executive_tier,        - executive_title_clean

                    fit_avg_executive_tier = EXCLUDED.fit_avg_executive_tier,        - standardized_roles (list)

                    fit_owner_transaction_count = EXCLUDED.fit_owner_transaction_count,        - seniority_tier (int)

                    fit_owner_ratio = EXCLUDED.fit_owner_ratio,        - is_owner_10pct (int)

                    updated_at = CURRENT_TIMESTAMP        - title_flags (list)

                """    """

                    def _apply(title):

                # Convert chunk to list of tuples        out = normalize_title(title)

                values = [tuple(row) for row in chunk.to_numpy()]        return pd.Series({

                            "executive_title_clean": out["executive_title_clean"],

                # Execute batch insert            "standardized_roles": out["standardized_roles"],

                cursor = self.db.connection.cursor()            "seniority_tier": out["seniority_tier"],

                cursor.executemany(insert_sql, values)            "is_owner_10pct": out["is_owner_10pct"],

                self.db.connection.commit()            "title_flags": out["flags"]

                        })

                total_inserted += len(chunk)    return df.join(df[title_col].apply(_apply))

                if i % (chunk_size * 10) == 0:

                    print(f"  📝 Inserted {total_inserted:,} / {len(insert_data):,} records...")# ------------------------- Report Generation -------------------------



            print(f"✅ Successfully inserted {total_inserted:,} insider transaction features")def generate_tier_report(output_path: str) -> None:

            print("🎉 Insider transactions transformation completed!")    """

    Generate CSV report with executive_title, tier_1_count, tier_2_count, tier_3_count, owner_count

        except Exception as e:    """

            print(f"❌ Error in insider transactions transformation: {e}")    # Read data from database

            raise    titles_data = read_titles_from_database()

        finally:

            if hasattr(self, 'db') and self.db.connection:    report_data = []

                self.db.close()

    for title_info in titles_data:

        title = title_info["title"]

def main() -> None:        count = title_info["count"]

    """CLI entry point for testing."""

    import argparse        # Normalize the title to get tier and owner info

            normalized = normalize_title(title)

    parser = argparse.ArgumentParser(description="Transform insider transactions data")        tier = normalized["seniority_tier"]

    parser.add_argument("--universe-id", help="Universe ID to filter data")        is_owner = normalized["is_owner_10pct"]

    args = parser.parse_args()

            # Create report row

    transformer = InsiderTransactionsTransformer(universe_id=args.universe_id)        row = {

    transformer.run()            "executive_title": title,

            "tier_1_count": count if tier == 1 else 0,

            "tier_2_count": count if tier == 2 else 0,

if __name__ == "__main__":            "tier_3_count": count if tier == 3 else 0,

    main()            "owner_count": count if is_owner == 1 else 0
        }
        report_data.append(row)

    # Write CSV report
    with Path(output_path).open('w', newline='', encoding='utf-8') as f:
        fieldnames = ['executive_title', 'tier_1_count', 'tier_2_count', 'tier_3_count', 'owner_count']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_data)


    # Print summary statistics
    sum(row['tier_1_count'] for row in report_data)
    sum(row['tier_2_count'] for row in report_data)
    sum(row['tier_3_count'] for row in report_data)
    sum(row['owner_count'] for row in report_data)


# ------------------------- CLI -------------------------

def main():
    ap = argparse.ArgumentParser(description="Generate insider executive title tier report.")
    ap.add_argument("--output", default="./executive_title_tier_report.csv", help="Path to write tier report CSV (default: ./executive_title_tier_report.csv).")
    args = ap.parse_args()

    generate_tier_report(args.output)

if __name__ == "__main__":
    main()
