#!/usr/bin/env python3
"""
Management CLI for incremental ETL system.
Provides easy interface to manage universes, check processing status, and generate processing plans.
"""

import os
import sys
import argparse
import json
from datetime import datetime

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))

from incremental_etl import IncrementalETLManager, DataType
from symbol_screener import SymbolScreener, ScreeningCriteria, ExchangeType, AssetType
from universe_management import UniverseManager


def get_snowflake_config() -> dict:
    """Get Snowflake configuration from environment variables."""
    required_vars = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA', 'SNOWFLAKE_WAREHOUSE'
    ]
    
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    return {
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'user': os.environ['SNOWFLAKE_USER'],
        'password': os.environ['SNOWFLAKE_PASSWORD'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
        'schema': os.environ['SNOWFLAKE_SCHEMA'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE']
    }


def cmd_setup(args):
    """Set up incremental ETL system tables and create default universes."""
    print("🚀 Setting up incremental ETL system...")
    
    config = get_snowflake_config()
    
    # Setup ETL manager tables
    etl_manager = IncrementalETLManager(config)
    etl_manager.create_processing_status_table()
    print("✅ Created processing status table")
    
    # Setup universe tables
    universe_manager = UniverseManager(config)
    universe_manager.create_universe_table()
    print("✅ Created universe management table")
    
    # Create default universes
    universe_manager.create_predefined_universes()
    print("✅ Created default universes")
    
    etl_manager.close_connection()
    universe_manager.close_connection()
    
    print("\n🎯 Incremental ETL system setup completed!")


def cmd_status(args):
    """Show processing status for symbols and universes."""
    config = get_snowflake_config()
    etl_manager = IncrementalETLManager(config)
    
    # Show universe status
    print("📊 Universe Status:")
    print("-" * 60)
    try:
        universe_manager = UniverseManager(config)
        universes = universe_manager.list_universes()
        for univ in universes:
            print(f"{univ['name']:25} | {univ['symbol_count']:6} symbols | {univ['source']:15}")
        universe_manager.close_connection()
    except Exception as e:
        print(f"❌ Could not load universes: {e}")
    
    # Show processing status by data type
    print(f"\n📈 Processing Status:")
    print("-" * 60)
    
    for data_type in [DataType.LISTING_STATUS, DataType.TIME_SERIES_DAILY_ADJUSTED, 
                      DataType.COMPANY_OVERVIEW, DataType.EARNINGS]:
        try:
            status = etl_manager.get_processing_status_summary(data_type.value)
            print(f"{data_type.value:25} | {status.get('total_symbols', 0):6} total | "
                  f"{status.get('processed_symbols', 0):6} processed | "
                  f"{status.get('needs_update', 0):6} need update")
        except Exception as e:
            print(f"{data_type.value:25} | ❌ Error: {e}")
    
    etl_manager.close_connection()


def cmd_plan(args):
    """Generate processing plan for a universe and data type."""
    config = get_snowflake_config()
    etl_manager = IncrementalETLManager(config)
    
    universe_name = args.universe
    data_type_str = args.data_type
    
    try:
        data_type = DataType(data_type_str)
    except ValueError:
        print(f"❌ Invalid data type: {data_type_str}")
        print(f"Valid types: {[dt.value for dt in DataType]}")
        return
    
    print(f"📋 Generating processing plan for {universe_name} - {data_type_str}...")
    
    try:
        plan = etl_manager.get_processing_plan(universe_name, data_type)
        
        print(f"\n🎯 Processing Plan Summary:")
        print(f"Universe: {universe_name}")
        print(f"Data Type: {data_type_str}")
        print(f"Total symbols: {plan.total_symbols}")
        print(f"Need processing: {len(plan.processing_items)}")
        print(f"Already current: {plan.total_symbols - len(plan.processing_items)}")
        
        if plan.processing_items:
            print(f"\n📝 Processing Summary:")
            for reason, count in plan.summary.items():
                print(f"  {reason}: {count}")
            
            print(f"\n🔧 Symbols to process:")
            for item in plan.processing_items[:10]:  # Show first 10
                print(f"  {item['symbol']} - {item['reason']}")
            if len(plan.processing_items) > 10:
                print(f"  ... and {len(plan.processing_items) - 10} more")
        else:
            print("\n✅ All symbols are up to date!")
        
        if args.output:
            plan_data = {
                'universe_name': universe_name,
                'data_type': data_type_str,
                'generated_at': datetime.now().isoformat(),
                'total_symbols': plan.total_symbols,
                'processing_items': plan.processing_items,
                'summary': plan.summary
            }
            with open(args.output, 'w') as f:
                json.dump(plan_data, f, indent=2)
            print(f"\n💾 Plan saved to {args.output}")
    
    except Exception as e:
        print(f"❌ Error generating plan: {e}")
    
    etl_manager.close_connection()


def cmd_universe(args):
    """Manage universes."""
    config = get_snowflake_config()
    universe_manager = UniverseManager(config)
    
    if args.universe_action == 'list':
        universes = universe_manager.list_universes()
        print("\n📊 Available Universes:")
        print("-" * 80)
        for univ in universes:
            print(f"{univ['name']:25} | {univ['symbol_count']:6} symbols | {univ['source']:15} | {univ['description']}")
    
    elif args.universe_action == 'show':
        universe_def = universe_manager.load_universe(args.name)
        print(f"\n🌐 Universe: {universe_def.name}")
        print(f"Description: {universe_def.description}")
        print(f"Symbols: {len(universe_def.symbols)}")
        print(f"Source: {universe_def.source}")
        print(f"Created: {universe_def.created_date}")
        
        if args.symbols:
            print(f"\nSymbols:")
            for symbol in universe_def.symbols[:50]:  # Show first 50
                print(f"  {symbol}")
            if len(universe_def.symbols) > 50:
                print(f"  ... and {len(universe_def.symbols) - 50} more")
    
    elif args.universe_action == 'create':
        universe_manager.create_predefined_universes()
        print("✅ Created predefined universes")
    
    universe_manager.close_connection()


def cmd_screen(args):
    """Screen symbols using symbol screener."""
    config = get_snowflake_config()
    screener = SymbolScreener(config)
    
    criteria = ScreeningCriteria(
        exchanges=[ExchangeType.NASDAQ] if args.exchange == 'NASDAQ' else [],
        min_price=args.min_price,
        min_avg_volume=args.min_volume,
        exclude_penny_stocks=args.exclude_penny,
        min_data_quality_score=args.min_quality
    )
    
    print(f"🔍 Screening symbols with criteria:")
    print(f"  Exchange: {args.exchange}")
    print(f"  Min price: ${args.min_price}")
    print(f"  Min volume: {args.min_volume:,}")
    print(f"  Min quality: {args.min_quality}")
    
    results = screener.screen_symbols(criteria)
    
    print(f"\n✅ Found {len(results)} symbols matching criteria")
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"💾 Results saved to {args.output}")
    else:
        # Show sample
        for symbol_data in results[:10]:
            print(f"  {symbol_data['symbol']} - Price: ${symbol_data.get('latest_close', 'N/A')}, Quality: {symbol_data.get('data_quality_score', 'N/A'):.2f}")
        if len(results) > 10:
            print(f"  ... and {len(results) - 10} more")
    
    screener.close_connection()


def main():
    """Main CLI interface."""
    parser = argparse.ArgumentParser(description='Incremental ETL Management CLI')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Setup command
    setup_parser = subparsers.add_parser('setup', help='Set up incremental ETL system')
    setup_parser.set_defaults(func=cmd_setup)
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show processing status')
    status_parser.set_defaults(func=cmd_status)
    
    # Plan command
    plan_parser = subparsers.add_parser('plan', help='Generate processing plan')
    plan_parser.add_argument('universe', help='Universe name')
    plan_parser.add_argument('data_type', help='Data type (listing_status, time_series_daily_adjusted, etc.)')
    plan_parser.add_argument('--output', help='Save plan to JSON file')
    plan_parser.set_defaults(func=cmd_plan)
    
    # Universe command
    universe_parser = subparsers.add_parser('universe', help='Manage universes')
    universe_subparsers = universe_parser.add_subparsers(dest='universe_action')
    
    list_parser = universe_subparsers.add_parser('list', help='List all universes')
    
    show_parser = universe_subparsers.add_parser('show', help='Show universe details')
    show_parser.add_argument('name', help='Universe name')
    show_parser.add_argument('--symbols', action='store_true', help='Show symbol list')
    
    create_parser = universe_subparsers.add_parser('create', help='Create predefined universes')
    
    universe_parser.set_defaults(func=cmd_universe)
    
    # Screen command
    screen_parser = subparsers.add_parser('screen', help='Screen symbols')
    screen_parser.add_argument('--exchange', default='NASDAQ', help='Exchange filter')
    screen_parser.add_argument('--min-price', type=float, default=5.0, help='Minimum price')
    screen_parser.add_argument('--min-volume', type=int, default=500000, help='Minimum average volume')
    screen_parser.add_argument('--exclude-penny', action='store_true', default=True, help='Exclude penny stocks')
    screen_parser.add_argument('--min-quality', type=float, default=0.8, help='Minimum data quality score')
    screen_parser.add_argument('--output', help='Save results to JSON file')
    screen_parser.set_defaults(func=cmd_screen)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        args.func(args)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()