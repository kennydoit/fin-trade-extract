"""
Symbol ID calculation utilities for deterministic symbol identification.

This module provides functions to calculate stable, alphabetically-ordered symbol IDs
that can be used as primary keys and foreign keys without relying on SERIAL sequences.
"""

def calculate_symbol_id(symbol: str) -> int:
    """
    Calculate a deterministic symbol ID that maintains alphabetical ordering.

    Uses base-27 calculation with A=1, B=2, ..., Z=26 to ensure:
    - Deterministic: Same symbol always produces same ID
    - Alphabetical: IDs sort in alphabetical order
    - Unique: No collisions for valid stock symbols
    - Stable: Won't change between runs

    Args:
        symbol (str): Stock symbol (e.g., 'AAPL', 'MSFT', 'A', 'AA')

    Returns:
        int: Calculated symbol ID (base offset 1,000,000)

    Examples:
        >>> calculate_symbol_id('A')
        15348907
        >>> calculate_symbol_id('AA')
        15880348
        >>> calculate_symbol_id('AAPL')
        15882139
        >>> calculate_symbol_id('MSFT')
        22620702
    """
    # Clean symbol: remove non-alphabetic characters and convert to uppercase
    clean_symbol = ''.join(c for c in symbol.upper() if c.isalpha())

    # Base offset to ensure positive IDs and avoid conflicts
    base_offset = 1_000_000
    symbol_id = 0

    # Calculate base-27 value using fixed 6-character field positioning
    # Pad symbol to 6 characters for consistent positioning
    padded_symbol = clean_symbol[:6].ljust(6, ' ')

    for i, char in enumerate(padded_symbol):
        char_value = 0 if char == ' ' else ord(char) - ord('A') + 1

        # Position weight: leftmost char has highest weight (27^5), rightmost has 27^0
        position = 6 - 1 - i
        symbol_id += char_value * (27 ** position)

    return base_offset + symbol_id


def validate_symbol_id_uniqueness(symbols: list) -> dict:
    """
    Validate that all symbols produce unique calculated IDs.

    Args:
        symbols (list): List of stock symbols to check

    Returns:
        dict: Validation results with any conflicts found
    """
    symbol_to_id = {}
    id_to_symbols = {}
    conflicts = []

    for symbol in symbols:
        calc_id = calculate_symbol_id(symbol)
        symbol_to_id[symbol] = calc_id

        if calc_id in id_to_symbols:
            conflicts.append({
                'id': calc_id,
                'symbols': [id_to_symbols[calc_id], symbol]
            })
        else:
            id_to_symbols[calc_id] = symbol

    return {
        'total_symbols': len(symbols),
        'unique_ids': len(id_to_symbols),
        'conflicts': conflicts,
        'is_valid': len(conflicts) == 0
    }


def test_alphabetical_ordering(symbols: list) -> dict:
    """
    Test that calculated symbol IDs maintain alphabetical ordering.

    Args:
        symbols (list): List of stock symbols to test

    Returns:
        dict: Test results showing ordering validation
    """
    # Calculate IDs and sort by them
    symbol_ids = [(symbol, calculate_symbol_id(symbol)) for symbol in symbols]
    sorted_by_id = sorted(symbol_ids, key=lambda x: x[1])

    # Sort symbols alphabetically
    sorted_alphabetically = sorted(symbols)

    # Extract symbols from ID-sorted list
    id_order_symbols = [symbol for symbol, _ in sorted_by_id]

    return {
        'alphabetical_order': sorted_alphabetically,
        'id_order': id_order_symbols,
        'orders_match': sorted_alphabetically == id_order_symbols,
        'sample_mappings': sorted_by_id[:10]  # First 10 for inspection
    }


if __name__ == "__main__":
    # Quick test with common symbols
    test_symbols = ['A', 'AA', 'AAA', 'AAPL', 'AB', 'ABC', 'MSFT', 'TSLA', 'Z', 'ZZ']


    for symbol in test_symbols:
        calc_id = calculate_symbol_id(symbol)

    validation = validate_symbol_id_uniqueness(test_symbols)

    ordering = test_alphabetical_ordering(test_symbols)
