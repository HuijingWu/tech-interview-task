#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# tech test - Huijing Wu


import psycopg2
import datetime

# Quality control checks for the trades table

# This part is to define a function to check if the hash values are within the expected length
def check_hash_length(data, hash_index, expected_length):
    """Check hash length."""
    for row in data:
        hash_value = row[hash_index]
        if len(hash_value) != expected_length:
            print(f"Hash with unexpected length found: {hash_value}")
            
# This function is to check if there are duplicate hash values
def check_duplicate_hashes(data, hash_index):
    """Check for duplicate hash values."""
    seen_hashes = set()
    for row in data:
        hash_value = row[hash_index]
        if hash_value in seen_hashes:
            print(f"Duplicate hash found: {hash_value}")
        else:
            seen_hashes.add(hash_value)
            
# This function is to check if there is any unexpected characters in symbol values           
def check_unexpected_strings(trades_data, symbol_index):
    """Check for unexpected characters in symbol column."""
    for trade in trades_data:
        symbol = trade[symbol_index]
        if not all(char.isalnum() or char in ['.', '-', '_'] for char in symbol):
            print(f"Unexpected characters found in symbol column: {symbol}")
            
# This function is to check if we have trades between the same assets, say "USDUSD"    
def check_duplicate_assets(trade_data,symbol_index):
    """Check for trades involving the same assets."""
    for trade in trades_data:
        symbol = trade[symbol_index]
        half_length = len(symbol) // 2
        asset1, asset2 = symbol[:half_length], symbol[half_length:]
        if asset1 == asset2:
            print(f"Duplicate assets found in symbol: {symbol}")
            
# This function is to check if the numerical values are within reasonable ranges            
def check_numerical_values(trades_data, digits_index, cmd_index, contractsize_index):
    """Check numerical values in trades columns."""
    for trade in trades_data:
        digits, cmd, contractsize = trade[digits_index], trade[cmd_index], trade[contractsize_index]
        if not (2 <= digits <= 5):
            print(f"Digits value out of expected range (2-5): {digits}")
        if cmd not in [0, 1]:
            print(f"Invalid value found in cmd column: {cmd}")
        if contractsize is not None and contractsize <= 0:
            print(f"Invalid value found in contractsize column: {contractsize}")          
            
# This function is to check if volumn is bigger than 0
def check_negative_volume(trades_data, volume_index):
    """Check for negative volume values in trades."""
    for trade in trades_data:
        volume = trade[volume_index]
        if volume < 0:
            print(f"Negative volume found in trade: {trade}")
            
# This function is to check if the date columns are of right format
def check_date_format(trades_data, open_time_index, close_time_index):
    """Check date format in open_time and close_time columns."""
    for trade in trades_data:
        open_time, close_time = trade[open_time_index], trade[close_time_index]
        if not isinstance(open_time, datetime.datetime):
            print(f"Invalid datetime format in open_time column: {open_time}")
        if not isinstance(close_time, datetime.datetime):
            print(f"Invalid datetime format in close_time column: {close_time}")
            
# This function is to check if the close time is later than the open time 
def check_close_time_before_open_time(trades_data, open_time_index, close_time_index):
    """Check if close time is before open time in trades."""
    for trade in trades_data:
        open_time = trade[open_time_index]
        close_time = trade[close_time_index]
        if close_time < open_time:
            print(f"Close time {close_time} is earlier than open time {open_time} in trade: {trade}")

# This function is to check if the open price is bigger than 0             
def check_positive_open_price(trades_data, open_price_index):
    """Check if the open price is bigger than 0."""
    for trade in trades_data:
        open_price = trade[open_price_index]
        if open_price <= 0:
            print(f"Open price is not positive in trade: {trade}")

            
# Quality control checks for the users table

# This function is to 
def check_currency(users_data, currency_index):
    """Check if currency codes are valid."""
    for user in users_data:
        currency = user[currency_index]
        if currency not in ['USD', 'EUR', 'GBP', 'JPY', 'AUD']:
            print(f"Invalid currency code found: {currency}")
            
# This function is to check if enable column has valid values
def check_enable_column(users_data, enable_index):
    """Check if enable column contains only boolean values."""
    for user in users_data:
        enable = user[enable_index]
        if enable not in [0, 1]:
            print(f"Invalid value found in enable column: {enable}")
            
            
# This function is to report all missing records
            
def check_missing_records(trades_data, users_data, trades_hash_index, users_hash_index, table1_name, table2_name):
    """Report records present in one table but not in the other."""
    table1_hashes = set(row[trades_hash_index] for row in trades_data)
    table2_hashes = set(row[users_hash_index] for row in users_data)
    
    # Records in trades table but not in users table
    missing_in_users = table1_hashes - table2_hashes
    if missing_in_users:
        print(f"Records present in {table1_name} table but not in {table2_name} table:")
        for hash_value in missing_in_users:
            print(hash_value)
    
    # Records in users table but not in trades table
    missing_in_trades = table2_hashes - table1_hashes
    if missing_in_trades:
        print(f"Records present in {table2_name} table but not in {table1_name} table:")
        for hash_value in missing_in_trades:
            print(hash_value)

        
        
# This part is to build up connection       
try:
    # Connect to the PostgreSQL database
    with psycopg2.connect(
        dbname='technical_test',
        user='candidate',
        password='NW337AkNQH76veGc',
        host='technical-test-1.cncti7m4kr9f.ap-south-1.rds.amazonaws.com',
        port='5432'
    ) as conn:

        # Create a cursor object
        with conn.cursor() as cursor:

            # Query the trades table
            cursor.execute("SELECT * FROM trades;")
            trades_data = cursor.fetchall()

            # Query the users table 
            cursor.execute("SELECT * FROM users;")
            users_data = cursor.fetchall()

            # Get column indices from data catalog for trades table
            trades_data_catalog = {
                'login_hash': 0, 
                'ticket_hash': 1, 
                'server_hash': 2,
                'symbol': 3,
                'digits': 4,
                'cmd': 5,
                'volume': 6,
                'open_time': 7,
                'open_price': 8,
                'close_time': 9,
                'contractsize': 10
            }
            
            # Get column indices from data catalog for users table
            users_data_catalog = {
                'login_hash': 0, 
                'server_hash': 1, 
                'country_hash': 2,
                'currency': 3, 
                'enable': 4
            }

            # Perform quality control checks for trades data
            
            # Check hash values:
            check_hash_length(trades_data, trades_data_catalog['login_hash'], expected_length=32)
            check_duplicate_hashes(trades_data, trades_data_catalog['login_hash'])
            check_hash_length(trades_data, trades_data_catalog['ticket_hash'], expected_length=32)
            check_duplicate_hashes(trades_data, trades_data_catalog['ticket_hash'])
            check_hash_length(trades_data, trades_data_catalog['server_hash'], expected_length=32)
            
            # Check symbol column:
            check_unexpected_strings(trades_data, trades_data_catalog['symbol'])
            check_duplicate_assets(trades_data, trades_data_catalog['symbol'])
            
            # Check digits & cmd & contractsize columns:
            check_numerical_values(trades_data, trades_data_catalog['digits'], trades_data_catalog['cmd'], trades_data_catalog['contractsize'])
            
            # Check volume column:
            check_negative_volume(trades_data, trades_data_catalog['volume'])
            
            # Check date format:
            check_date_format(trades_data, trades_data_catalog['open_time'], trades_data_catalog['close_time'])
            check_close_time_before_open_time(trades_data, trades_data_catalog['open_time'], trades_data_catalog['close_time'])

            # Check open price column:
            check_positive_open_price(trades_data, trades_data_catalog['open_price'])
            
            
            
            # Perform quality control checks for users data
            
            # Check the hash values: 
            check_hash_length(users_data, users_data_catalog['login_hash'], expected_length=32)
            check_duplicate_hashes(users_data, users_data_catalog['login_hash'])
            check_hash_length(users_data, users_data_catalog['server_hash'], expected_length=32)            
            check_hash_length(users_data, users_data_catalog['country_hash'], expected_length=32)
            
            # Check the currency column:
            check_currency(users_data, users_data_catalog['currency'])
            
            # Check the enable column:
            check_enable_column(users_data, users_data_catalog['enable'])
            
            # check intersections between both tables:
            check_missing_records(trades_data, users_data, trades_data_catalog['login_hash'], users_data_catalog['login_hash'], 'trades', 'users')
            
            
except psycopg2.Error as e:
    print("Error:", e)

