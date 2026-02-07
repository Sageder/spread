import os
import json
import polars as pl
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from flatten_json import flatten
from datetime import datetime, timezone
import time
from pathlib import Path
from update_utils.update_markets import update_markets

# Global runtime timestamp - set once when program starts
RUNTIME_TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')

# Columns to save
COLUMNS_TO_SAVE = ['timestamp', 'maker', 'makerAssetId', 'makerAmountFilled', 'taker', 'takerAssetId', 'takerAmountFilled', 'transactionHash']

if not os.path.isdir('goldsky'):
    os.mkdir('goldsky')

CURSOR_FILE = 'goldsky/cursor_state.json'

def save_cursor(timestamp, last_id, sticky_timestamp=None):
    """Save cursor state to file for efficient resume."""
    state = {
        'last_timestamp': timestamp,
        'last_id': last_id,
        'sticky_timestamp': sticky_timestamp
    }
    with open(CURSOR_FILE, 'w') as f:
        json.dump(state, f)

def get_latest_cursor():
    """Get the latest cursor state for efficient resume.
    Returns (timestamp, last_id, sticky_timestamp) tuple."""
    # First try to load from cursor state file (most efficient)
    if os.path.isfile(CURSOR_FILE):
        try:
            with open(CURSOR_FILE, 'r') as f:
                state = json.load(f)
            timestamp = state.get('last_timestamp', 0)
            last_id = state.get('last_id')
            sticky_timestamp = state.get('sticky_timestamp')

            # Validate cursor state: if sticky_timestamp is set, last_id must also be set
            if sticky_timestamp is not None and last_id is None:
                print(f"Warning: Invalid cursor state (sticky_timestamp={sticky_timestamp} but last_id=None), clearing sticky state")
                sticky_timestamp = None

            if timestamp > 0:
                readable_time = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                print(f'Resuming from cursor state: timestamp {timestamp} ({readable_time}), id: {last_id}, sticky: {sticky_timestamp}')
                return timestamp, last_id, sticky_timestamp
        except Exception as e:
            print(f"Error reading cursor file: {e}")

    # Fallback: read from parquet file
    cache_file = 'goldsky/orderFilled.parquet'

    if not os.path.isfile(cache_file):
        print("No existing file found, starting from beginning of time (timestamp 0)")
        return 0, None, None

    try:
        # Read parquet file and get last timestamp
        df = pl.read_parquet(cache_file)
        if len(df) > 0 and 'timestamp' in df.columns:
            last_timestamp = df['timestamp'][-1]
            readable_time = datetime.fromtimestamp(int(last_timestamp), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            print(f'Resuming from parquet (no cursor file): timestamp {last_timestamp} ({readable_time})')
            # Go back 1 second to ensure no data loss (may create some duplicates)
            return int(last_timestamp) - 1, None, None
    except Exception as e:
        print(f"Error reading parquet file: {e}")

    # Fallback to beginning of time
    print("Falling back to beginning of time (timestamp 0)")
    return 0, None, None

def scrape(at_once=1000):
    QUERY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
    print(f"Query URL: {QUERY_URL}")
    print(f"Runtime timestamp: {RUNTIME_TIMESTAMP}")
    
    # Get starting cursor from latest file (includes sticky state for perfect resume)
    last_timestamp, last_id, sticky_timestamp = get_latest_cursor()
    count = 0
    total_records = 0

    print(f"\nStarting scrape for orderFilledEvents")

    output_file = 'goldsky/orderFilled.parquet'
    print(f"Output file: {output_file}")
    print(f"Saving columns: {COLUMNS_TO_SAVE}")

    # Ensure directory exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    # Load existing data if available
    if os.path.exists(output_file):
        existing_df = pl.read_parquet(output_file)
    else:
        existing_df = None

    # Batch buffer - accumulate batches before writing (reduces write amplification for GB files)
    batch_buffer = []
    WRITE_EVERY_N_BATCHES = 10  # Write every 10 batches to reduce disk I/O

    while True:
        # Build the where clause based on cursor state
        if sticky_timestamp is not None:
            # We're in sticky mode: stay at this timestamp and paginate by id
            where_clause = f'timestamp: "{sticky_timestamp}", id_gt: "{last_id}"'
        else:
            # Normal mode: advance by timestamp
            where_clause = f'timestamp_gt: "{last_timestamp}"'
        
        q_string = '''query MyQuery {
                        orderFilledEvents(orderBy: timestamp, orderDirection: asc
                                             first: ''' + str(at_once) + '''
                                             where: {''' + where_clause + '''}) {
                            fee
                            id
                            maker
                            makerAmountFilled
                            makerAssetId
                            orderHash
                            taker
                            takerAmountFilled
                            takerAssetId
                            timestamp
                            transactionHash
                        }
                    }
                '''

        query = gql(q_string)
        transport = RequestsHTTPTransport(url=QUERY_URL, verify=True, retries=3)
        client = Client(transport=transport)
        
        try:
            res = client.execute(query)
        except Exception as e:
            print(f"Query error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
            continue
        
        if not res['orderFilledEvents'] or len(res['orderFilledEvents']) == 0:
            if sticky_timestamp is not None:
                # Exhausted events at sticky timestamp, advance to next timestamp
                last_timestamp = sticky_timestamp
                sticky_timestamp = None
                last_id = None
                continue
            print(f"No more data for orderFilledEvents")
            break

        # Convert to polars DataFrame
        df = pl.DataFrame([flatten(x) for x in res['orderFilledEvents']])

        # Sort by timestamp and id for consistent ordering
        df = df.sort(['timestamp', 'id'])

        batch_last_timestamp = int(df['timestamp'][-1])
        batch_last_id = df['id'][-1]
        batch_first_timestamp = int(df['timestamp'][0])
        
        readable_time = datetime.fromtimestamp(batch_last_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        # Determine if we need sticky cursor for next iteration
        if len(df) >= at_once:
            # Batch is full - check if all events are at the same timestamp
            if batch_first_timestamp == batch_last_timestamp:
                # All events at same timestamp, need to continue paginating at this timestamp
                sticky_timestamp = batch_last_timestamp
                last_id = batch_last_id
                print(f"Batch {count + 1}: Timestamp {batch_last_timestamp} ({readable_time}), Records: {len(df)} [STICKY - continuing at same timestamp]")
            else:
                # Mixed timestamps - some events might be lost at the boundary timestamp
                # Stay sticky at the last timestamp to ensure we get all events
                sticky_timestamp = batch_last_timestamp
                last_id = batch_last_id
                print(f"Batch {count + 1}: Timestamps {batch_first_timestamp}-{batch_last_timestamp} ({readable_time}), Records: {len(df)} [STICKY - ensuring complete timestamp]")
        else:
            # Batch not full - we have all events, can advance normally
            if sticky_timestamp is not None:
                # We were in sticky mode, now exhausted - advance past this timestamp
                last_timestamp = sticky_timestamp
                sticky_timestamp = None
                last_id = None
                print(f"Batch {count + 1}: Timestamp {batch_last_timestamp} ({readable_time}), Records: {len(df)} [STICKY COMPLETE]")
            else:
                # Normal advancement
                last_timestamp = batch_last_timestamp
                print(f"Batch {count + 1}: Last timestamp {batch_last_timestamp} ({readable_time}), Records: {len(df)}")
        
        count += 1
        total_records += len(df)

        # Remove duplicates (by id to be safe)
        df = df.unique(subset=['id'])

        # Filter to only the columns we want to save
        df_to_save = df.select(COLUMNS_TO_SAVE)

        # Add to batch buffer
        batch_buffer.append(df_to_save)

        # Write to disk every N batches to reduce write amplification on GB files
        if count % WRITE_EVERY_N_BATCHES == 0 or len(df) < at_once:
            # Combine buffer batches
            if len(batch_buffer) > 1:
                buffer_combined = pl.concat(batch_buffer).unique(subset=['id'])
            else:
                buffer_combined = batch_buffer[0]

            # Append to existing data and save
            if existing_df is not None:
                combined_df = pl.concat([existing_df, buffer_combined])
            else:
                combined_df = buffer_combined

            # Remove duplicates across entire dataset
            combined_df = combined_df.unique(subset=['id'])

            # Ensure proper types for long IDs
            combined_df = combined_df.with_columns([
                pl.col("makerAssetId").cast(pl.Utf8),
                pl.col("takerAssetId").cast(pl.Utf8)
            ])

            combined_df.write_parquet(output_file)
            existing_df = combined_df
            batch_buffer = []  # Clear buffer

            print(f"   💾 Checkpoint: Written {len(combined_df):,} total records to disk")

        # Save cursor state for efficient resume (no duplicates on restart)
        save_cursor(last_timestamp, last_id, sticky_timestamp)

        if len(df) < at_once and sticky_timestamp is None:
            break

    # Write any remaining buffered data before exit
    if len(batch_buffer) > 0:
        print(f"   💾 Writing final {len(batch_buffer)} buffered batch(es)...")
        if len(batch_buffer) > 1:
            buffer_combined = pl.concat(batch_buffer).unique(subset=['id'])
        else:
            buffer_combined = batch_buffer[0]

        if existing_df is not None:
            combined_df = pl.concat([existing_df, buffer_combined])
        else:
            combined_df = buffer_combined

        combined_df = combined_df.unique(subset=['id'])
        combined_df = combined_df.with_columns([
            pl.col("makerAssetId").cast(pl.Utf8),
            pl.col("takerAssetId").cast(pl.Utf8)
        ])
        combined_df.write_parquet(output_file)
        print(f"   💾 Final checkpoint: {len(combined_df):,} total records")

    # Clear cursor file on successful completion
    if os.path.isfile(CURSOR_FILE):
        os.remove(CURSOR_FILE)

    print(f"Finished scraping orderFilledEvents")
    print(f"Total new records: {total_records}")
    print(f"Output file: {output_file}")

def update_goldsky():
    """Run scraping for orderFilledEvents"""
    print(f"\n{'='*50}")
    print(f"Starting to scrape orderFilledEvents")
    print(f"Runtime: {RUNTIME_TIMESTAMP}")
    print(f"{'='*50}")
    try:
        scrape()
        print(f"Successfully completed orderFilledEvents")
    except Exception as e:
        print(f"Error scraping orderFilledEvents: {str(e)}")