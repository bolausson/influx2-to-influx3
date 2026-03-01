#!/bin/env python3
# -*- coding: utf-8 -*-
#
# -----------------------------------------------------------------------------
# influx_migrate.py - Generic InfluxDB 2 to InfluxDB 3 migration tool
# -----------------------------------------------------------------------------
#
# Migrates all data from an InfluxDB 2.x instance to InfluxDB 3.x.
# Auto-discovers buckets, measurements, tags, and fields.
# Supports resume, rate limiting, streaming reads, and batched writes.
#
# Dependencies:
#   pip install influxdb-client influxdb3-python requests
#

import re
import sys
import time
import json
import signal
import argparse
import configparser
import urllib3
import requests
from pathlib import Path
from datetime import datetime, timedelta
from os.path import expanduser

urllib3.disable_warnings()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_CONFIG_PATH = expanduser('~/.influx2-to-influx3.conf')
SYSTEM_BUCKETS = {'_monitoring', '_tasks'}
METADATA_KEYS = {'_start', '_stop', '_time', '_measurement', 'result', 'table',
                 '_field', '_value'}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser(
    description='Migrate data from InfluxDB 2.x to InfluxDB 3.x',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  %(prog)s --generate-config                    # Generate config file and exit
  %(prog)s --discover-only                      # Show all buckets, measurements, tags, fields
  %(prog)s --all --dry-run                      # Dry-run migration of all data
  %(prog)s --all                                # Migrate all data
  %(prog)s --all --resume                       # Resume an interrupted migration
  %(prog)s --all --buckets openhab/autogen      # Migrate a single bucket
  %(prog)s --incremental                        # Migrate only data added since last run
  %(prog)s --start 2024-01-01 --end 2024-12-31  # Migrate a time range
  %(prog)s --start -30d --dry-run               # Dry-run last 30 days
"""
)

parser.add_argument('--config', dest='config', default=DEFAULT_CONFIG_PATH,
                    help=f'Path to config file (default: {DEFAULT_CONFIG_PATH})')
parser.add_argument('--generate-config', dest='generate_config', action='store_true',
                    default=False, help='Generate config file and exit')
parser.add_argument('--start', dest='start_time', default=None,
                    help='Start time (e.g., 2024-01-01, -30d, -1y)')
parser.add_argument('--end', dest='end_time', default='now',
                    help='End time (e.g., 2024-12-31, now) (default: now)')
parser.add_argument('--all', dest='migrate_all', action='store_true', default=False,
                    help='Migrate all data from earliest available')
parser.add_argument('--buckets', dest='buckets', default=None,
                    help='Comma-separated bucket names to migrate (overrides config)')
parser.add_argument('--batch-size', dest='batch_size', type=int, default=None,
                    help='Records per write batch (overrides config, default: 5000)')
parser.add_argument('--chunk-days', dest='chunk_days', type=int, default=None,
                    help='Days per query chunk (overrides config, default: 7)')
parser.add_argument('--rate-limit', dest='rate_limit', type=int, default=None,
                    help='Max records/sec, 0=unlimited (overrides config)')
parser.add_argument('--write-delay', dest='write_delay', type=float, default=None,
                    help='Seconds to pause between batch writes (overrides config, default: 0)')
parser.add_argument('--chunk-delay', dest='chunk_delay', type=float, default=None,
                    help='Seconds to pause between chunks, letting the target persist (overrides config, default: 0)')
parser.add_argument('--wal-window', dest='wal_window', type=int, default=None,
                    help='Write at full speed for this many seconds, then pause (default: 0 = disabled)')
parser.add_argument('--wal-pause', dest='wal_pause', type=float, default=None,
                    help='Seconds to pause after each --wal-window cycle (default: 30)')
parser.add_argument('--dry-run', '-t', dest='dryrun', action='store_true', default=False,
                    help='Show what would be migrated without writing')
parser.add_argument('--verbose', '-v', dest='verbose', action='store_true', default=False,
                    help='Enable verbose output')
parser.add_argument('--resume', dest='resume', action='store_true', default=False,
                    help='Resume a previously interrupted migration')
parser.add_argument('--progress-file', dest='progress_file', default=None,
                    help='Path to progress file (overrides config)')
parser.add_argument('--discover-only', dest='discover_only', action='store_true',
                    default=False,
                    help='Discover and print all buckets, measurements, tags, and fields, then exit')
parser.add_argument('--incremental', dest='incremental', action='store_true', default=False,
                    help='Migrate only new data since the last successful migration')
parser.add_argument('--short', dest='short', action='store_true', default=False,
                    help='With --discover-only: only show bucket names and measurement counts')

args = parser.parse_args()

# ---------------------------------------------------------------------------
# Config generation
# ---------------------------------------------------------------------------


def generate_config(config_path):
    """Generate a config file with dummy placeholder values."""

    content = f"""\
# InfluxDB 2 to InfluxDB 3 migration configuration
# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

[INFLUXDB2]
url = https://localhost:8086
token = YOUR_INFLUXDB2_TOKEN
org = YOUR_ORG
verify_ssl = False

[INFLUXDB3]
url = http://localhost:8181
token = YOUR_INFLUXDB3_TOKEN
verify_ssl = False

[MIGRATION]
# Comma-separated bucket names to migrate, or "all" to auto-discover
buckets = all
# Number of days per query chunk
chunk_days = 7
# Records per write batch
batch_size = 5000
# Max records per second (0 = unlimited)
rate_limit = 0
# Seconds to pause between batch writes (0 = no delay)
write_delay = 0
# Seconds to pause between chunks, letting the target persist (0 = no delay)
chunk_delay = 0
# Write at full speed for this many seconds, then pause (0 = disabled)
wal_window = 0
# Seconds to pause after each wal_window cycle
wal_pause = 30
# Progress file for resume support
progress_file = ~/.influx_migration_progress.json
"""

    Path(config_path).parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, 'w') as f:
        f.write(content)
    print(f'Config written to: {config_path}')


if args.generate_config:
    print('Generating config file...')
    generate_config(args.config)
    sys.exit(0)

# ---------------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------------

if not Path(args.config).is_file():
    print(f'Config file not found: {args.config}')
    print('Generating default config...')
    generate_config(args.config)
    print(f'Please edit {args.config} and re-run.')
    sys.exit(1)

config = configparser.ConfigParser()
config.read(args.config)

for section in ('INFLUXDB2', 'INFLUXDB3'):
    if section not in config:
        print(f'Error: [{section}] section missing in {args.config}', file=sys.stderr)
        sys.exit(1)

IFDB2_URL = config['INFLUXDB2'].get('url', 'https://localhost:8086')
IFDB2_TOKEN = config['INFLUXDB2'].get('token', '')
IFDB2_ORG = config['INFLUXDB2'].get('org', '')
IFDB2_VERIFY_SSL = config['INFLUXDB2'].get('verify_ssl', 'False').lower() in ('true', '1')

IFDB3_URL = config['INFLUXDB3'].get('url', 'http://localhost:8181')
IFDB3_TOKEN = config['INFLUXDB3'].get('token', '')
IFDB3_VERIFY_SSL = config['INFLUXDB3'].get('verify_ssl', 'False').lower() in ('true', '1')

# Migration settings (CLI overrides config)
migrate_sec = config['MIGRATION'] if 'MIGRATION' in config else {}
CFG_BUCKETS = args.buckets or migrate_sec.get('buckets', 'all')
CHUNK_DAYS = args.chunk_days or int(migrate_sec.get('chunk_days', '7'))
BATCH_SIZE = args.batch_size or int(migrate_sec.get('batch_size', '5000'))
RATE_LIMIT = args.rate_limit if args.rate_limit is not None else int(migrate_sec.get('rate_limit', '0'))
WRITE_DELAY = args.write_delay if args.write_delay is not None else float(migrate_sec.get('write_delay', '0'))
CHUNK_DELAY = args.chunk_delay if args.chunk_delay is not None else float(migrate_sec.get('chunk_delay', '0'))
WAL_WINDOW = args.wal_window if args.wal_window is not None else int(migrate_sec.get('wal_window', '0'))
WAL_PAUSE = args.wal_pause if args.wal_pause is not None else float(migrate_sec.get('wal_pause', '30'))
PROGRESS_FILE = args.progress_file or expanduser(migrate_sec.get(
    'progress_file', '~/.influx_migration_progress.json'))

# Validate (skip for discover-only mode)
if not args.discover_only and not args.start_time and not args.migrate_all and not args.incremental:
    print('Error: Either --start, --all, --incremental, or --discover-only must be specified',
          file=sys.stderr)
    parser.print_help()
    sys.exit(1)

# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


def parse_time_arg(time_arg):
    """Parse time argument into a Flux-compatible time string."""
    if time_arg == 'now':
        return 'now()'
    if time_arg.startswith('-') and time_arg[-1] in 'dhms':
        return time_arg
    try:
        dt = datetime.fromisoformat(time_arg)
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        print(f'Error: Invalid time format: {time_arg}', file=sys.stderr)
        sys.exit(1)


def format_duration(seconds):
    """Format seconds into a human-readable duration string."""
    if seconds < 60:
        return f'{seconds:.0f}s'
    elif seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f'{m}m {s:02d}s'
    elif seconds < 86400:
        h, rem = divmod(int(seconds), 3600)
        m = rem // 60
        return f'{h}h {m:02d}m'
    else:
        d, rem = divmod(int(seconds), 86400)
        h = rem // 3600
        m = (rem % 3600) // 60
        return f'{d}d {h}h {m:02d}m'


def generate_time_chunks(start_dt, end_dt, chunk_days):
    """Generate non-overlapping (start, end) time chunks."""
    chunks = []
    current = start_dt
    delta = timedelta(days=chunk_days)
    while current < end_dt:
        chunk_end = min(current + delta, end_dt)
        chunks.append((current, chunk_end))
        current = chunk_end
    return chunks


# ---------------------------------------------------------------------------
# Progress file management
# ---------------------------------------------------------------------------


def load_progress():
    """Load progress from the progress file."""
    pf = Path(PROGRESS_FILE)
    if pf.is_file():
        try:
            with open(pf) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f'Warning: Could not read progress file: {e}', file=sys.stderr)
    return {}


def progress_identity():
    """Return a dict identifying the current migration source/dest."""
    return {
        'source_url': IFDB2_URL,
        'source_org': IFDB2_ORG,
        'dest_url': IFDB3_URL,
    }


def validate_progress(progress):
    """Check that a progress file matches the current migration config.

    Returns True if it matches, False otherwise.
    """
    identity = progress.get('identity')
    if not identity:
        return True  # old-format progress file, allow it
    current = progress_identity()
    for key in ('source_url', 'source_org', 'dest_url'):
        if identity.get(key) != current.get(key):
            return False
    return True


def save_progress(progress):
    """Save progress to the progress file."""
    progress['timestamp'] = datetime.now().isoformat()
    progress['identity'] = progress_identity()
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress, f, indent=2)
    except OSError as e:
        print(f'Warning: Could not save progress file: {e}', file=sys.stderr)


def save_completion(end_time_str):
    """Save a completion watermark to the progress file.

    This replaces the in-progress data with a slim record that --incremental
    can use as its start time for the next run.
    """
    watermark = {
        'completed': True,
        'completed_at': datetime.now().isoformat(),
        'end_time': end_time_str,
        'identity': progress_identity(),
    }
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(watermark, f, indent=2)
    except OSError as e:
        print(f'Warning: Could not save completion watermark: {e}', file=sys.stderr)


# ---------------------------------------------------------------------------
# Signal handling (graceful Ctrl+C)
# ---------------------------------------------------------------------------

interrupted = False


def sigint_handler(signum, frame):
    global interrupted
    if interrupted:
        print('\nForce quit.', file=sys.stderr)
        sys.exit(1)
    interrupted = True
    print('\nInterrupted — finishing current chunk before saving progress...',
          file=sys.stderr)


signal.signal(signal.SIGINT, sigint_handler)

# ---------------------------------------------------------------------------
# Handle --incremental: read watermark from previous completion
# ---------------------------------------------------------------------------

if args.incremental:
    watermark = load_progress()
    if not watermark:
        print('Error: No progress file found. Run a full migration first (--all).',
              file=sys.stderr)
        sys.exit(1)
    if not watermark.get('completed'):
        print('Error: Previous migration did not complete. Use --resume to finish it first.',
              file=sys.stderr)
        sys.exit(1)
    if not validate_progress(watermark):
        identity = watermark.get('identity', {})
        print(f'Error: Progress file belongs to a different migration:', file=sys.stderr)
        print(f'  Progress: {identity.get("source_url")} → {identity.get("dest_url")}',
              file=sys.stderr)
        print(f'  Current:  {IFDB2_URL} → {IFDB3_URL}', file=sys.stderr)
        sys.exit(1)
    # Use the end_time of the last successful migration as the new start
    args.start_time = watermark['end_time']
    args.end_time = 'now'
    print(f'Incremental mode: migrating data since {args.start_time}')

# ---------------------------------------------------------------------------
# Parse time arguments
# ---------------------------------------------------------------------------

if not args.discover_only:
    if args.migrate_all:
        args.start_time = '1970-01-01'

    start_time = parse_time_arg(args.start_time)
    end_time = parse_time_arg(args.end_time)

# ---------------------------------------------------------------------------
# Print settings
# ---------------------------------------------------------------------------

if args.discover_only:
    print(f'Source InfluxDB 2: {IFDB2_URL}')
else:
    print('Migration settings:')
    print(f'  Source InfluxDB 2:  {IFDB2_URL}')
    print(f'  Dest   InfluxDB 3:  {IFDB3_URL}')
    print(f'  Buckets:            {CFG_BUCKETS}')
    print(f'  Time range:         {start_time} to {end_time}')
    print(f'  Chunk days:         {CHUNK_DAYS}')
    print(f'  Batch size:         {BATCH_SIZE}')
    print(f'  Rate limit:         {RATE_LIMIT} rec/s' if RATE_LIMIT else '  Rate limit:         unlimited')
    print(f'  Write delay:        {WRITE_DELAY}s' if WRITE_DELAY else '  Write delay:        none')
    print(f'  Chunk delay:        {CHUNK_DELAY}s' if CHUNK_DELAY else '  Chunk delay:        none')
    if WAL_WINDOW:
        print(f'  WAL window:         write {WAL_WINDOW}s, pause {WAL_PAUSE}s')
    print(f'  Dry run:            {args.dryrun}')
    if args.incremental:
        print(f'  Mode:               incremental (since last completion)')
print()

# ---------------------------------------------------------------------------
# Connect to InfluxDB 2 (source)
# ---------------------------------------------------------------------------

try:
    from influxdb_client import InfluxDBClient
except ImportError:
    print("Error: 'influxdb-client' package required. Install with: pip install influxdb-client",
          file=sys.stderr)
    sys.exit(1)

print('Connecting to InfluxDB 2 (source)...')
ifdbc2 = InfluxDBClient(
    url=IFDB2_URL,
    token=IFDB2_TOKEN,
    org=IFDB2_ORG,
    verify_ssl=IFDB2_VERIFY_SSL,
    timeout=(60_000, 600_000),
)
ifdbc2_query = ifdbc2.query_api()

# ---------------------------------------------------------------------------
# InfluxDB 3 write client setup (deferred per-database)
# ---------------------------------------------------------------------------

write_errors = []
ifdb3_clients = {}  # database_name -> InfluxDBClient3

if not args.dryrun:
    try:
        from influxdb_client_3 import (InfluxDBClient3, write_client_options,
                                        WriteOptions, WritePrecision)
        from influxdb_client_3.write_client.client.write_api import WriteType
    except ImportError:
        print("Error: 'influxdb3-python' package required. Install with: pip install influxdb3-python",
              file=sys.stderr)
        sys.exit(1)


def get_ifdb3_client(database):
    """Get or create an InfluxDB 3 client for the given database."""
    if database in ifdb3_clients:
        return ifdb3_clients[database]
    if args.dryrun:
        return None

    def _on_write_success(conf, data):
        if args.verbose:
            lines = data.split('\n') if isinstance(data, str) else [data]
            print(f'      Batch flushed: {len(lines)} lines')

    def _on_write_error(conf, data, exception):
        write_errors.append(str(exception))
        print(f'    ERROR writing batch: {exception}', file=sys.stderr)

    def _on_write_retry(conf, data, exception):
        if args.verbose:
            print(f'      Retrying batch: {exception}')

    wco = write_client_options(
        write_options=WriteOptions(
            write_type=WriteType.batching,
            batch_size=BATCH_SIZE,
            flush_interval=10_000,
            retry_interval=5_000,
            max_retries=5,
            max_retry_delay=30_000,
            exponential_base=2,
            max_close_wait=300_000,
        ),
        success_callback=_on_write_success,
        error_callback=_on_write_error,
        retry_callback=_on_write_retry,
    )
    client = InfluxDBClient3(
        host=IFDB3_URL,
        database=database,
        token=IFDB3_TOKEN,
        write_client_options=wco,
        write_timeout=60_000,
    )
    ifdb3_clients[database] = client
    if args.verbose:
        print(f'    Connected to InfluxDB 3 database: {database}')
    return client


# ---------------------------------------------------------------------------
# InfluxDB 2 discovery functions
# ---------------------------------------------------------------------------


def discover_buckets():
    """Auto-discover all user buckets via the InfluxDB 2 API."""
    print('Discovering buckets...')
    url = f'{IFDB2_URL}/api/v2/buckets?limit=100'
    headers = {'Authorization': f'Token {IFDB2_TOKEN}'}
    try:
        resp = requests.get(url, headers=headers, verify=IFDB2_VERIFY_SSL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        buckets = []
        for b in data.get('buckets', []):
            name = b['name']
            if name not in SYSTEM_BUCKETS:
                buckets.append(name)
        return sorted(buckets)
    except Exception as e:
        print(f'Error discovering buckets: {e}', file=sys.stderr)
        sys.exit(1)


def discover_measurements(bucket):
    """Discover all measurements in a bucket using Flux schema."""
    flux = f'''import "influxdata/influxdb/schema"
schema.measurements(bucket: "{bucket}")'''
    try:
        tables = ifdbc2_query.query(flux)
        measurements = []
        for table in tables:
            for record in table.records:
                measurements.append(record.get_value())
        return sorted(measurements)
    except Exception as e:
        print(f'  Error discovering measurements for {bucket}: {e}', file=sys.stderr)
        return []


def discover_tag_keys(bucket, measurement):
    """Discover tag keys for a specific measurement using Flux schema."""
    flux = f'''import "influxdata/influxdb/schema"
schema.measurementTagKeys(bucket: "{bucket}", measurement: "{measurement}")'''
    try:
        tables = ifdbc2_query.query(flux)
        tags = set()
        for table in tables:
            for record in table.records:
                val = record.get_value()
                # Filter out internal tag keys
                if val and not val.startswith('_'):
                    tags.add(val)
        return tags
    except Exception as e:
        print(f'    Warning: Could not discover tag keys for {measurement}: {e}',
              file=sys.stderr)
        return set()


def discover_field_keys(bucket, measurement):
    """Discover field keys for a specific measurement using Flux schema."""
    flux = f'''import "influxdata/influxdb/schema"
schema.measurementFieldKeys(bucket: "{bucket}", measurement: "{measurement}")'''
    try:
        tables = ifdbc2_query.query(flux)
        fields = set()
        for table in tables:
            for record in table.records:
                val = record.get_value()
                if val:
                    fields.add(val)
        return fields
    except Exception as e:
        print(f'    Warning: Could not discover field keys for {measurement}: {e}',
              file=sys.stderr)
        return set()


def find_earliest_timestamp(bucket, measurement):
    """Find the earliest timestamp for a measurement using InfluxQL (fast)."""
    db_name = bucket.split('/')[0] if '/' in bucket else bucket
    query = f'SELECT * FROM "{measurement}" ORDER BY time ASC LIMIT 1'
    url = f'{IFDB2_URL}/query'
    try:
        resp = requests.get(
            url,
            params={'db': db_name, 'q': query},
            headers={'Authorization': f'Token {IFDB2_TOKEN}'},
            verify=IFDB2_VERIFY_SSL,
            timeout=60,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if 'results' in data and data['results'] and 'series' in data['results'][0]:
            ts_str = data['results'][0]['series'][0]['values'][0][0]
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            return dt.replace(tzinfo=None)
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Bucket name → InfluxDB 3 database name mapping
# ---------------------------------------------------------------------------


def bucket_to_database(bucket):
    """Map an InfluxDB 2 bucket name to an InfluxDB 3 database name.

    Strips the retention policy suffix (e.g., 'openhab/autogen' → 'openhab').
    """
    return bucket.split('/')[0] if '/' in bucket else bucket


# ---------------------------------------------------------------------------
# Streaming migration logic
# ---------------------------------------------------------------------------


def build_flux_query(bucket, measurement, start, stop, rename_tags=None):
    """Build a Flux query with pivot for the given measurement and time range.

    rename_tags: dict mapping original tag names to temporary names, used to
    avoid pivot() collisions when a tag and field share the same name.
    """
    rename_step = ''
    if rename_tags:
        col_map = ', '.join(f'"{old}": "{new}"' for old, new in rename_tags.items())
        rename_step = f'\n  |> rename(columns: {{{col_map}}})'
    return f'''from(bucket: "{bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r["_measurement"] == "{measurement}"){rename_step}
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''


def process_stream(record_stream, tag_keys, ifdb3_client, stats,
                   renamed_tags=None):
    """Process a streaming iterator of FluxRecords, writing to InfluxDB 3.

    tag_keys: set of column names that are tags (from schema discovery).
    renamed_tags: dict mapping original tag names to their temporary renamed
                  names in the query results (for tag/field collision handling).
    stats: dict with 'records_read', 'records_written', 'batches_written', 'sample'.
    """
    batch = []
    batch_start_time = time.time()
    # Reverse map: temporary name → original name
    reverse_rename = {v: k for k, v in (renamed_tags or {}).items()}

    for record in record_stream:
        stats['records_read'] += 1

        ts = record.get_time()
        measurement = record.values.get('_measurement', 'unknown')

        # Build tags — only columns in the discovered tag_keys set
        # For renamed tags, look up the temporary name but store the original
        tags = {}
        for key in tag_keys:
            lookup = renamed_tags.get(key, key) if renamed_tags else key
            if lookup in record.values and record.values[lookup] is not None:
                tags[key] = str(record.values[lookup])

        # Build fields — everything else that isn't metadata or tags
        fields = {}
        for key, value in record.values.items():
            if key in METADATA_KEYS or key in tag_keys or value is None:
                continue
            # Skip renamed tag columns (e.g. _tag_singleton)
            if key in reverse_rename:
                continue
            # Preserve type: try float, fall back to string
            if isinstance(value, (int, float)):
                fields[key] = float(value)
            elif isinstance(value, bool):
                fields[key] = value
            elif isinstance(value, str):
                try:
                    fields[key] = float(value)
                except (ValueError, TypeError):
                    fields[key] = value
            else:
                try:
                    fields[key] = float(value)
                except (ValueError, TypeError):
                    pass

        if not fields:
            continue

        rec = {
            'measurement': measurement,
            'tags': tags,
            'fields': fields,
            'time': ts.isoformat(),
        }

        if stats['sample'] is None:
            stats['sample'] = rec

        if args.dryrun:
            stats['records_written'] += 1
        else:
            batch.append(rec)

            if len(batch) >= BATCH_SIZE:
                ifdb3_client.write(record=batch)
                stats['records_written'] += len(batch)
                stats['batches_written'] += 1
                batch = []

                # Rate limiting
                if RATE_LIMIT > 0:
                    elapsed = time.time() - batch_start_time
                    expected = BATCH_SIZE / RATE_LIMIT
                    if elapsed < expected:
                        time.sleep(expected - elapsed)
                    batch_start_time = time.time()

                # Write delay — give InfluxDB 3 time to persist
                if WRITE_DELAY > 0:
                    time.sleep(WRITE_DELAY)

    # Write remaining records
    if batch and not args.dryrun:
        ifdb3_client.write(record=batch)
        stats['records_written'] += len(batch)
        stats['batches_written'] += 1


# ---------------------------------------------------------------------------
# Determine which buckets to migrate
# ---------------------------------------------------------------------------

if CFG_BUCKETS.lower() == 'all':
    buckets_to_migrate = discover_buckets()
else:
    buckets_to_migrate = [b.strip() for b in CFG_BUCKETS.split(',') if b.strip()]

if not buckets_to_migrate:
    print('No buckets found to migrate.')
    sys.exit(0)

print(f'Buckets: {", ".join(buckets_to_migrate)}')
print()

# ---------------------------------------------------------------------------
# Discover-only mode: print schema info and exit
# ---------------------------------------------------------------------------

if args.discover_only:
    total_measurements = 0
    for bucket in buckets_to_migrate:
        database = bucket_to_database(bucket)

        measurements = discover_measurements(bucket)
        total_measurements += len(measurements)

        if args.short:
            count = len(measurements) if measurements else 0
            print(f'{bucket} → {database}  ({count} measurements)')
        else:
            print(f'=== Bucket: {bucket} → Database: {database} ===')
            if not measurements:
                print('  (no measurements)')
                print()
                continue

            print(f'  {len(measurements)} measurements:')
            for measurement in measurements:
                tag_keys = discover_tag_keys(bucket, measurement)
                field_keys = discover_field_keys(bucket, measurement)
                parts = []
                if tag_keys:
                    parts.append(f'tags: {", ".join(sorted(tag_keys))}')
                if field_keys:
                    parts.append(f'fields: {", ".join(sorted(field_keys))}')
                detail = f'  ({"; ".join(parts)})' if parts else ''
                print(f'    {measurement}{detail}')
            print()

    print(f'Total: {len(buckets_to_migrate)} buckets, {total_measurements} measurements')
    ifdbc2.close()
    sys.exit(0)

# ---------------------------------------------------------------------------
# Load resume progress
# ---------------------------------------------------------------------------

progress = {}
existing_progress = load_progress()

if args.incremental:
    # --incremental already validated the watermark above; start fresh progress
    progress = {}
elif args.resume:
    if existing_progress:
        if existing_progress.get('completed'):
            print(f'Error: Previous migration already completed. Use --incremental to migrate new data.',
                  file=sys.stderr)
            sys.exit(1)
        if not validate_progress(existing_progress):
            identity = existing_progress.get('identity', {})
            print(f'Error: Progress file belongs to a different migration:', file=sys.stderr)
            print(f'  Progress: {identity.get("source_url")} → {identity.get("dest_url")}',
                  file=sys.stderr)
            print(f'  Current:  {IFDB2_URL} → {IFDB3_URL}', file=sys.stderr)
            print(f'  Delete {PROGRESS_FILE} first, or use --progress-file for a different path.',
                  file=sys.stderr)
            sys.exit(1)
        progress = existing_progress
        print(f'Loaded progress from: {PROGRESS_FILE}')
        if args.verbose:
            for bk, bv in progress.get('buckets', {}).items():
                for ms, mv in bv.get('measurements', {}).items():
                    if mv == 'done':
                        print(f'  {bk}/{ms}: done')
                    else:
                        print(f'  {bk}/{ms}: through {mv.get("completed_through", "?")}')
        print()
    else:
        print('No progress file found, starting from the beginning.')
        print()
elif existing_progress:
    # Not resuming, but a progress file exists — refuse to overwrite
    if existing_progress.get('completed'):
        # Completion watermark from a previous run — safe to overwrite with a new migration
        pass
    elif not validate_progress(existing_progress):
        print(f'Error: Progress file from a different migration exists: {PROGRESS_FILE}',
              file=sys.stderr)
        print(f'  Delete it first, or use --progress-file for a different path.',
              file=sys.stderr)
        sys.exit(1)
    else:
        print(f'Warning: Progress file exists from a previous run: {PROGRESS_FILE}')
        print(f'  Use --resume to continue, or delete it to start fresh.')
        sys.exit(1)

if 'buckets' not in progress:
    progress['buckets'] = {}

# ---------------------------------------------------------------------------
# Main migration loop
# ---------------------------------------------------------------------------

migration_start = time.time()
grand_total_read = 0
grand_total_written = 0
grand_total_batches = 0
bucket_count = 0
timing_report = []  # [(bucket, elapsed, [(measurement, elapsed), ...])]

for bucket in buckets_to_migrate:
    if interrupted:
        break

    database = bucket_to_database(bucket)
    bucket_start_time = time.time()
    meas_timings = []  # [(measurement, elapsed)]
    print(f'=== Bucket: {bucket} → Database: {database} ===')

    # Discover measurements
    measurements = discover_measurements(bucket)
    if not measurements:
        print(f'  No measurements found, skipping.')
        print()
        continue

    print(f'  Measurements: {", ".join(measurements)}')

    if bucket not in progress['buckets']:
        progress['buckets'][bucket] = {'measurements': {}}

    for measurement in measurements:
        if interrupted:
            break

        meas_progress = progress['buckets'][bucket]['measurements'].get(measurement)
        if meas_progress == 'done':
            print(f'  [{measurement}] Already completed, skipping.')
            continue

        print(f'  [{measurement}]')

        # Discover tag and field keys for generic tag/field separation
        # (skip in dry-run unless verbose, since it's a Flux query per measurement)
        tag_keys = set()
        field_keys = set()
        conflicting_keys = {}
        if not args.dryrun or args.verbose:
            tag_keys = discover_tag_keys(bucket, measurement)
            field_keys = discover_field_keys(bucket, measurement)
            if args.verbose and tag_keys:
                print(f'    Tag keys: {", ".join(sorted(tag_keys))}')
            # Detect tag/field name collisions that break pivot()
            overlap = tag_keys & field_keys
            if overlap:
                conflicting_keys = {k: f'_tag_{k}' for k in overlap}
                print(f'    Tag/field collision: {", ".join(sorted(overlap))} '
                      f'— renaming tags in query')

        # Determine time range
        if args.migrate_all:
            earliest = find_earliest_timestamp(bucket, measurement)
            if earliest is None:
                print(f'    No data found, skipping.')
                continue
            start_dt = earliest
            if args.verbose:
                print(f'    Earliest data: {start_dt}')
        else:
            # Parse relative/absolute start time to datetime
            st = start_time
            if st.startswith('-') and st[-1] in 'dhms':
                unit = st[-1]
                val = int(st[1:-1])
                delta_map = {'d': 'days', 'h': 'hours', 'm': 'minutes', 's': 'seconds'}
                start_dt = datetime.now() - timedelta(**{delta_map[unit]: val})
            elif st == 'now()':
                start_dt = datetime.now()
            else:
                start_dt = datetime.fromisoformat(st.replace('Z', '').replace('T', ' ').split('+')[0])

        et = end_time
        if et == 'now()':
            end_dt = datetime.now()
        elif et.startswith('-') and et[-1] in 'dhms':
            unit = et[-1]
            val = int(et[1:-1])
            delta_map = {'d': 'days', 'h': 'hours', 'm': 'minutes', 's': 'seconds'}
            end_dt = datetime.now() - timedelta(**{delta_map[unit]: val})
        else:
            end_dt = datetime.fromisoformat(et.replace('Z', '').replace('T', ' ').split('+')[0])

        chunks = generate_time_chunks(start_dt, end_dt, CHUNK_DAYS)
        print(f'    {len(chunks)} chunks ({CHUNK_DAYS}-day), '
              f'{start_dt.strftime("%Y-%m-%d")} to {end_dt.strftime("%Y-%m-%d")}')

        # Dry run: just report discovery, skip actual data streaming
        if args.dryrun:
            if tag_keys:
                print(f'    Tags: {", ".join(sorted(tag_keys))}')
            continue

        # Resume: skip completed chunks
        skip_until = None
        if meas_progress and isinstance(meas_progress, dict):
            skip_until = meas_progress.get('completed_through')
            if skip_until:
                print(f'    Resuming after: {skip_until}')
                grand_total_read += meas_progress.get('records_read', 0)
                grand_total_written += meas_progress.get('records_written', 0)

        # Get InfluxDB 3 client for this database
        ifdb3_client = get_ifdb3_client(database)

        stats = {
            'records_read': meas_progress.get('records_read', 0) if isinstance(meas_progress, dict) else 0,
            'records_written': meas_progress.get('records_written', 0) if isinstance(meas_progress, dict) else 0,
            'batches_written': 0,
            'sample': None,
        }
        meas_start_time = time.time()
        chunks_completed = 0
        wal_window_start = time.time()
        total_pause_time = 0.0

        for i, (chunk_start, chunk_end) in enumerate(chunks):
            chunk_start_str = chunk_start.strftime('%Y-%m-%dT%H:%M:%SZ')
            chunk_end_str = chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ')

            # Skip completed chunks
            if skip_until and chunk_end_str <= skip_until:
                if args.verbose:
                    print(f'    Chunk {i+1}/{len(chunks)}: skipped (done)')
                continue

            if interrupted:
                print(f'    Interrupted before chunk {i+1}. Resume with --resume.')
                break

            if args.verbose:
                print(f'    Chunk {i+1}/{len(chunks)}: {chunk_start_str} to {chunk_end_str}')

            flux = build_flux_query(bucket, measurement, chunk_start_str, chunk_end_str,
                                   rename_tags=conflicting_keys or None)

            try:
                t0 = time.time()
                record_stream = ifdbc2_query.query_stream(flux)
                process_stream(record_stream, tag_keys, ifdb3_client, stats,
                               renamed_tags=conflicting_keys or None)
                t1 = time.time()
            except Exception as e:
                # Auto-detect pivot collision and retry with rename
                err_str = str(e)
                if 'pivot' in err_str and 'already exists' in err_str:
                    m = re.search(r'value\s+[\\"]+([\w]+)[\\"]+ appears in a column key column',
                                  err_str)
                    if m:
                        col_name = m.group(1)
                        if col_name not in conflicting_keys:
                            conflicting_keys[col_name] = f'_tag_{col_name}'
                            tag_keys.add(col_name)
                            print(f'\n    Pivot collision detected: {col_name} '
                                  f'— retrying with rename')
                            try:
                                t0 = time.time()
                                flux2 = build_flux_query(
                                    bucket, measurement,
                                    chunk_start_str, chunk_end_str,
                                    rename_tags=conflicting_keys)
                                record_stream = ifdbc2_query.query_stream(flux2)
                                process_stream(
                                    record_stream, tag_keys, ifdb3_client, stats,
                                    renamed_tags=conflicting_keys)
                                t1 = time.time()
                            except Exception as e2:
                                print(f'\n    Error in chunk {i+1} (retry): {e2}',
                                      file=sys.stderr)
                                continue
                        else:
                            print(f'\n    Error in chunk {i+1}: {e}', file=sys.stderr)
                            continue
                    else:
                        print(f'\n    Error in chunk {i+1}: {e}', file=sys.stderr)
                        continue
                else:
                    print(f'\n    Error in chunk {i+1}: {e}', file=sys.stderr)
                    continue

            chunks_completed += 1

            # Progress with elapsed time and ETA
            meas_elapsed = t1 - meas_start_time
            write_time = meas_elapsed - total_pause_time
            remaining_chunks = len(chunks) - (i + 1)
            avg_write_per_chunk = write_time / chunks_completed if chunks_completed else 0
            # Project future WAL pauses into ETA
            if WAL_WINDOW > 0 and avg_write_per_chunk > 0:
                chunks_per_window = WAL_WINDOW / avg_write_per_chunk
                future_pauses = (remaining_chunks / chunks_per_window) * WAL_PAUSE if chunks_per_window > 0 else 0
            else:
                future_pauses = 0
            eta_sec = (avg_write_per_chunk * remaining_chunks) + future_pauses
            rate = stats['records_written'] / write_time if write_time > 0 else 0
            wal_info = ''
            if WAL_WINDOW > 0:
                wal_remaining = max(0, WAL_WINDOW - (time.time() - wal_window_start))
                wal_info = f' | write {int(wal_remaining)}s'
            line = (f'    Chunk {i+1}/{len(chunks)} | {rate:,.0f} rec/s '
                    f'| elapsed: {format_duration(meas_elapsed)} '
                    f'| ETA: {format_duration(eta_sec)}{wal_info}')
            print(f'\r{line}\033[K', end='', flush=True)

            # Save progress after each chunk
            progress['buckets'][bucket]['measurements'][measurement] = {
                'completed_through': chunk_end_str,
                'records_read': stats['records_read'],
                'records_written': stats['records_written'],
            }
            save_progress(progress)

            # Chunk delay — let InfluxDB 3 snapshot and persist
            if CHUNK_DELAY > 0 and remaining_chunks > 0:
                time.sleep(CHUNK_DELAY)

            # WAL window — write at full speed, then pause to let
            # InfluxDB 3 snapshot before the next cycle
            if WAL_WINDOW > 0 and remaining_chunks > 0:
                wal_elapsed = time.time() - wal_window_start
                if wal_elapsed >= WAL_WINDOW:
                    pause_start = time.time()
                    pause_remaining = int(WAL_PAUSE)
                    while pause_remaining > 0:
                        wall_elapsed = time.time() - meas_start_time
                        line = (f'    Chunk {i+1}/{len(chunks)} | {rate:,.0f} rec/s '
                                f'| elapsed: {format_duration(wall_elapsed)} '
                                f'| ETA: {format_duration(eta_sec)} '
                                f'| WAL pause {pause_remaining}s')
                        print(f'\r{line}\033[K', end='', flush=True)
                        time.sleep(1)
                        pause_remaining -= 1
                    total_pause_time += time.time() - pause_start
                    wal_window_start = time.time()

        # Newline after carriage-returned progress line
        if chunks_completed > 0:
            print()

        # Mark measurement as done if all chunks completed without interruption
        meas_elapsed = time.time() - meas_start_time
        if not interrupted:
            grand_total_read += stats['records_read']
            grand_total_written += stats['records_written']
            grand_total_batches += stats['batches_written']
            progress['buckets'][bucket]['measurements'][measurement] = 'done'
            save_progress(progress)
            rate = stats['records_written'] / meas_elapsed if meas_elapsed > 0 else 0
            print(f'    Done: {stats["records_read"]:,} read, '
                  f'{stats["records_written"]:,} written '
                  f'({format_duration(meas_elapsed)}, {rate:,.0f} rec/s)')
        else:
            # Accumulate partial stats for interrupted measurement
            grand_total_read += stats['records_read']
            grand_total_written += stats['records_written']
            grand_total_batches += stats['batches_written']
        meas_timings.append((measurement, meas_elapsed))

    bucket_elapsed = time.time() - bucket_start_time
    timing_report.append((bucket, bucket_elapsed, meas_timings))
    bucket_count += 1
    print(f'  Bucket done: {format_duration(bucket_elapsed)}')
    print()

# ---------------------------------------------------------------------------
# Flush and close all InfluxDB 3 clients
# ---------------------------------------------------------------------------

if ifdb3_clients:
    print('Flushing remaining writes...')
    for db, client in ifdb3_clients.items():
        try:
            client.close()
        except Exception as e:
            print(f'  Error closing client for {db}: {e}', file=sys.stderr)
    ifdb3_clients.clear()

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

migration_elapsed = time.time() - migration_start

print()
if interrupted:
    print('Migration interrupted — progress saved.')
    print(f'  Resume with: {sys.argv[0]} --all --resume')
    print(f'  Progress file: {PROGRESS_FILE}')
else:
    print('Migration complete!')
    # Save completion watermark so --incremental can pick up from here
    completion_end = datetime.now().isoformat()
    save_completion(completion_end)
    print(f'  Watermark saved. Use --incremental to migrate new data later.')

if timing_report:
    print('Measurement timing:')
    for bucket, bucket_elapsed, meas_timings in timing_report:
        print(f'  {bucket}: {format_duration(bucket_elapsed)}')
        for meas_name, meas_elapsed in meas_timings:
            print(f'    {meas_name}: {format_duration(meas_elapsed)}')
    print()

    if len(timing_report) > 1:
        print('Bucket timing:')
        for bucket, bucket_elapsed, _ in timing_report:
            print(f'  {bucket}: {format_duration(bucket_elapsed)}')
        print()

print('Overall:')
print(f'  Buckets processed:            {bucket_count}')
print(f'  Total records read:           {grand_total_read:,}')
print(f'  Total records written:        {grand_total_written:,}')
print(f'  Total batches:                {grand_total_batches:,}')
print(f'  Elapsed time:                 {format_duration(migration_elapsed)}')
if grand_total_written > 0 and migration_elapsed > 0:
    print(f'  Throughput:                   {grand_total_written / migration_elapsed:,.0f} records/s')

if write_errors:
    print(f'\n  WARNING: {len(write_errors)} write error(s) occurred!', file=sys.stderr)
    for err in write_errors[:5]:
        print(f'    - {err}', file=sys.stderr)

if args.dryrun:
    print()
    print('[DRY RUN] No data was actually written to InfluxDB 3')

ifdbc2.close()
