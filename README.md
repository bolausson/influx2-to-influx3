# influx2-to-influx3

Migrate data from InfluxDB 2.x to InfluxDB 3.x (Community Edition / OSS).

## Features

- **Auto-discovery** — finds all buckets, measurements, tags, and fields automatically
- **Streaming reads** — memory-efficient Flux queries with chunked time windows
- **Batched writes** — configurable batch size with retry and error handling
- **Resume support** — interrupted migrations can be resumed from the last completed chunk
- **Incremental sync** — re-run to migrate only new data since the last successful run
- **WAL-aware throttling** — write at full speed, then pause to let InfluxDB 3 snapshot and persist, preventing OOM on resource-constrained targets
- **Pivot collision handling** — automatically detects and resolves tag/field name conflicts that break Flux `pivot()` queries
- **Progress display** — real-time throughput, elapsed time, and ETA with in-place terminal updates
- **Config file** — all settings can be stored in a config file; CLI arguments override config values

## Requirements

- Python 3.8+
- An InfluxDB 2.x instance (source)
- An InfluxDB 3.x instance (target)

```bash
pip install influxdb-client influxdb3-python requests
```

## Quick Start

```bash
# Generate a config file with placeholder values
./influx2-to-influx3.py --generate-config

# Edit the config with your connection details
vi ~/.influx2-to-influx3.conf

# Discover what's available (no data is written)
./influx2-to-influx3.py --discover-only

# Dry-run to see what would be migrated
./influx2-to-influx3.py --all --dry-run

# Run the migration
./influx2-to-influx3.py --all
```

## Configuration

The config file (`~/.influx2-to-influx3.conf`) is generated with `--generate-config`:

```ini
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
buckets = all
chunk_days = 7
batch_size = 5000
rate_limit = 0
write_delay = 0
chunk_delay = 0
wal_window = 0
wal_pause = 30
progress_file = ~/.influx_migration_progress.json
```

## Usage

```
./influx2-to-influx3.py [OPTIONS]
```

### Data Selection

| Option | Description |
|--------|-------------|
| `--all` | Migrate all data from the earliest available timestamp |
| `--buckets NAME[,NAME]` | Comma-separated bucket names (e.g., `openhab/autogen,chia/autogen`) |
| `--start TIME` | Start time (`2024-01-01`, `-30d`, `-1y`) |
| `--end TIME` | End time (`2024-12-31`, `now`) — default: `now` |
| `--incremental` | Migrate only new data since the last successful run |

### Performance Tuning

| Option | Default | Description |
|--------|---------|-------------|
| `--batch-size N` | 5000 | Records per write batch |
| `--chunk-days N` | 7 | Days per query chunk |
| `--rate-limit N` | 0 | Max records/sec (0 = unlimited) |
| `--write-delay SEC` | 0 | Pause between batch writes |
| `--chunk-delay SEC` | 0 | Pause between chunks |
| `--wal-window SEC` | 0 | Write at full speed for N seconds, then pause (0 = disabled) |
| `--wal-pause SEC` | 30 | Pause duration after each WAL window cycle |

### Other Options

| Option | Description |
|--------|-------------|
| `--config PATH` | Path to config file (default: `~/.influx2-to-influx3.conf`) |
| `--generate-config` | Generate a config file with placeholder values and exit |
| `--discover-only` | Print all buckets, measurements, tags, and fields, then exit |
| `--short` | With `--discover-only`: only show bucket names and measurement counts |
| `--dry-run`, `-t` | Show what would be migrated without writing |
| `--verbose`, `-v` | Enable verbose output |
| `--resume` | Resume a previously interrupted migration |
| `--progress-file PATH` | Path to progress file (overrides config) |

## WAL-Aware Throttling

When migrating to a resource-constrained InfluxDB 3 target (e.g., Raspberry Pi), the WAL (Write-Ahead Log) snapshot can consume all available RAM and crash the system. The `--wal-window` and `--wal-pause` options address this:

```bash
# Write at full speed for 110 seconds, then pause 30 seconds
./influx2-to-influx3.py --all --wal-window 110 --wal-pause 30
```

The pause gives InfluxDB 3 time to snapshot the WAL and persist data to Parquet files before the next write cycle.

**Tuning**: Set `--wal-window` slightly above your InfluxDB 3 `--wal-snapshot-size` value (default: 600 WAL flushes at ~1/sec = ~600 seconds). For example, with `INFLUXDB3_WAL_SNAPSHOT_SIZE=100`, use `--wal-window 110`.

## Examples

```bash
# Discover all buckets and measurements
./influx2-to-influx3.py --discover-only

# Migrate a single bucket
./influx2-to-influx3.py --all --buckets openhab/autogen

# Migrate last 30 days
./influx2-to-influx3.py --start -30d

# Migrate with conservative settings for a low-resource target
./influx2-to-influx3.py --all --chunk-days 1 --batch-size 1000 --wal-window 110 --wal-pause 30

# Resume after interruption
./influx2-to-influx3.py --all --resume

# Incremental sync (only new data since last run)
./influx2-to-influx3.py --incremental
```

## Bucket Naming

InfluxDB 2 buckets with retention policies (e.g., `openhab/autogen`) are mapped to InfluxDB 3 databases using the bucket name prefix (e.g., `openhab`). The retention policy suffix is stripped.

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE) for details.
