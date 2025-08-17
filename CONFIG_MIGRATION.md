# Configuration File Migration

## Summary

Both the broker (server) and client have been migrated from command-line arguments to configuration file-based startup parameters. This provides better configuration management, easier deployment, and more flexible parameter handling.

## Changes Made

### 1. New Configuration Package

Created `internal/config/config.go` with:
- `BrokerConfig` struct for broker configuration
- `ClientConfig` struct for client configuration  
- `LoadBrokerConfig()` and `LoadClientConfig()` functions
- JSON file parsing with duration support

### 2. Broker Configuration

**Before:** Command line arguments
```bash
./broker -log=broker.log -port=9092 -data=./data
```

**After:** Configuration file
```bash
./broker -config=configs/broker.json
```

Configuration file structure (`configs/broker.json`):
```json
{
  "data_dir": "./data",
  "max_topic_partitions": 16,
  "segment_size": 1073741824,
  "retention_time": "168h",
  "max_storage_size": 107374182400,
  "flush_interval": "1s",
  "cleanup_interval": "1h",
  "max_message_size": 1048576,
  
  "compression_enabled": true,
  "compression_type": 3,
  "compression_threshold": 100,
  
  "deduplication_enabled": true,
  "deduplication_config": {
    "hash_type": 1,
    "max_entries": 100000,
    "ttl": "24h",
    "enabled": true
  },

  "server": {
    "port": "9092",
    "log_file": ""
  }
}
```

### 3. Client Configuration

**Before:** Command line arguments
```bash
./client -cmd=produce -topic=my-topic -partition=0 -message="Hello" -broker=localhost:9092
```

**After:** Configuration file with optional overrides
```bash
./client -config=configs/client.json
# OR with overrides:
./client -config=configs/client.json -cmd=produce -topic=override-topic
```

Configuration file structure (`configs/client.json`):
```json
{
  "broker": "localhost:9092",
  "timeout": "10s",
  "log_file": "",
  "command": {
    "type": "",
    "topic": "",
    "partition": 0,
    "message": "",
    "offset": 0,
    "count": 1
  }
}
```

### 4. Example Configurations

Created specialized configuration files:
- `configs/client-create-topic.json` - For creating topics
- `configs/client-produce.json` - For producing messages
- `configs/client-consume.json` - For consuming messages

### 5. Command Line Override Support

The client still supports command-line arguments, but now they override configuration file values:

```bash
# Use config file completely
./client -config=configs/client-produce.json

# Override specific parameters
./client -config=configs/client.json -cmd=produce -topic=new-topic -message="Override message"

# Override broker address
./client -config=configs/client.json -broker=remote-server:9092
```

### 6. Updated VS Code Debug Configurations

Updated `.vscode/launch.json` to use configuration files instead of command-line arguments:
- "Debug Broker" now uses `-config=configs/broker.json`
- All client debug configurations use respective config files
- Added "Debug Client - Command Line Override" to demonstrate override functionality

## Benefits

1. **Better Configuration Management**: All settings in centralized files
2. **Environment-Specific Configs**: Easy to have dev/staging/prod configs
3. **Backward Compatibility**: Command-line overrides still work
4. **Easier Deployment**: Single config file per environment
5. **Version Control**: Configuration can be tracked in git
6. **Complex Configurations**: Better support for nested configurations

## Usage Examples

### Start Broker
```bash
# Default config
./broker-server

# Custom config
./broker-server -config=configs/production-broker.json
```

### Client Operations
```bash
# Create topic from config
./client-tool -config=configs/client-create-topic.json

# Produce with override
./client-tool -config=configs/client-produce.json -message="Custom message"

# Pure command line (still supported)
./client-tool -cmd=consume -topic=my-topic -partition=0 -offset=0 -broker=localhost:9092
```

## Migration Guide

For existing setups:
1. Copy your command-line arguments to appropriate config files
2. Update scripts/deployment files to use `-config` parameter
3. Test with both config files and command-line overrides
4. Gradually migrate to pure config file usage

## Configuration File Validation

The system automatically:
- Sets default values for missing parameters
- Validates configuration structure
- Provides clear error messages for invalid configs
- Supports duration parsing (e.g., "24h", "30m", "5s") 