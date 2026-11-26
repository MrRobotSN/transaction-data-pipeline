
# Config-Driven Transaction Pipeline

## Overview

A config-driven batch data pipeline built using PySpark
and Delta Lake to process, validate and incrementally
load transaction data.

## Key Features

- Config-driven schema enforcement
- Data-type validation
- Invalid and malformed record quarantine
- UTC timestamp standardisation
- Primary key deduplication
- Incremental file processing
- Late-arriving transaction handling
- Idempotent Delta Lake MERGE
- Record reconciliation

## Technology Stack

- Python
- PySpark
- Delta Lake
- JSON
- Ubuntu / WSL

## Project Structure

- `pipeline.py` - Data processing logic
- `config.json` - Pipeline configuration
- `requirements.txt` - Project dependencies
- `docs/` - Architecture and governance documentation
- `data/` - Local source and target storage

## Execution

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create the incoming directory:

```bash
mkdir -p data/incoming
```

Place the transaction CSV files in `data/incoming/`
and execute:

```bash
python pipeline.py
```

## Documentation

- [Pipeline Architecture](docs/Architecture.md)
- [Data Quality, Governance and Security](docs/Governance.md)

## Production Considerations

The pipeline runs locally to simulate a cloud-based
data lake. The proposed AWS architecture addresses
scalability, orchestration, monitoring, security
and failure recovery.
