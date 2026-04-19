pipecheck

> CLI tool to validate and monitor ETL pipeline health with configurable alerting

---

## Installation

```bash
pip install pipecheck
```

Or install from source:

```bash
git clone https://github.com/yourname/pipecheck.git && cd pipecheck && pip install -e .
```

---

## Usage

Run a health check against your pipeline configuration:

```bash
pipecheck run --config pipeline.yaml
```

Validate a config file without executing checks:

```bash
pipecheck validate --config pipeline.yaml
```

Example `pipeline.yaml`:

```yaml
pipeline:
  name: sales_etl
  checks:
    - type: row_count
      source: orders
      min: 1000
    - type: null_check
      column: customer_id
      threshold: 0.01
  alerts:
    email: ops@example.com
    on: [failure, warning]
```

Run with a specific alert channel:

```bash
pipecheck run --config pipeline.yaml --alert slack
```

---

## Features

- Schema and row-level validation
- Configurable alert thresholds and channels
- Supports multiple pipeline sources (Postgres, MySQL, S3)
- CI/CD friendly exit codes

---

## License

This project is licensed under the [MIT License](LICENSE).