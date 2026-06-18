# Kafka Spark Streaming Pipeline

A real-time event pipeline for ingesting, validating, aggregating, and checkpointing sales events with replay-safe processing.

## Architecture

`	ext
Producer -> Kafka -> Spark -> Delta Bronze/Silver -> Metrics
`

## Technology stack

Apache Kafka, Spark Structured Streaming, Delta Lake

## Repository blueprint

- project.yaml — scope, pipeline stages, and quality expectations
- src/ — ingestion and transformation implementation
- 	ests/ — unit, integration, and data-quality tests
- docs/ — architecture decisions and operational guidance

## Implementation roadmap

1. Create representative source data and document its contract.
2. Implement the pipeline stages with idempotent processing.
3. Add automated schema, null, duplicate, and business-rule checks.
4. Capture run metadata, rejected records, and performance metrics.
5. Add CI validation and publish screenshots or sample outputs.

## Definition of done

- Reproducible setup with no embedded credentials
- Incremental and restart-safe processing
- Automated tests and documented quality thresholds
- Observable runs with clear failure handling
- Architecture diagram and demonstration results

## Status

Portfolio scaffold created. Implementation milestones are tracked in the roadmap above.