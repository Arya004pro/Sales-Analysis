# Modules Directory Guide

This folder contains non-UI orchestration and business logic extracted from Motia step files.

## Structure

- `pipeline/`
  - Core query pipeline logic used by step wrappers.
  - Contains extracted implementations for:
    - parse intent
    - text-to-SQL
    - execute query
    - forecast projection
    - anomaly detection
    - result formatting

- `reporting/`
  - Business report generation and report endpoint logic.
  - Contains extracted implementations for:
    - report generation
    - report scheduler
    - report run endpoint
    - report get endpoint

- `utilities/`
  - Shared utility HTTP/ingest/query services used by thin step handlers.
  - Contains extracted implementations for:
    - query intake/session follow-up resolution
    - ingest orchestration response builder
    - schema/query/report utility endpoint responses

## Rule of thumb

- Keep files in `motia/steps/` thin and process-visible for UI flow tracking.
- Keep heavy logic, SQL composition, parsing, and report generation in `modules/`.
