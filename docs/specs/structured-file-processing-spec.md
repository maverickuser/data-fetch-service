# Structured File Processing Specification: CSV and JSON

## Purpose

Define a simple, traceable process for reading CSV files and JSON objects, validating their input fields and values, mapping accepted values to an internal model, and retaining source evidence.

## Principles

- Preserve the original uploaded CSV or JSON file unchanged.
- Treat every CSV cell as a string when it is first read; parse JSON primitives according to JSON's native types.
- Keep the raw string even after a value is parsed or normalized.
- Keep the canonical representation independent of supplier-specific headers and internal application field names.
- Map only validated source values to the internal model.
- Do not silently discard or replace invalid values; quarantine them with errors.

## Deployment network constraint

The processing service and the data-fetch service run in the same AWS VPC. A separate shared-network Terraform state owns the VPC, private subnets, route tables, NAT gateways, VPC endpoints, and shared service security groups. The two application Terraform stacks consume the same versioned outputs rather than accepting independent VPC or subnet configuration. Processing submission uses a private internal load balancer and private DNS; the fetch Lambda security group is the only approved client of that listener. A deployment must fail if the VPC IDs, subnet VPC IDs, load-balancer VPC, or security-group VPCs do not match.

## CSV example input

```csv
Invoice No,Invoice Date,Vendor,Amount,Currency
INV-1042,2026-09-15,Acme Ltd,500.00,USD
INV-1043,2026-09-16,Globex,invalid,USD
```

## Process

```text
Upload CSV
  -> Store original file
  -> Parse headers, rows, and cells
  -> Create canonical representation
  -> Validate source headers
  -> Parse and validate source values
  -> Quarantine invalid rows
  -> Map valid source fields to internal fields
  -> Validate internal business rules
  -> Store usable records with lineage
```

The same logical process applies to JSON. The source location changes from CSV row/column coordinates to a JSONPath.

### 1. Store the original file

Store the exact uploaded file once in object storage, for example:

```text
documents/doc_123/source.csv
```

Record its document ID, original filename, format, content hash, upload timestamp, source, and access metadata. The original file is immutable evidence and supports replay. Do not store a redundant JSON copy of the entire CSV.

### 2. Parse the CSV

Read the encoding, delimiter, header row, rows, and cells. At this stage, every cell is a string. Record its precise source location.

```json
{
  "source_row": 2,
  "cells": [
    {"source_header": "Invoice No", "raw_value": "INV-1042"},
    {"source_header": "Amount", "raw_value": "500.00"}
  ]
}
```

### 3. Create a canonical representation

Persist parsed source data in a generic, format-independent structure. The canonical structure retains source headers, raw values, row/column locations, parsed values, and validation results. It is not yet an invoice or other internal business object.

```json
{
  "document_id": "doc_123",
  "format": "csv",
  "source_row": 2,
  "cell": {
    "source_header": "Amount",
    "raw_value": "500.00",
    "location": {"row": 2, "column": "Amount"}
  }
}
```

### 4. Validate source headers

Apply a versioned source contract.

```yaml
required_headers:
  - Invoice No
  - Invoice Date
  - Amount
  - Currency

header_aliases:
  Invoice #: Invoice No
  Invoice Number: Invoice No
```

Validate that required headers exist, headers are not duplicated, aliases are recognized, and unexpected headers follow the configured policy.

### 5. Parse and validate source values

The source contract defines field types and constraints.

```yaml
fields:
  Invoice No:
    type: string
    required: true
  Invoice Date:
    type: date
    format: yyyy-MM-dd
    required: true
  Amount:
    type: decimal
    required: true
    min: 0
    scale: 2
  Currency:
    type: string
    allowed_values: [USD, INR, EUR]
```

For a numeric field, first preserve the original text, then parse it using the declared locale and formatting rules. Use decimal arithmetic for money rather than floating point.

```json
{
  "source_header": "Amount",
  "raw_value": "500.00",
  "parsed_value": 500.00,
  "expected_type": "decimal",
  "validation_status": "passed",
  "location": {"row": 2, "column": "Amount"}
}
```

An invalid value stays visible with a precise reason.

```json
{
  "source_header": "Amount",
  "raw_value": "invalid",
  "parsed_value": null,
  "expected_type": "decimal",
  "validation_status": "failed",
  "error_code": "not_a_number",
  "location": {"row": 3, "column": "Amount"}
}
```

### 6. Quarantine invalid rows

Rows with failed required validation do not become internal business records. Preserve them in the canonical layer with validation issues for review or correction.

```text
Row 2: valid -> continue
Row 3: Amount is not a decimal -> quarantine
```

### 7. Map valid source fields to the internal model

Apply a separate, versioned mapping contract only to valid source values.

```yaml
source_to_internal_mapping:
  Invoice No: invoice.invoice_number
  Invoice Date: invoice.invoice_date
  Vendor: invoice.vendor_name
  Amount: invoice.total_amount
  Currency: invoice.currency
```

The result is an application-friendly object with retained lineage.

```json
{
  "invoice_number": "INV-1042",
  "invoice_date": "2026-09-15",
  "vendor_name": "Acme Ltd",
  "total_amount": 500.00,
  "currency": "USD",
  "lineage": {
    "invoice.total_amount": {
      "source_header": "Amount",
      "source_location": {"document_id": "doc_123", "row": 2, "column": "Amount"},
      "raw_value": "500.00",
      "mapping_contract_version": "supplier-invoice-to-internal-v1"
    }
  }
}
```

### 8. Validate internal business rules

Validate constraints that belong to the internal application, such as invoice-number uniqueness, supported currencies, known vendor resolution, and business-specific date rules. A source value can be structurally valid but still fail an internal rule.

### 9. Store the results

Maintain three logical categories of data:

1. Original file: immutable `source.csv` stored once.
2. Canonical records: source headers, raw values, parsed typed values, locations, validation outcomes, and errors.
3. Internal records: validated, mapped domain objects such as invoices, each linked back to its canonical source data.

## JSON object processing

JSON is structurally richer than CSV. A JSON document may contain nested objects, arrays, numbers, booleans, and null values. Preserve the original JSON document once, then represent each JSON value in the canonical layer with its JSONPath.

### JSON example input

```json
{
  "supplier": "Acme Ltd",
  "invoices": [
    {
      "invoiceNo": "INV-1042",
      "invoiceDate": "2026-09-15",
      "amount": "500.00",
      "currency": "USD"
    },
    {
      "invoiceNo": "INV-1043",
      "invoiceDate": "2026-09-16",
      "amount": "invalid",
      "currency": "USD"
    }
  ]
}
```

### 1. Store the original JSON once

Store the unchanged file, for example:

```text
documents/doc_124/source.json
```

The original file remains the source evidence. Store document metadata and a content hash as for CSV.

### 2. Parse JSON and create canonical nodes

Parse JSON while retaining its hierarchy. Each canonical value has a path that identifies its exact source location.

```json
{
  "document_id": "doc_124",
  "format": "json",
  "path": "$.invoices[0].amount",
  "source_key": "amount",
  "raw_value": "500.00",
  "json_type": "string"
}
```

An object or array can also be retained as a canonical structural node:

```json
{
  "path": "$.invoices",
  "node_type": "array",
  "child_count": 2
}
```

For JSON, a business record is usually one item in an array. Here, `$.invoices[0]` and `$.invoices[1]` are invoice records.

### 3. Validate the JSON source contract

The JSON source contract defines required paths, expected JSON types, and value constraints.

```yaml
source_contract:
  name: supplier-invoices-json
  version: v1
  record_path: $.invoices[*]
  fields:
    invoiceNo:
      path: $.invoiceNo
      type: string
      required: true
    invoiceDate:
      path: $.invoiceDate
      type: date
      format: yyyy-MM-dd
      required: true
    amount:
      path: $.amount
      type: decimal
      required: true
      min: 0
      scale: 2
    currency:
      path: $.currency
      type: string
      allowed_values: [USD, INR, EUR]
```

The `record_path` identifies the repeatable source entity. Each relative field path is evaluated within one record.

### 4. Validate JSON values

JSON has native types, but source contracts remain necessary. A JSON number may be valid JSON but invalid for money because it is negative or has too many decimal places. A JSON string may need parsing as a date or decimal.

For the example, `$.invoices[0].amount` is a string, so the pipeline parses it as a decimal without losing the original value:

```json
{
  "path": "$.invoices[0].amount",
  "raw_value": "500.00",
  "json_type": "string",
  "parsed_value": 500.00,
  "expected_type": "decimal",
  "validation_status": "passed"
}
```

The second amount fails and its containing invoice object is quarantined:

```json
{
  "record_path": "$.invoices[1]",
  "field_path": "$.invoices[1].amount",
  "raw_value": "invalid",
  "validation_status": "failed",
  "error_code": "not_a_number"
}
```

### 5. Map validated JSON fields to the internal model

Use a separate mapping contract. It maps source paths rather than CSV headers.

```yaml
mapping_contract:
  name: supplier-invoice-json-to-internal
  version: v1
  mappings:
    $.invoiceNo: invoice.invoice_number
    $.invoiceDate: invoice.invoice_date
    $.amount: invoice.total_amount
    $.currency: invoice.currency
```

For the first JSON invoice, the resulting internal record is the same as for CSV:

```json
{
  "invoice_number": "INV-1042",
  "invoice_date": "2026-09-15",
  "total_amount": 500.00,
  "currency": "USD",
  "lineage": {
    "invoice.total_amount": {
      "source_document": "doc_124",
      "source_path": "$.invoices[0].amount",
      "raw_value": "500.00",
      "mapping_contract_version": "v1"
    }
  }
}
```

### 6. Validate the internal record and store results

Run the same internal business validation as CSV: unique invoice number, supported currency, vendor resolution, and other business rules. Valid records are persisted as internal business records. Invalid records and their JSONPath-based errors remain in the canonical layer.

## Common source-contract pattern

CSV and JSON use the same concept with different source locators:

| Concept | CSV | JSON |
|---|---|---|
| Repeatable record | Row | Array item, such as `$.invoices[*]` |
| Source field identifier | Header, such as `Amount` | JSONPath, such as `$.amount` |
| Source location | Row + column | JSONPath, such as `$.invoices[0].amount` |
| Raw value | Cell string | JSON primitive or serialized representation |
| Mapping source | Header → internal field | Path → internal field |

## Final architecture

```text
source.csv or source.json
  -> Original-file storage
  -> Canonical records (raw value + typed value + source location + validation)
  -> Validated mapping contract
  -> Internal business records with field-level lineage
```

The canonical layer is the durable source representation. Internal tables are useful projections for applications and can be rebuilt from valid canonical data and versioned mapping rules.
