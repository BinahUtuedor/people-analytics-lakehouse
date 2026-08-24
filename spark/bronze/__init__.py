"""
Bronze-layer processing package.

The Bronze layer is responsible for converting validated Raw datasets
into governed lakehouse datasets while preserving source fidelity.

Implemented local foundation:

    S3 Raw
        ↓
    Bronze reader
        ↓
    Bronze transformation
        ↓
    Bronze validation and reconciliation
        ↓
    Duplicate-safe Bronze writer
"""
