"""
Bronze-layer processing package.

The Bronze layer is responsible for converting validated Raw datasets
into governed lakehouse datasets while preserving source fidelity.

Current implementation stage:

    S3 Raw
        ↓
    Bronze reader
        ↓
    Spark DataFrame

Future components:

    transformer.py
    writer.py
    processor.py
"""
