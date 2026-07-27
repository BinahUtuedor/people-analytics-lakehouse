"""
Data quality package.

The functions of the modules in this package follow the same pattern:

- they accept a SQLAlchemy session;
- they query the database directly;
- they return records that violate the validation rule;
- they do not write reports themselves.

The main quality.validation module is responsible for converting the
returned records into ValidationResult objects and exporting them to:

    quality_reports/validation_report.csv
    quality_reports/validation_report.json

"""