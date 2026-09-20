# Gold Phase 5 validation — `fact_attendance`

Phase 5 implements the approved restricted Gold attendance fact in
`spark/gold/attendance.py`. The source contract is Silver `attendance`: one
row per employee work date, identified by `attendance_id`, with governed
statuses `Present`, `Remote`, `Hybrid`, `Training`, `Business Travel`, and
`Absent`. The exact source columns are `attendance_id BIGINT NOT NULL`,
`employee_id BIGINT NOT NULL`, `work_date DATE NOT NULL`, `status STRING NOT
NULL`, nullable clock-in/out times, `hours_worked DECIMAL(5,2) NOT NULL`,
`overtime_hours DECIMAL(5,2) NOT NULL`, nullable `absence_reason STRING(100)`,
and Silver `_batch_id`. `absence_reason` is a controlled category and is
required only for `Absent`; it is rejected for other statuses.

The Gold grain is one legitimate Silver record. `attendance_id` is the primary
key and `(employee_key, work_date_key)` is also unique because the simulator
and source contract guarantee one daily record. `work_date_key` is the
calendar date of `work_date`; date values and midnight timestamps are accepted,
while time-bearing timestamps are rejected. Records must satisfy hire date,
termination date (when present), and `source_cutoff` bounds. The extended
calendar reference does not extend business coverage: a 2024-04-15 cutoff may
contain a 2024-04-30 `dim_date` row, but attendance on April 16 or April 30 is
rejected.

Assignment is resolved from supplied historical `dim_employee_assignment` at
`work_date`. Current employee organisation is not used for historical
attribution. Missing, overlapping, invalid, or unresolved required references
fail validation. `Absent` alone contributes `absent_day_count = 1`; every
accepted source row contributes `recorded_day_count = 1`. Leave requests are
not joined and no attendance rows are manufactured from leave data.

`hours_worked` and `overtime_hours` are `DECIMAL(18,4)`, converted through
decimal text with finite, lossless, non-negative and overflow checks. Zero is
valid. Metadata and canonical record hashes use the shared deterministic Gold
build context. Output is partitioned by `reporting_year` under the immutable
year/build layout used by the other Gold facts.

The controlled fixture reconciles 7 source rows and 7 distinct attendance IDs
to 7 Gold rows and 7 distinct IDs, with zero missing, unexpected, or duplicate
keys. Hours, overtime, recorded-day, and absent-day measures reconcile exactly.
It covers leap day, year boundaries, cutoff and employment boundaries,
historical assignment changes, unknown manager handling, status/reason rules,
decimal precision, duplicate keys, invalid references, and deterministic
reordering. Physical tests cover schema/nullability, decimal types, year
partitions, hashes/metadata, immutable writes, invalid pre-write rejection,
corrupt readback, and partition inventory. The final focused attendance suite
passed 8/8, including 2 physical tests. The complete Gold suite passed 173/173.

The earlier Gold module names `test_phase2.py` and `test_phase3.py` were renamed
to `test_assignment_history.py` and `test_workforce_monthly.py` to match the
implemented Gold models; behaviour and coverage are unchanged.

Governance review confirms no names, email addresses, DOB, gender, home or
bank details, tax/national identifiers, unrestricted or medical narrative, or
leave-request text is projected. Attendance status and controlled absence
categories are approved restricted analytical fields. Hashes are integrity
identifiers, not anonymisation.

Final terminal evidence: complete Gold **173/173** (exit 0), main Linux Spark
**214/214** (exit 0), local EMR compatibility **214/214** (exit 0), and safe
Bronze/Silver **41/41** (exit 0). The EMR-compatible runtime is Python
3.11.13, Java 17.0.16, and PySpark 3.5.6. No AWS or S3 access is part of this
validation.
