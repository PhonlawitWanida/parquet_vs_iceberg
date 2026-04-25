# Apache Iceberg with PySpark

This project demonstrates Apache Iceberg operations using PySpark and compares it with traditional Parquet storage.

---

## Project Structure

```bash
parquet_vs_iceberg/
│
├── scripts/
│   ├── basic_iceberg.py
│   └── parquet_vs_iceberg.py
│
├── raw/
│   └── healthcare_dataset.csv
│
├── images/
│   └── output.png
│
├── .gitignore
└── README.md
```
1) basic_iceberg.py

This script demonstrates basic Apache Iceberg operations using PySpark.

Features
Create Iceberg table
Insert data
Query data
Delete data
Show final result
Run
python scripts/basic_iceberg.py
<img width="817" height="742" alt="Screenshot 2026-04-25 223443" src="https://github.com/user-attachments/assets/a88f1344-fa3e-4e8c-956a-a621541ea4f1" />

2) parquet_vs_iceberg.py

This script compares Parquet and Iceberg using the same healthcare dataset.

Parquet Workflow
Initial write
Append data
Delete data (requires full rewrite)
Iceberg Workflow
Create table
Append data
Delete data using SQL
Run
python scripts/parquet_vs_iceberg.py
<img width="1314" height="809" alt="Screenshot 2026-04-25 224410" src="https://github.com/user-attachments/assets/b4bae700-b8ae-475a-b1b9-ecf6cb1059d5" />

Dataset

Healthcare dataset

Total rows:

55,500 rows

Columns include:

Name
Age
Gender
Blood Type
Medical Condition
Doctor
Hospital
Billing Amount
Medication
Test Results
Output Example
Parquet vs Iceberg Result

Technologies Used
Python
PySpark
Apache Iceberg
Apache Parquet
Docker
Git/GitHub
Key Difference
Feature	Parquet	Iceberg
Append	Yes	Yes
Delete	Rewrite required	Direct SQL delete
Time Travel	No	Yes
Metadata	Manual	Automatic
ACID Support	No	Yes
Why Apache Iceberg?

Apache Iceberg solves many limitations of traditional data lakes by providing:

ACID transactions
Time travel
Better schema evolution
Easier delete/update operations
Better metadata handling

Author
Phonlawit Wanida
