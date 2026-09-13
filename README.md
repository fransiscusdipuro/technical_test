# Technical Design & Pipeline Documentation: HDB Resale Flat Data Pipeline

# PART1

[Folder](technical_test\part1\)
[Jupyter notebook file](technical_test\part1\part1.ipynb)

## 1. Executive Summary
This ETL pipeline standardizes, validates, and transforms HDB resale flat transaction data from January 2012 to December 2016. It guarantees data consistency across schema variances across multiple reporting periods and produces an analytical dataset ready for Data Science consumption.

---

## 2. Ingestion & Filtering Mechanics
- **Temporal Filter Scope**: Data is filtered strictly between `2012-01` and `2016-12` using parsed `YYYY-MM` timestamps.
- **Schema Harmonization**: Column headers are dynamically converted to lowercase, trimmed, and unified across disparate CSV structure versions (e.g., handling missing or pre-formatted `remaining_lease` columns).

---

## 3. Data Validation & Baseline Rules
Using `Jan 2012` as the authoritative baseline set:
- **Town, Flat Type, Flat Model, Storey Range**: Standardized via uppercase transformations and stripped of whitespace before comparative evaluation.
- **Handling Schema Drift**: Any new flat models or storey range schemas introduced post-2012 (e.g., higher storey ranges like `36 TO 40`) are flagged programmatically via delta profiling without breaking downstream parsing.

---

## 4. Derived Attribute Logic (Remaining Lease)
- **Lease Assumption**: 99-year total lease length.
- **Calculation Formula**:
$$\text{Expiry Year} = \text{lease commence date} + 99$$
$$\text{Remaining Months} = (\text{Expiry Year} \times 12) - \text{Transaction Date Total Months}$$
$$\text{Formatted Lease} = \lfloor\text{Remaining Months} / 12\rfloor \text{ years } + (\text{Remaining Months} \pmod{12}) \text{ months}$$

---

## 5. Conflict Resolution Strategy
- **Composite Key Definition**: All attributes combined **excluding** `resale_price` (i.e., `[month, town, flat_type, block, street_name, storey_range, floor_area_sqm, flat_model, lease_commence_date]`).
- **Deduplication Heuristic**: When duplicates are detected across non-price columns, the record with the **maximum `resale_price`** is retained, resolving potential transaction adjustments or input errors.

---

## 6. Anomaly Detection Architecture
To satisfy data quality verification where clean datasets may lack extreme anomalies, a dual-layer statistical approach is embedded:
1. **IQR Outlier Detection**: Applied within localized groupings (`town` + `flat_type`) to capture region-specific pricing anomalies ($1.5 \times \text{IQR}$).
2. **Z-Score Normalization**: Evaluates `price_per_sqm` globally; transactions exceeding $|Z| > 3.0$ standard deviations are flagged for manual domain review.

---

## 7. Baseline Insights & Data Findings
- **Missing Attributes Across Cohorts**: Source files prior to 2015 lack the `remaining_lease`.
- **Schema Drift Observations**: Pinnacle@Duxton and other modern high-rises introduced higher storey ranges (e.g., `36 TO 40`, `46 TO 48`) and new models (`DBSS`, `Type S1`, `Type S2`) post-2012.
- **Duplicate Volume**: Approximately ~1.7% (~1,600 entries) of raw records shared duplicate composite non-price keys and were deduplicated by retaining the maximum resale price.

---

## 8. Resale Identifier Generation Architecture
To create a unique transaction tracking key across historical runs, an unhashed `resale_identifier` is derived using a deterministic 5-component rule set:

1. **Static Prefix**: Character `S`.
2. **Block Component**: First 3 numerical digits of the `block` attribute after stripping non-numeric characters. If under 3 digits, left-zero-padded (e.g., block `19` $\rightarrow$ `019`).
3. **Price Grouping Prefix**: 1st and 2nd digits of the average resale price calculated over the transaction's `[month, town, flat_type]` grouping.
4. **Month Component**: 2-digit transaction month extracted from `month` (e.g., `2012-01` $\rightarrow$ `01`).
5. **Town Suffix**: First character of the standardized `town` attribute (e.g., `ANG MO KIO` $\rightarrow$ `A`).

*Example Output*: Block `19A`, Avg Group Price `$234,500`, Month `2012-01`, Town `ANG MO KIO` $\rightarrow$ **`S0192301A`**.

---

## 9. Cryptographic Hashing & Uniqueness Strategy
- **Security Requirement**: To protect proprietary transaction references while maintaining global consistency, the `resale_identifier` is hashed using the **SHA-256** algorithm.
- **Uniqueness Preservation**: Because SHA-256 is deterministic and collision-resistant, a $1:1$ bijection is preserved between the unhashed identifier string and the resulting 64-character hexadecimal digest string.
- **Verification Protocol**: Hash uniqueness is verified programmatically via set cardinality checks:
$$\text{Count}(\text{Unique Unhashed Identifiers}) == \text{Count}(\text{Unique Hashed Identifiers})$$

---

## 10. Mandatory Output Group Management & Pipeline Flow
The pipeline structures data into **five mandatory output groups** exported directly as standardized CSV files:

| Output Group | Target CSV Name | Description & Ingestion Criteria |
| :--- | :--- | :--- |
| **1. Raw** | `01_raw_dataset.csv` | Unmodified source data combined across input files, scoped strictly between `2012-01` and `2016-12`. |
| **2. Cleaned** | `02_cleaned_dataset.csv` | Validated master records passing baseline checks, deduplicated (retaining higher price records), with derived lease attributes added. |
| **3. Transformed** | `03_transformed_dataset.csv` | Enriched dataset containing the derived `avg_group_resale_price` and unhashed `resale_identifier`. |
| **4. Quarantined** | `04_quarantined_dataset.csv` | Isolated records failed or excluded during processing, including lower-price duplicate transactions and statistical price anomalies (Grouped IQR outliers). |
| **5. Hashed** | `05_hashed_dataset.csv` | Final analytical delivery dataset where the `resale_identifier` is securely encrypted. |

# PART 2: Architecture & System Design

[Folder](technical_test\part2\)

## 1. Executive Summary
Part 2 outlines a cloud-native, enterprise-grade data platform on Amazon Web Services (AWS) designed to automate the ingestion, transformation, storage from public datasets (data.gov.sg).

The architecture emphasizes **serverless scalability**, **strict security and privacy controls**, and **minimal infrastructure cost/overhead** by leveraging AWS Glue Python Shell, S3 storage tiers, Glue Data Catalog, and Amazon Athena routed privately via AWS VPC Endpoints.

Logs and monitoring by Amazon CloudTrail and CloudWatch. 

---

## 2. High-Level System Architecture Diagram
[Architecture Diagram](part2/aws_arch.drawio.png)