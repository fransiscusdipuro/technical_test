# Technical Design & Pipeline Documentation: HDB Resale Flat Data Pipeline

# PART1

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
$$\text{Expiry Year} = \text{lease\_commence\_date} + 99$$
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