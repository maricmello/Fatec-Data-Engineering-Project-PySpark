# 🛍️ Fashion Retail Sales - Analysis with Microsoft Fabric & Power BI

This project focuses on cleaning, modeling, and analyzing retail sales data from the [Fashion Retail Sales dataset](https://www.kaggle.com/datasets/atharvasoundankar/fashion-retail-sales), using a multi-layer architecture (**Bronze**, **Silver**, and **Gold**) in **Microsoft Fabric** (Spark) and building visual insights with **Power BI**.

---

## 🗂️ Layered Architecture

### 🥉 Bronze Layer
- **Purpose:** Raw data ingestion.
- **Transformations:** None — just reading and saving the file in Parquet format.

### 🥈 Silver Layer
- **Purpose:** Data cleansing and standardization.
- **Key transformations:**
  - Renamed columns to Portuguese.
  - Cast data types (e.g., dates, integers, decimals).
  - Replaced null values with default values.
  - Added a column with the **month name** (`nome_mes`).

### 🥇 Gold Layer
- **Purpose:** Dimensional modeling for business analysis.
- **Created tables:**
  - `dim_cliente` (Customer Dimension)
  - `dim_item` (Item Dimension)
  - `dim_tempo` (Date Dimension, including year, month, day, and month name)
  - `dim_pagamento` (Payment Method Dimension)
  - `fato_vendas` (Fact Table with purchases)

---

## 📊 Power BI Dashboard

Power BI visuals include:
- Sales by month and year
- Average ticket (purchase value)
- Review ratings distribution
- Top selling products
- Payment method breakdown
- Time series trends
- Monthly performance with translated month names

### Example DAX Measures:
```DAX
Total Sales = SUM('fato_vendas'[valor_compra])

Total Items = COUNT('fato_vendas'[id_cliente])

Average Ticket = DIVIDE([Total Sales], [Total Items])

Cash Payments = CALCULATE([Total Items], 'dim_pagamento'[forma_pagamento] = "Cash")

Credit Card Payments = CALCULATE([Total Items], 'dim_pagamento'[forma_pagamento] = "Credit Card")
