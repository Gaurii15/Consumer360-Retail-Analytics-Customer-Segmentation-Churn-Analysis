# Consumer360 – Retail Analytics & Customer Segmentation

Consumer360 is an end-to-end retail analytics project designed to analyze customer purchasing behavior, identify high-value customer segments, and detect potential churn risks using RFM (Recency, Frequency, Monetary) analysis.

The project simulates a real-world retail analytics workflow by integrating SQL, Python, and Power BI, along with an automated ETL pipeline to deliver actionable business insights for decision-making.

Business Problem

Retail companies often struggle to answer key business questions such as:

Which customers generate the most revenue?

Which customers are at risk of churn?

Which regions and product categories drive sales growth?

How can the business improve customer retention and maximize lifetime value?

Without structured analytics, businesses cannot effectively identify high-value customers or prevent revenue loss from churn.

This project builds a Customer 360° analytics system to help retail decision-makers understand customer behavior and optimize marketing and retention strategies.

Project Objectives

Build a Customer 360° analytical view

Perform RFM-based customer segmentation

Identify high-value customers and churn-risk segments

Analyze sales performance across regions, categories, and products

Develop interactive dashboards for business monitoring

Automate the data pipeline for scheduled data refresh

Tech Stack
Technology	Purpose
SQL	Data extraction, joins, aggregation
Python (Pandas, NumPy)	RFM analysis, CLV calculation, ETL automation
Power BI	Interactive dashboards and visualization
Windows Task Scheduler	Pipeline automation
CSV / Flat Files	Raw data simulation
Data Pipeline Architecture

Raw Retail Dataset
↓
SQL Data Cleaning & Aggregation
↓
Python RFM & Customer Analytics
↓
Automated CSV Data Output
↓
Power BI Dashboard Visualization

This pipeline simulates a real-world analytics workflow used in retail companies.

Dashboard Overview
1️⃣ Retail Analytics Dashboard

Provides a high-level business performance overview.

Key metrics include:

Total Orders

Total Customers

Total Sales

Total Quantity Sold

Average Order Value

Key visual insights:

Monthly Sales Trends

Sales by Country & Region

Product and Category Performance

Region and Year filters for dynamic exploration

2️⃣ Customer Segmentation & Churn Dashboard

Uses RFM analysis to classify customers into meaningful segments:

Champions – High-value, frequent buyers

Loyal Customers – Consistent purchasers

At Risk – Previously valuable customers with declining engagement

Others – Low engagement or new customers

Additional analysis includes:

Customer Lifetime Value (CLV)

Churn Risk vs Active Customers

Segment-wise Recency, Frequency, and Monetary value

Segment revenue contribution

Key Business Insights

Champion customers generate the highest revenue and CLV, indicating strong loyalty and engagement.

At Risk customers show increasing recency values, suggesting declining engagement and potential churn.

West and South regions contribute the highest sales, indicating strong regional demand.

Electronics and Clothing categories dominate revenue generation.

A small percentage of customers contribute a large share of revenue, demonstrating the Pareto Principle (80/20 rule) common in retail businesses.

Business Recommendations

Based on the analysis, the following strategic actions are recommended:

1. Retention Campaigns

Launch targeted promotions for At Risk customers to reduce churn.

2. Loyalty Programs

Reward Champion and Loyal customers with exclusive offers to maintain engagement.

3. Regional Expansion

Expand marketing efforts in high-performing regions to maximize sales potential.

4. Product Strategy

Increase inventory and promotions for high-performing categories like Electronics and Clothing.

5. Personalized Marketing

Use RFM segments to design personalized marketing campaigns and improve conversion rates.

Automation

The project includes an automated ETL pipeline:

Python scripts perform data processing and RFM calculation

Windows Task Scheduler triggers the ETL pipeline

Updated outputs are automatically reflected in Power BI dashboards

This ensures continuous data updates without manual intervention.

Skills Demonstrated

Data Cleaning & Transformation

SQL Data Analysis

Python ETL Automation

Customer Segmentation (RFM)

Churn Analysis

Power BI Data Modeling & DAX

Business Insight Generation

Internship Acknowledgement

This project was developed as part of my Data Analytics Internship at Infotact Solutions, where I worked on solving real-world retail analytics problems using modern data analysis tools.
