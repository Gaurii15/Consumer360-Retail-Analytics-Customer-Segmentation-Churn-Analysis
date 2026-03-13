# Consumer360 : Retail Analytics - Customer Segmentation & Churn Analysis

Consumer360 is an end-to-end retail analytics project designed to analyze customer purchasing behavior, identify high-value customer segments, and detect potential churn risks using RFM (Recency, Frequency, Monetary) analysis.

The project simulates a real-world retail analytics workflow by integrating SQL, Python, and Power BI, along with an automated ETL pipeline to deliver actionable business insights for decision-making.

## Business Problem

• Retail businesses often struggle to identify high-value customers and churn-risk segments.

• Lack of centralized analytics makes it difficult to understand customer purchasing behavior.

• Businesses need insights into regional sales performance, product demand, and customer engagement.

• Without proper segmentation, companies cannot design targeted marketing and retention strategies.

• A data-driven system is required to create a Customer 360° view for better business decisions.

## Project Objectives

• Build a Customer 360° analytical view

• Perform RFM-based customer segmentation

• Identify high-value customers and churn-risk segments

• Analyze sales performance across regions, categories, and products

• Develop interactive dashboards for business monitoring

• Automate the data pipeline for scheduled data refresh

## Key Insights

• Champion customers contribute the highest Customer Lifetime Value (CLV) and generate the largest share of revenue.

• At-Risk customers show higher recency values, indicating declining engagement and potential churn.

• West and South regions generate the highest total sales, indicating strong market demand in these regions.

• Electronics and Clothing categories contribute the most to overall revenue.

• A small percentage of customers contribute a significant portion of total revenue, demonstrating the Pareto (80/20) principle.

• Loyal customers maintain consistent purchasing behavior, making them important for long-term revenue stability.

• Some customers show low frequency and low monetary value, indicating low engagement with the business.

## Business Recommendations

• Launch targeted retention campaigns for At-Risk customers through personalized offers and email marketing.

• Introduce loyalty reward programs to retain Champion and Loyal customers.

• Increase marketing efforts in high-performing regions to further boost sales.

• Promote high-performing product categories like Electronics and Clothing with seasonal campaigns.

• Develop personalized product recommendations based on customer purchasing patterns.

• Use customer segmentation insights to create data-driven marketing strategies.

• Monitor churn-risk customers regularly to reduce revenue loss and improve customer retention.

## Tech Stack
Technology	Purpose
SQL	Data extraction, joins, aggregation
Python (Pandas, NumPy)	RFM analysis, CLV calculation, ETL automation
Power BI	Interactive dashboards and visualization
Windows Task Scheduler	Pipeline automation
CSV / Flat Files	Raw data simulation

## Data Pipeline Architecture

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

##  Dashboard Overview
⭐ Retail Analytics Dashboard

Provides a high-level business performance overview.

### Key metrics include:

• Total Orders
• Total Customers
• Total Sales
• Total Quantity Sold
• Average Order Value
• Key visual insights:
• Monthly Sales Trends
• Sales by Country & Region
• Product and Category Performance
• Region and Year filters for dynamic exploration

⭐ Customer Segmentation & Churn Dashboard

• Uses RFM analysis to classify customers into meaningful segments:
• Champions – High-value, frequent buyers
• Loyal Customers – Consistent purchasers
• At Risk – Previously valuable customers with declining engagement
• Others – Low engagement or new customers

### Additional analysis includes:

• Customer Lifetime Value (CLV)
• Churn Risk vs Active Customers
• Segment-wise Recency, Frequency, and Monetary value
• Segment revenue contribution

