import pandas as pd
import numpy as np
from datetime import timedelta

df = pd.read_csv("Raw Amazon dataset.csv")

df.head()
print(df.columns)

df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
print(df.columns)

df['orderdate'] = pd.to_datetime(df['orderdate'])

df['revenue'] = df['quantity'] * df['unitprice']

rfm = df.groupby('customerid').agg({
    'orderdate': 'max',      # Last purchase date
    'orderid': 'nunique',    # Frequency
    'revenue': 'sum'          # Monetary
}).reset_index()

rfm.columns = ['customer_id', 'last_purchase_date', 'frequency', 'monetary']

analysis_date = rfm['last_purchase_date'].max() + pd.Timedelta(days=1)

rfm['recency'] = (analysis_date - rfm['last_purchase_date']).dt.days


rfm.head()
rfm.describe()

rfm['r_score'] = pd.qcut(rfm['recency'], 5, labels=[5,4,3,2,1])
rfm['f_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=[1,2,3,4,5])
rfm['m_score'] = pd.qcut(rfm['monetary'], 5, labels=[1,2,3,4,5])

rfm['r_score'] = rfm['r_score'].astype(int)
rfm['f_score'] = rfm['f_score'].astype(int)
rfm['m_score'] = rfm['m_score'].astype(int)

def segment_customer(row):
    if row['r_score'] >= 4 and row['f_score'] >= 4 and row['m_score'] >= 4:
        return 'Champions'
    elif row['r_score'] >= 3 and row['f_score'] >= 3:
        return 'Loyal Customers'
    elif row['r_score'] <= 2 and row['f_score'] >= 3:
        return 'At Risk'
    else:
        return 'Others'

rfm['segment'] = rfm.apply(segment_customer, axis=1)

rfm.groupby('segment')['monetary'].mean().sort_values(ascending=False)

rfm['clv'] = rfm['monetary'] * rfm['frequency']

# Create overall RFM score (IMPORTANT for Power BI)
rfm['rfm_score'] = (
    rfm['r_score'] +
    rfm['f_score'] +
    rfm['m_score']
)



rfm.to_csv("rfm_output.csv", index=False)

rfm['segment'].value_counts()

import psycopg2
import pandas as pd

# rfm.to_sql(
#     "rfm_output",
#     con=engine,
#     if_exists="replace",
#     index=False
# )

from datetime import datetime

with open("automation_log.txt", "a") as f:
    f.write(f"Pipeline ran successfully at {datetime.now()}\n")

rfm['run_time'] = datetime.now()

