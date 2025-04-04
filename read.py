import pandas as pd

df_paraquet = pd.read_parquet('output_data.parquet')
df_csv = pd.read_csv('data/veridion_product_deduplication_challenge.snappy.csv')
duplicates = df_paraquet[df_paraquet.duplicated(subset=['product_summary'], keep=False)]
duplicate_counts = duplicates['product_title'].value_counts()
print("Duplicate:")
print(duplicates)
print(duplicate_counts)


