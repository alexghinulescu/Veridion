import pyarrow.parquet as pq
import pandas as pd

df_paraquet = pd.read_parquet('data/veridion_product_deduplication_challenge.snappy.parquet')
df_csv = pd.read_csv('data/veridion_product_deduplication_challenge.snappy.csv')
complete_grain_free = df_paraquet[df_paraquet['product_title'] == 'Complete Grain Free Pate with Salmon for Adult Cat']
pq.read_table(complete_grain_free)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

print("Produse cu titlul 'Complete Grain Free Pate with Salmon for Adult Cat':")
print(complete_grain_free)
