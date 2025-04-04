import pandas as pd

df_paraquet = pd.read_parquet('data/veridion_product_deduplication_challenge.snappy.parquet')

test = df_paraquet[df_paraquet['product_title'] == 'Complete Grain Free Pate with Salmon for Adult Cat']
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
print(test)

