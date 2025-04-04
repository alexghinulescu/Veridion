import pandas as pd
import numpy as np

df_paraquet = pd.read_parquet('data/veridion_product_deduplication_challenge.snappy.parquet')

df_test = df_paraquet.head(1000)


def aggregate_columns(series):
    if series.dtype == 'O':
        series = series.dropna().apply(lambda x: str(x) if isinstance(x, (dict, list, np.ndarray)) else x)
        return ', '.join(map(str, set(series)))
    else:
        return series.max()


df_new = df_test.groupby('product_title', as_index=False).aggregate(aggregate_columns)
df_new.to_parquet('output_data3.parquet', engine='pyarrow')


print(df_new)
print(df_test.dtypes)