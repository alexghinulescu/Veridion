# Product Deduplication Challenge
The project goal is to read data from a parquet file and identify the duplicates. The dataset contains product details extracted from various web pages which resulted in multiple entries of the same product wits varied data.
The challenge consists in merging these duplicates into a single entry.

First, I inspected the dataset and found the duplicates based on product_title. Some contaoned lists with values collected from those web pages, which were sometimes identical and sometimes different.

To address this problem, I attempted to read the file and find the number of duplicates per product_title using pandas. I used groupby and aggregate and I had a result, but not as intended. Not all values were retained. 

For example, for the product_title 'Ferris Wheel Press EU Fountain Pen Ink', the intended industries were: 

- [Writing, Art]
- [Writing]
- [Writing]
- [Writing, Art]
- [Writing, Art]
- [Writing & Art]
- [Writing, Art]
- [Writing Accessories]
- [Writing, Office Supplies]
  
and after the execution, only [Writing, Art] was kept.

I explored more options and found pyspark as a better solution.  

In the next test, I used groupBy and collect_set in order to merge the rows, but I lost columns in the output. 

I analyzed the dataset and found the structured fields, so I tried another different approach. 

I tried concatenating the complex fields and used array_distinct for the duplicates. I aggregated the columns using unite and grouped the data by the product_title. 

Finally, it was saved under the no_duplicate_output.parquet folder. 

It was the first time I handled parquet files! I used my data analysis experience, the python university course and online resources, to successfully complete the challenge. 
