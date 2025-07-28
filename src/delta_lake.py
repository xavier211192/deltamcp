import polars as pl
from deltalake import DeltaTable
data = {
    'product_code' : ['0001', '0002', '0003', '0004'],
    'color' : ['red', 'green','blue','yellow'],
    'size': ['small','medium','large','x-large']
}

df = pl.DataFrame(data).with_columns(
    [
        pl.lit(True).alias('is_current'),
    ]
)
table_path = "src/data/deltalake/product"
# Create the delta table for testing
df.write_delta(table_path, mode='overwrite')

##Reading
table_path = "src/data/deltalake/product"
df = pl.read_delta(table_path)
# print(df)


##History
# print history returns a list of transactions on the table
# turn this into a dataframe using polars
dt = DeltaTable(table_path)
hist = pl.DataFrame(dt.history())
# print(hist)

##Optimize
dt.optimize.compact()
dt = DeltaTable(table_path)
hist = pl.DataFrame(dt.history())
print(hist)

print(dt.vacuum())
# dt.vacuum(dry_run=False)