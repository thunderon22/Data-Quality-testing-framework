import pandas as pd

df = pd.read_csv("Datasets/dirty_cafe_sales.csv")

# print(df.shape)          # how many rows and columns
# print(df.dtypes)         # what types pandas detected
# print(df.columns.tolist())  # exact column names
# print(df.head(3))        # first 3 rows
print(df.isnull().sum())
