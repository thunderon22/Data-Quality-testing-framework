# importing necessary libraries
from datasets import load_dataset
import pandas as pd
# loading the datasets and converting to pandas dataframe
squad_v1 = load_dataset("squad")
squad_v2 = load_dataset("squad_v2")
df_v1 = squad_v1["train"].to_pandas()
df_v2 = squad_v2["train"].to_pandas()
# sampling the datasets and saving to csv   
df_v1 = df_v1.sample(2000, random_state=42)
df_v2 = df_v2.sample(2000, random_state=42)
# saving the datasets to csv
df_v1.to_csv("df_v1.csv", index=False)
df_v2.to_csv("df_v2.csv", index=False)