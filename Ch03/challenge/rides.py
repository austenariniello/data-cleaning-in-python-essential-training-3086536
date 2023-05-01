# %%
# Find out all the rows that have bad values
import pandas as pd

df = pd.read_csv('rides.csv')
df
#%%
#Missing values are not allowed
null_mask = df.isnull().any(axis=1)
df[null_mask]

#%%
#A plate must be a combination of at least 3 upper case letters or digits
plate_mask = ~df['plate'].str.match(r'^[0-9A-Z]{3,}', na=False)
df[plate_mask]

#%%
#Distance much be bigger than 0
dist_mask = ~(df['distance'] > 0)
df[dist_mask]

#%%
#Combine masks to find all invalid data
mask = null_mask | plate_mask | dist_mask
df[mask]

#%%
import pandera as pa

schema = pa.DataFrameSchema(
    {
      "name": pa.Column(str),
      "plate": pa.Column(str, [
          pa.Check.str_length(3),
          pa.Check(lambda s: s.str.isupper())
      ]),
      "distance": pa.Column(float, pa.Check.gt(0)),
    })

schema.validate(df)

# %%
