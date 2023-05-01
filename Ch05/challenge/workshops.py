# %%
import re
import pandas as pd

df = pd.read_csv('workshops.csv')
df
df.dtypes
# %%
"""
Fix the data frame. At the end, row should have the following columns:
- start: pd.Timestemap
- end: pd.Timestamp
- name: str
- topic: str (python or go)
- earnings: np.float64
"""

# %%
# Fix year dtype and add to all rows
df['Year'].fillna(df['Year'].mode()[0], inplace=True)

df['Year'] = df['Year'].apply(int)
df

# %%
# front fill month column
df['Month'].fillna(method='ffill', inplace=True)
df

# %%
# remove rows with missing values
df.dropna(inplace=True)
df

# %%
# fix start/end/earnings dtypes

df['Start'] = df['Start'].apply(int)
df['End'] = df['End'].apply(int)
df['Earnings'] = df['Earnings'].apply(
    lambda s: float(re.sub('[^0-9]', '', s))
)
df

# %%
# create a topic column based on class name


def course_topic(course_name):
    s = course_name.lower()

    if 'go' in s:
        return 'go'
    elif 'python' in s:
        return 'python'
    else:
        return 'none'


df['topic'] = df['Name'].apply(course_topic)
df

# %%
# make dataframe to format the start and end date strings
date_str = pd.DataFrame()

date_str['start_str'] = df['Month'].str.cat(
    [df['Start'].apply(str), df['Year'].apply(str)], sep='/')
date_str['end_str'] = df['Month'].str.cat(
    [df['End'].apply(str), df['Year'].apply(str)], sep='/')

date_str

# %%
# convert strings to timestamp objects

date_str['start_datetime'] = pd.to_datetime(
    date_str['start_str'], format='%B/%d/%Y')
date_str['end_datetime'] = pd.to_datetime(
    date_str['end_str'], format='%B/%d/%Y')

date_str.dtypes

# %%
# add datetime object columns to main df
df['Start'] = date_str['start_datetime']
df['End'] = date_str['end_datetime']

df

# %%
# reformat dataframe
df.rename(columns={
    'Start': 'start',
    'End': 'end',
    'Name': 'name',
    'Earnings': 'earnings',
}, inplace=True)

df = df[['start', 'end', 'name', 'topic', 'earnings']]

df.reset_index(drop=True, inplace=True)

df

# %%
# print datatypes

df.info()
# %%
