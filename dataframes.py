import pandas as pd
pd.options.display.max_columns = 99

loan_first_five = pd.read_csv('loans_2007.csv', nrows=5)
loan_first_five

loan_first_1000 = pd.read_csv('loans_2007.csv', nrows=1000)
print(loan_first_1000.memory_usage(deep=True).sum() / (1024*1024))

loan_inc_dec = pd.read_csv('loans_2007.csv', chunksize=3100)
total_mem = 0
total_len = 0
for loan_inc_dec_chunk in loan_inc_dec:
    mem = loan_inc_dec_chunk.memory_usage(deep=True).sum() / (1024*1024)
    #print(mem)
    total_len += (loan_inc_dec_chunk.shape[0])
    total_mem += mem
print("Total Memory = " + str(total_mem))
print("Total Rows = " + str(total_len))

loan_inc_dec = pd.read_csv('loans_2007.csv', chunksize=3100)
for loan_inc_dec_chunk in loan_inc_dec:
    print(loan_inc_dec_chunk.dtypes.value_counts())

loan_inc_dec = pd.read_csv('loans_2007.csv', chunksize=3100)
obj_cols = []
for loan_inc_dec_chunk in loan_inc_dec:
    loan_inc_dec_chunk_cols = loan_inc_dec_chunk.select_dtypes(include=['object']).columns.tolist()
    if(len(obj_cols) > 0):
        is_same = obj_cols == loan_inc_dec_chunk_cols
        if not is_same:
            print("Not Same Columns = ", loan_inc_dec_chunk_cols)
    else:
        obj_cols = loan_inc_dec_chunk_cols

used_cols = loan_first_five.columns.tolist()
used_cols.remove('id')
print("Used cols: ", used_cols)

loan_chunk_iter = pd.read_csv('loans_2007.csv', chunksize=3100, usecols=used_cols)
str_cols_vc = {}

for chunk in loan_chunk_iter:
    str_cols = chunk.select_dtypes(include=['object'])
    for col in str_cols.columns:
        current_col_vc = str_cols[col].value_counts()
        if col in str_cols_vc:
            str_cols_vc[col].append(current_col_vc)
        else:
            str_cols_vc[col] = [current_col_vc]
            
## Combine the value counts.
combined_vcs = {}

print("Cols with less than 50% unique values:")
cat_cols = []

for col in str_cols_vc:
    combined_vc = pd.concat(str_cols_vc[col])
    final_vc = combined_vc.groupby(combined_vc.index).sum()
    combined_vcs[col] = final_vc
    
    if len(final_vc)/total_len < 0.5:
        cat_cols.append(col)
        print(col)

loan_chunk_iter = pd.read_csv('loans_2007.csv', chunksize=3000, usecols=used_cols)
null_c = {}

for chunk in loan_chunk_iter:
    float_cols = chunk.select_dtypes(include=['float']).columns
    for c in float_cols:
        total_null_count = chunk[c].isnull().sum()
        if c not in null_c:
            null_c[c] = total_null_count
        else:
            null_c[c] += total_null_count

for col in cat_cols:
    print(col)
    print(combined_vcs[col])
    print("-----------")

cat_cols.remove('int_rate')
cat_cols.remove('revol_util')
convert_col_dtypes = {}

for col in cat_cols:
    convert_col_dtypes[col] = "category"

def clean_percent_symbol(row):
    if type(row['int_rate']) is str: 
        row['int_rate'] = row['int_rate'].replace("%","")
    
    if type(row['revol_util']) is str: 
        row['revol_util'] = row['revol_util'].replace("%","")
    return row

chunk_iter = pd.read_csv('loans_2007.csv', chunksize=3100, usecols=used_cols, dtype=convert_col_dtypes)
for chunk in chunk_iter:
    print(chunk.dtypes)
    break

chunk_iter = pd.read_csv('loans_2007.csv', chunksize=3100, usecols=used_cols, dtype=convert_col_dtypes,
                        parse_dates=["issue_d", "earliest_cr_line", "last_pymnt_d", "last_credit_pull_d"])
total_len = 0
total_mem = 0
for chunk in chunk_iter:
    chunk = chunk.apply(clean_percent_symbol, axis=1)
    chunk.term = chunk.term.str.lstrip(" ").str.rstrip(" months")
    
    chunk.int_rate = pd.to_numeric(chunk.int_rate)
    chunk.revol_util = pd.to_numeric(chunk.revol_util)
    chunk.term = pd.to_numeric(chunk.term)
    
    mem = chunk.memory_usage(deep=True).sum()/(1024*1024)
    total_len += chunk.shape[0]
    total_mem += mem
    
print("Total Rows: ", total_len)
print("Total Memory: ", total_mem)
