import pandas as pd  

part_1 = pd.read_csv("ard_details.csv")
part_2 = pd.read_csv("kerala_fps.csv")
merged_df = pd.merge(part_1, part_2, left_on='ard_id', right_on='ARD', how='left')
columns_to_drop = ['Sl.No', 'ARD', 'Total Cards', 'License', 'Owner Name', 'Mobile', 'Status', 'AFSO Name', 'District Name']
merged_df.drop(columns=columns_to_drop, inplace=True)
merged_df.to_csv("merged_data_kerala1.csv", index=False)
