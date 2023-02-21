import csv
import pandas as pd
from collections import Counter
import threading
from datetime import datetime

print("Getting Data")

#Get Data and Save as Pandas Dataframe
constituents = pd.read_csv('https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv',usecols = ['cons_id','source','create_dt','modified_dt'])
emails = pd.read_csv('https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv', usecols = ['email','cons_id','cons_email_id'])
subscriptions = pd.read_csv('https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv',usecols = ['cons_email_id','isunsub','chapter_id'])

print("Finished Getting Data")

#Get Data and Save as Pandas
temp_res = emails.merge(subscriptions, on='cons_email_id')
final_res =  temp_res.merge(constituents, on='cons_id')
final_res_filtered=final_res.loc[final_res['chapter_id']==1].copy(deep=True)
final_res_filtered.pop('chapter_id')
final_res_filtered.pop('cons_id')
final_res_filtered.pop('cons_email_id')
final_res_filtered.rename(columns={'isunsub': 'is_unsub'}, inplace=True)
final_res_filtered.rename(columns={'source': 'code'}, inplace=True)

final_res_filtered = final_res_filtered[['email', 'code', 'is_unsub', 'create_dt','modified_dt']]
final_res_filtered.to_csv('people.csv', encoding='utf-8', index=False)

print("Creating Acquisition CSV.")

field_names = ['acquisition_date', 'acquisitions']
acqusitions_holder = pd.to_datetime(final_res_filtered['create_dt']).dt.strftime('%a, %Y-%m-%d')
counter = Counter(acqusitions_holder.values)
final_acquisition_format=[]
for ele in counter:
    final_acquisition_format.append({'acquisition_date':ele, 'acquisitions':counter[ele]})
final_acquisition_format.sort(key=lambda x: datetime.strptime(x["acquisition_date"], '%a, %Y-%m-%d'))

print("Writing Acquisition Data.")

with open('acquisition_facts.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames = field_names)
    writer.writeheader()
    writer.writerows(final_acquisition_format)

print("Done.")