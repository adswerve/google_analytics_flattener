"""deletes all flat tables from dataset
purpose: clean up after a unit test
USE WITH CAUTION
Credit: https://stackoverflow.com/questions/52151185/bigquery-best-way-to-drop-date-sharded-tables
"""


from google.cloud import bigquery

'''*****************************'''
''' Configuration Section Start '''
'''*****************************'''
my_project_id = 'as-dev-ruslan'
my_dataset_id = '24973611'
delete = True # False to just discover flat tables
'''*****************************'''
'''  Configuration Section End  '''
'''*****************************'''

client = bigquery.Client(project=my_project_id)

dataset_ref = client.dataset(my_dataset_id)

tables = list(client.list_tables(dataset_ref))  # API request(s), now you have the list of tables in this dataset
tables_to_delete=[]
print("discovered flat tables:")
for table in tables:
    if table.table_id.startswith("ga_flat"): #will perform the action only if the table has the desired prefix
        tables_to_delete.append(table.table_id)
        print(table.full_table_id)
print("\n")
if delete:
    for table_id in tables_to_delete:
        table_ref = client.dataset(my_dataset_id).table(table_id)
        client.delete_table(table_ref)
        print("deleted table %s.%s.%s" % (table_ref.project,  table_ref.dataset_id,  table_ref.table_id))