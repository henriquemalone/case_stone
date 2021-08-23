import os
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\teste\\data-case-study-322621-dd48c388547e.json"
bigquery_client = bigquery.Client()

def bq_create_table():
    dataset_ref = bigquery_client.dataset('henriquemalone')

    # Prepares a reference to the table
    table_ref = dataset_ref.table('crypto_currency')

    try:
        bigquery_client.get_table(table_ref)
        return 'false'
    except:
        schema = [
            bigquery.SchemaField('id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('symbol', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('snapshot_date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('current_price_usd', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('current_price_eur', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('current_price_brl', 'FLOAT', mode='REQUIRED'),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="snapshot_date")
        table = bigquery_client.create_table(table)
        result = 'table {} created.'.format(table.table_id)

        return 'true'

def insertDB(id, symbol, name, date, usd, eur, brl):
    # # TODO(developer): Set table_id to the ID of table to append to.
    table_id = "data-case-study-322621.henriquemalone.crypto_currency"

    rows_to_insert = [
        {u"id": id, 
        u"symbol": symbol, 
        u"name": name, 
        u"snapshot_date": date, 
        u"current_price_usd": usd, 
        u"current_price_eur": eur, 
        u"current_price_brl": brl}
    ]

    errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

#Confere se a tabela já esta atualizada com as informações do dia
def confDate(cdate):
    query_job = bigquery_client.query(
        """SELECT snapshot_date FROM `data-case-study-322621.henriquemalone.crypto_currency` 
        ORDER BY snapshot_date DESC LIMIT 2"""
    )

    results = query_job.result()  # Waits for job to complete.

    for row in results:
        if(str(row.snapshot_date) == str(cdate)):
            return 'true'
        else:
            return 'false'

# def deleteTable():
# Construct a BigQuery client object.
# bigquery_client = bigquery.Client()

# # TODO(developer): Set table_id to the ID of the table to fetch.
# table_id = 'data-case-study-322621.henriquemalone.crypto_currency'

# # If the table does not exist, delete_table raises
# # google.api_core.exceptions.NotFound unless not_found_ok is True.
# bigquery_client.delete_table(table_id, not_found_ok=True)  # Make an API request.
# print("Deleted table '{}'.".format(table_id))