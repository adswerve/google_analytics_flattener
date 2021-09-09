from google.cloud import pubsub_v1
import json
import datetime, time

# To authenticate, run the following command.  The account you choose will execute this python script
# gcloud auth application-default login

'''*****************************'''
''' Configuration Section Start '''
'''*****************************'''
topic_id =  "adswervegaflat-topic"  # pubsub topic your cloud function is subscribed to Example: [Deployment Name]-topic
project_id = "newellbrandsga360"  # GCP project ID, example:  [PROJECT_ID]
dry_run = False   # set to False to Backfill.  Setting to True will not pubish any messages to pubsub, but simply show what would have been published.
# Desired dates to backfill, both start and end are inclusive
backfill_range_start = datetime.datetime(2019, 1, 21)
backfill_range_end = datetime.datetime(2021, 7, 28)  # datetime.datetime.today()
datasets_to_backfill = ["82589673"]     #GA Views to backfill, "24973611"
'''*****************************'''
'''  Configuration Section End  '''
'''*****************************'''
#Seconds to sleep between each property date shard
SLEEP_TIME = 5  # throttling

num_days_in_backfill_range = int((backfill_range_end - backfill_range_start).days) + 1
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


for db in range(0, num_days_in_backfill_range):
    date_shard = (backfill_range_end - datetime.timedelta(days=db)).strftime('%Y%m%d')
    for dataset_id in datasets_to_backfill:
        SAMPLE_LOAD_DATA = {"protoPayload": {
            "serviceData": {"jobCompletedEvent": {"job": {"jobConfiguration": {"load": {"destinationTable": {
                "datasetId": dataset_id
                , "projectId": project_id
                , "tableId": "ga_sessions_%s" % date_shard
            }}}}}}}}

        print('Publishing backfill message to topic %s for %s.%s.ga_sessions_%s' % (topic_id, project_id, dataset_id, date_shard))
        if not dry_run:
            publisher.publish(topic_path, json.dumps(SAMPLE_LOAD_DATA).encode('utf-8'), origin='python-unit-test'
                                          , username='gcp')
            time.sleep(SLEEP_TIME)

