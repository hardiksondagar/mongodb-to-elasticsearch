# MongoDB to ElasticSearch
Ingest data from Mongodb to Elasticsearch


### Steps
Create virtual environment and activate.
```
virtualenv -p python3 env
source env/bin/activate
```

Install python dependencies
```
pip install -r requirements.txt
```

Update configuration in `__main__` block and run
```
python mongodb-to-elasticsearch.py
```


### Incremental Add/Update
To add or update of mongodb documents incrementally, set one field in the document that keeps created and updated timestamps. Use this field to create query to fetch documents created/updated between timestamp range.

Let's say created/updated timestamp being stored in mongodb with the field name `updatedAt`. Now use this field to create query that fetches documents created/updated in last 5 minutes.

```
import datetime
now = datetime.datetime.now()
start = now - datetime.timedelta(minutes=5)
end = now

mongodb_query = {
    "updatedAt": {
        "$gte": start,
        "$lte": end
    }
}
```
Set cronjob to run script on every 5 mins
```
*/5 * * * * /path-to-your-python-env/bin/python mongodb-to-elasticsearch.py
```

### Tested with
- Python v3.6
- ElasticSearch v7.9
- MongoDB v4.2.1
