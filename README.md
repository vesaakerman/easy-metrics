# easy-metrics
Application to produce statistics for EASY and DataverseNL

# Installation
```
CentOS: yum install mongodb
yum install python-pip

Ubuntu: apt-get install mongodb
apt-get install python-pip

# Install the basic requirements
pip install pymongo
yum install mongodb
yum install mongodb-server

# Create directories
In easy-metrics home-directory:
mkdir easyimports/datasets
mkdir easyimports/datasets/imported_datasets
mkdir easyimports/logs
mkdir easyimports/logs/imported_logs
mkdir easyimports/reports

# Run mongodb
mongod --smallfiles &

#Create collections in Mongodb
Create 'easy' database
In 'easy' database create collections 'dataset', 'logs' and 'status'
In 'dataset' collection create index for field 'pid'
In 'logs' collection create index for field 'date'
In 'status' collection add document:
    {
        "last_dataset_number" : NumberInt(0),
        "document" : NumberInt(1)
    }

# Import test logs and datasets
Datasets to be imported have to be in easyimports/datasets directory as a .tgz file
./cmd/importdatasets.py

Log events to be imported have to be in easyimports/logs directory
./cmd/importlogs.py
```

# Check if all data are imported in MongoDB
```
# Run from console:
mongo management

# Run commands one by one to see inserted records 
db.data.count()
db.data.find()
use metadata
db.data.find()
```

# Run first use case to see if application is working fine
```
./core/usecases.py 
It should show output:
{u'count': 31, u'_id': u'easy-dataset:48101'}
{u'count': 32, u'_id': u'easy-dataset:64410'}
{u'count': 62, u'_id': u'easy-dataset:49840'}
{u'count': 33, u'_id': u'easy-dataset:51548'}
{u'count': 32, u'_id': u'easy-dataset:64220'}
{u'count': 32, u'_id': u'easy-dataset:51346'}
{u'count': 34, u'_id': u'easy-dataset:58343'}
{u'count': 4351, u'_id': None}
{u'count': 31, u'_id': u'easy-dataset:37221'}
{u'count': 73, u'_id': u'easy-dataset:44426'}
```

# Connect Metrics to Dataverse storage to get data
```
cp ./easy/settings/dataverse-settings.py ./easy/settings/dataverseset.py
vi ./easy/settings/dataverseset.py

# Change settings to provide access to logs and metadata dataset:
dataversehost = 'dataverse.host'
key = 'dataverse-token-from-account'
handle = 'hdl:org/identifier' 
```
