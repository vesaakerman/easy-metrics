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

# Run mongodb
mongod --smallfiles &

# Import test logs and metadata
./cmd/importlogs.py
./cmd/importmetadata.py
```

# Check if imported data are imported in MongoDB
```
mongo management
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
