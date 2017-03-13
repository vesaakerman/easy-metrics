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

# Check if imported data are in mongo
mongo management
db.data.count()
db.data.find()
use metadata
db.data.find()
```
