# Talaria

TalariaDB is a distributed, highly available, and low latency time-series database that stores real-time data. It's built on top of [Badger DB.](https://github.com/dgraph-io/badger)  
Blog: https://engineering.grab.com/big-data-real-time-presto-talariadb

#### About TalariaDB
In Grab, millions and millions of transactions and connections take place every day on our platform, which requires data-driven decision making. And these decisions need to be made based on real-time data. For example, an experiment might inadvertently cause a significant increase of waiting time for riders.
  
To overcome the challenge of retrieving information from large amounts of data, we designed and built 
TalariaDB. It addresses our need to query at least 2-3 terabytes of data per hour with predictable low query latency and low cost. Most importantly, it plays very nicely with the different tools’ ecosystems and lets us query data using SQL.

## Architecture
![alt text](https://gitlab.myteksi.net/grab-x/talaria/raw/master/architecture.png)

The diagram above shows how TalariaDB ingests and serves data.
* The upstream ETL pipeline prepares the ORC files and store them in the S3 bucket as input.
* AWS SQS is created as the event notification service of the S3 bucket and notifies TalariaDB of any new object uploaded on S3.
* Presto is connected to TalariaDB through Thrift since it implements PrestoThriftService https://prestodb.github.io/docs/current/connector/thrift.html.
* AWS Route53 is used as DNS web service of TalariaDB, whose domain is registered on Presto cluster. The IP addresses of the DNS record is registered by TalariaDB using Gossip protocol in bootstrap phase.

Currently this project is currently highly coupled with AWS services like SQS, S3 and Route53. We will make these components (storage, DNS) pluggable and make TalariaDB useful for more generic case.  


## Quick Start
#### Preconditions
* setup AWS profile and make sure your machine is accessible to AWS and has enough permission to read from S3, SQS and manipulate Route53 records.

#### Steps
1. Set env vars
``` bash
export X_TALARIA_CONF=(path-to-this-repo)/config-ci.json
```
2. Edit config-ci.json with your own configurations (including AWS Route53 and SQS configs)
3. Start application
``` bash
go run (path-to-this-repo)/main.go
```


## About Us
TalariaDB is maintained by:
* [Roman Atachiants](https://www.linkedin.com/in/atachiants/)
* [Wang Yichao](https://www.linkedin.com/in/wangyichao/)

## License

TalariaDB is licensed under the --- (LICENSE.md)