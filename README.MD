# Streaming Jobs Manager

This service is a management application for EMR based Spark and Flink jobs.

----

## Before starting development

Streaming Jobs Manager is implemented in Play2 framework

----

## Running locally

1) Create local MySQL database:
```
CREATE SCHEMA `dvmsdb` DEFAULT CHARACTER SET utf8 ;
```

Make sure you have SBT installed.
Navigate to the project root folder and execute:

```
sbt run
```

Application should be available at http://localhost:9000
The database structure will be created on the first launch by Play2 evolution mechanism,
see https://www.playframework.com/documentation/latest/Evolutions

Populate the database with initial data from [SQL file](docs/init_data-dev.sql)

### debugging

To enable remote debug on port 9999, add `-jvm-debug 9999` to SBT command:
```
sbt -jvm-debug 9999 run
```

## Deploying

Build the distribution which is a ZIP archive containing startup shell script, jar libraries and configs:
```
sbt dist
```

Then SSH to the EC2 instance and:

1) Shutdown already running previous version
```
sudo kill `cat ./streaming-jobs-manager-1.0/RUNNING_PID`
```

2) Unzip distribution:
```
unzip streaming-jobs-manager-1.0.zip
```

3) Run application:
```
sudo ./streaming-jobs-manager-1.0/bin/streaming-jobs-manager -Dconfig.resource=application.conf -Dhttp.port=80 &
```
(for Production, instead of `application.conf` specify `prod.conf`)