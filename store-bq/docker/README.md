# Secondary Watcher docker support

The files in this directory allow you to build a docker image for the BQ secondary watcher.
The store-pg assembly must be copied to `store-bq-assembly.jar` in this directory before building.

## Required Runtime Variables

All variables required by [the data-coordinator secondary watcher base image](https://github.com/socrata/data-coordinator/tree/master/coordinator/docker-secondary-watcher#required-runtime-variables)
are required.  

In addition, the following is required:

* `PG_SECONDARY_DB_PASSWORD_LINE` - Full line of config for DB password.  Designed to be either `password = "foo"` or `include /path/to/file`.  Must be the same across all instances.
* `PROJECT-ID` - The Bigquery project id
* `DATASET_ID` - The Bigquery dataset id
* `BATCH_SIZE` - The maximum number of bytes to send in a load job.

## Optional Runtime Variables

All optional variables supported by [the data-coordinator secondary watcher base image](https://github.com/socrata/data-coordinator/tree/master/coordinator/docker-secondary-watcher#optional-runtime-variables)
are supported.  

In addition, the following optional variables are supported.  For defaults, see the [Dockerfile](Dockerfile).

* `LOG_METRICS` - Should various metrics information be logged to the log
* `LOG_LEVEL` - The logging level
* `PG_SECONDARY_DB_NAME` - DB database name.
* `PG_SECONDARY_DB_PORT` - DB port number.
* `PG_SECONDARY_DB_USER` - DB user name.
