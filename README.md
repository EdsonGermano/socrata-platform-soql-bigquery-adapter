soql-bigquery-adapter
=====================

The BigQuery Adapter for SODAServer includes:
- **soql-server-bq**: query server for querying datasets in Google BigQuery
- **secondary-watcher-bq**: secondary watcher plugin for replicating (big) data

## Build Requirements
sbt
webdav4sbt - available at https://bitbucket.org/diversit/webdav4sbt.git

## Build and Test

```sh
sudo -u postgres createdb -O blist -E utf-8 secondary
createdb -O blist -E utf-8 secondary
sbt test package assembly
```

## Setup

#### Credentials

This service requires that a Google BigQuery credentials file is stored in the environment variable `GOOGLE_APPLICATION_CREDENTIALS`.

This credentials file is tied to your Google BigQuery project, which you should specify in the [configuration](https://github.com/socrata-platform/soql-bigquery-adapter/blob/master/soql-server-bq/src/main/resources/reference.conf) for the project:

```
bigquery {
    project-id: "my_bigquery_project_id"
    dataset-id: "my_bigquery_dataset_id"
}
```

#### Assembly

To create the jars:

```
cd $BIGQUERY_ADAPTER_DIR
sbt clean assembly
```

Link the secondary-watcher-bq jar:

```
ln -s /store-bq/target/scala-2.10/store-bq-assembly-*.jar ~/secondary-stores
```


## Running the service

For active development, when you always want the latest up to date code in your repo, you will probably be executing this from an SBT shell:

    soql-bigquery-adapter/run

For running the soql-bigquery-adapter as one of several microservices, it might
be better to build the assembly and run it to save on memory:

    bin/start_bq_adapter.sh

Running from sbt is recommended in a development environment because
it ensures you are running the latest migrations without having to build a
new assembly.

#### Check that the query server is up and running

```
curl http://localhost:6060/version
```

