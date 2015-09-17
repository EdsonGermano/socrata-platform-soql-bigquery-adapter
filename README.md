soql-bigquery-adapter
=====================

The Bigquery Adapter for SODAServer includes:
- **soql-server-bq**: query server for querying datasets in Google BigQuery
  - [Docker README](https://github.com/socrata-platform/soql-bigquery-adapter/blob/master/soql-server-bq/docker/README.md)
- **secondary-watcher-bq**: secondary watcher plugin for replicating (big) data
  - [Docker README](https://github.com/socrata-platform/soql-bigquery-adapter/blob/master/store-bq/docker/README.md)

## About Google Bigquery

You can access the Google Bigquery console for a particular table at [https://bigquery.cloud.google.com/table/your-project-id:your-dataset-id.your-table-name](https://bigquery.cloud.google.com/table/your-project-id:your-dataset-id.your-table-name).

There are three levels of organization within Bigquery: projects, datasets, and tables. Bigquery projects contain datasets, each of which contain tables, which actually store data. In the image below, `alpha_49088_1` is a table, `socrata_staging` is a dataset, and `socrata-data` is a project.

**Note**: Your project id is not necessarily the same as your project's name. In the image below, `thematic-bee-98521` is the project id.

!["Bigquery project structure"](/images/project-hierarchy.png "")

Bigquery uses a SQL-like language that supports most SQL clauses and has a [variety of useful functions](https://cloud.google.com/bigquery/query-reference). **soql-server-bq** supports most basic SoQL operations, including the majority of queries used on Data Lens pages; however, there is limited support for geo-spatial functions. You can issue queries against your tables using the Bigquery console.

!["Query"](/images/query.png "")

**secondary-watcher-bq** uses load jobs, not streaming inserts, to put data in Bigquery. You can read about load job quotas [here](https://cloud.google.com/bigquery/quota-policy#import).

Whether you are uploading data using the **secondary-watcher-bq** plugin, or through Google Bigquery's frontend interface, be aware that it may take several minutes for data to actually appear in your tables after the load jobs have completed.

## Build and Test

```sh
sudo -u postgres createdb -O blist -E utf-8 secondary
createdb -O blist -E utf-8 secondary
sbt test package assembly
```

## Setup

#### Credentials

This service requires that a Google BigQuery credentials file is stored in the environment variable `GOOGLE_APPLICATION_CREDENTIALS`.

This credentials file is tied to your Google Bigquery project, which you should specify in the [configuration](https://github.com/socrata-platform/soql-bigquery-adapter/blob/master/soql-server-bq/src/main/resources/reference.conf) for the project:

```
bigquery {
    project-id: "my_bigquery_project_id"
    dataset-id: "my_bigquery_dataset_id"
}
```

#### Assembly

To create the jars from the project root:

```
sbt clean assembly
```

Link the **secondary-watcher-bq** jar:

```
ln -s ./store-bq/target/scala-2.10/store-bq-assembly-*.jar ~/secondary-stores
```


## Running the service

For active development, when you always want the latest up to date code in your repo, you will probably be executing this from an SBT shell:

    soql-bigquery-adapter/run

For running the soql-bigquery-adapter as one of several microservices, it might be better to build the assembly and run it to save on memory. After building the assembly, run:

    bin/start_bq_adapter.sh

Running from sbt is recommended in a development environment because it ensures you are running the latest migrations without having to build a new assembly.

#### Check that the query server is up and running

```
curl http://localhost:6060/version
```

Example response:

```
{"service":"soql-server-bq","version":"0.6.11-SNAPSHOT","revision":"9311523301-dirty","scala":"2.10.4"}
```

