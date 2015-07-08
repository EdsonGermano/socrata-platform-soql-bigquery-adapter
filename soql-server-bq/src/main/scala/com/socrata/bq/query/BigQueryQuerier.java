package com.socrata.bq.query;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import scala.util.parsing.json.JSONObject;

import java.io.IOException;
import java.util.Iterator;
import java.util.Scanner;

public class BigQueryQuerier {


    // Executes query
    public static void query(String queryString) throws Exception {
        Scanner scanner = new Scanner(System.in);
        String projectId = "thematic-bee-98521";
        boolean batch = false;
        long waitTime = 100;
        scanner.close();
        long curTime = System.currentTimeMillis();
        Iterator<GetQueryResultsResponse> pages = run(projectId, queryString,
                batch, waitTime);
        long difference = System.currentTimeMillis() - curTime;
        while (pages.hasNext()) {
            BigqueryUtils.printRows(pages.next().getRows(), System.out);
        }
        System.out.println("elapsed milliseconds bigquery: " + difference);
    }


    /**
     *
     * @param projectId Get this from Google Developers console
     * @param queryString Query we want to run against BigQuery
     * @param batch True if you want to batch the queries
     * @param waitTime How long to wait before retries
     * @return An interator to the result of your pages
     * @throws IOException Thrown if there's an IOException
     * @throws InterruptedException Thrown if there's an Interrupted Exception
     */
    public static Iterator<GetQueryResultsResponse> run(final String projectId,
                                                        final String queryString,
                                                        final boolean batch,
                                                        final long waitTime)
            throws IOException, InterruptedException {

        Bigquery bigquery = BigqueryServiceFactory.getService();

        Job query = asyncQuery(bigquery, projectId, queryString, batch);
        Bigquery.Jobs.Get getRequest = bigquery.jobs().get(
                projectId, query.getJobReference().getJobId());

        //Poll every waitTime milliseconds,
        //retrying at most retries times if there are errors
        BigqueryUtils.pollJob(getRequest, waitTime);

        Bigquery.Jobs.GetQueryResults resultsRequest = bigquery.jobs().getQueryResults(
                projectId, query.getJobReference().getJobId());

        return BigqueryUtils.getPages(resultsRequest);
    }

    // [START asyncQuery]
    /**
     * Inserts an asynchronous query Job for a particular query.
     *
     * @param bigquery  an authorized BigQuery client
     * @param projectId a String containing the project ID
     * @param querySql  the actual query string
     * @param batch True if you want to run the query as BATCH
     * @return a reference to the inserted query job
     * @throws IOException Thrown if there's a network exception
     */
    public static Job asyncQuery(final Bigquery bigquery,
                                 final String projectId,
                                 final String querySql,
                                 final boolean batch) throws IOException {

        JobConfigurationQuery queryConfig = new JobConfigurationQuery()
                .setQuery(querySql);

        if (batch) {
            queryConfig.setPriority("BATCH");
        }

        Job job = new Job().setConfiguration(
                new JobConfiguration().setQuery(queryConfig));

        return bigquery.jobs().insert(projectId, job).execute();
    }

}
