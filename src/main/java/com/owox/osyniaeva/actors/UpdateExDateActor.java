package com.owox.osyniaeva.actors;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;

import java.time.LocalDate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UpdateExDateActor extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(context().system(), this);

    private BigQuery authorizedClient;

    public UpdateExDateActor() {
        receive(ReceiveBuilder.
                match(UpdateExpDatesMessage.class, message -> {
                    authorizedClient = message.getClient();
                    updateTables(message.getProjectId(), message.getDatasetId());
                    sender().tell("Tables exp data successfully updated", self());
                }).
                matchAny(o -> log.info("received unknown message")).build()
        );
    }

    public static class UpdateExpDatesMessage {
        private String projectId, datasetId;
        private BigQuery client;

        public UpdateExpDatesMessage(String projectId, String datasetId, BigQuery client) {
            this.projectId = projectId;
            this.datasetId = datasetId;
            this.client = client;
        }

        public String getProjectId() {
            return projectId;
        }

        public String getDatasetId() {
            return datasetId;
        }

        public BigQuery getClient() {
            return client;
        }
    }

    private void updateTables(String projectId, String datasetId) throws Exception {
        DatasetId dataset = DatasetId.of(projectId, datasetId);
        Dataset myDataset = authorizedClient.getDataset(dataset);

        Page<Table> tables = authorizedClient.listTables(myDataset.getDatasetId().getDataset(),
                BigQuery.TableListOption.pageSize(100));
        tables.iterateAll().forEach(this::updateTableExpData);
    }

    private void updateTableExpData(Table table) {
        if (matchesToThePattern(table.getTableId().getTable())) {
            TableInfo tableInfo = table.toBuilder().setExpirationTime(LocalDate.now().plusWeeks(2)
                    .toEpochDay()).build();
            authorizedClient.update(tableInfo);
        }
    }

    private static boolean matchesToThePattern(String testString) {
        Pattern p = Pattern.compile("^tmp*");
        Matcher m = p.matcher(testString);
        return m.matches();
    }

}

