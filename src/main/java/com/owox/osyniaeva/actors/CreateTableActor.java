package com.owox.osyniaeva.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import com.google.api.gax.paging.Page;
import com.google.api.services.bigquery.Bigquery;
import com.google.cloud.bigquery.*;
import org.springframework.stereotype.Component;

import java.util.Calendar;
import java.util.Date;

public class CreateTableActor extends AbstractActor {

    private BigQuery authorizedClient;

    private final LoggingAdapter log = Logging.getLogger(context().system(), this);

    public CreateTableActor() {
        receive(ReceiveBuilder.
                match(CreateTableMessage.class, message -> {
                    authorizedClient = message.getClient();
                    createTable(message.getProjectId(), message.getDatasetId(),
                            message.getPrefix(), message.getCount());
                    sender().tell("new Tables Successfully created", self());
                }).
                matchAny(o -> log.info("received unknown message")).build()
        );
    }


    private void createTable(String projectId, String datasetId, String prefix, int count) throws Exception {

        DatasetId dataset = DatasetId.of(projectId, datasetId);
        Dataset myDataset = authorizedClient.getDataset(dataset);

        for (int i = 0; i < count; i++) {
            TableId tableId = TableId.of(myDataset.getDatasetId().getDataset(), prefix + i);
            Field field0 = Field.of("hitId", LegacySQLTypeName.STRING);
            Field field1 = Field.of("userId", LegacySQLTypeName.STRING);
            Schema schema = Schema.of(field0, field1);
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

            Table table = authorizedClient.create(tableInfo);
        }
    }

    public static class CreateTableMessage {
        private String projectId, datasetId, prefix;
        private int count;
        private BigQuery client;

        public CreateTableMessage(String projectId, String datasetId, String prefix,
                                  int count, BigQuery client) {
            this.projectId = projectId;
            this.datasetId = datasetId;
            this.prefix = prefix;
            this.count = count;
            this.client = client;
        }

        public String getProjectId() {
            return projectId;
        }

        public String getDatasetId() {
            return datasetId;
        }

        public String getPrefix() {
            return prefix;
        }

        public int getCount() {
            return count;
        }

        public BigQuery getClient() {
            return client;
        }
    }

    static Props props() {
        return Props.create(CreateTableActor.class, () -> new CreateTableActor());
    }

}
