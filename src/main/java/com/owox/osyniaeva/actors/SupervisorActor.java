package com.owox.osyniaeva.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import com.google.cloud.bigquery.BigQuery;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

public class SupervisorActor extends AbstractActor {

    private static String projectId, datasetId, prefix;
    private static int count;
    private final LoggingAdapter log = Logging.getLogger(context().system(), this);


    public SupervisorActor() {
        receive(ReceiveBuilder.
                match(InitMessage.class, initMessage -> {
                    projectId = initMessage.getProjectId();
                    datasetId = initMessage.getDatasetId();
                    prefix = initMessage.getPrefix();
                    count = initMessage.getCount();
                    self().tell(new AuthorizationActor.AuthorizationMessage(projectId), self());
                }).
                match(AuthorizationActor.AuthorizationMessage.class, message -> {
                    ActorRef aouthActor = getContext().actorOf(
                            Props.create(AuthorizationActor.class));
                    aouthActor.forward(message, getContext());
                }).
                match(BigQuery.class, query -> {
                    ActorRef createTableActor = getContext().actorOf(
                            Props.create(CreateTableActor.class));
                    createTableActor.tell(new CreateTableActor
                            .CreateTableMessage(projectId, datasetId, prefix, count, query), self());

                    ActorRef updateTableActor = getContext().actorOf(
                            Props.create(UpdateExDateActor.class));
                    updateTableActor.tell(new UpdateExDateActor
                            .UpdateExpDatesMessage(projectId, datasetId, query), self());

                }).
                match(String.class, message -> log.info(message)).
                matchAny(o -> log.info("received unknown message")).build()
        );
    }


    static Props props() {
        return Props.create(SupervisorActor.class, () -> new SupervisorActor());
    }

    public static class InitMessage {
        private String[] args;

        public String getProjectId() {
            return args[0];
        }

        public String getDatasetId() {
            return args[1];
        }

        public String getPrefix() {
            return args[2];
        }

        public int getCount() {
            return Integer.parseInt(args[3]);
        }

        public InitMessage(String... args) {
            this.args = args;
        }
    }

    public static class State {
        public String projectId;
    }

}
