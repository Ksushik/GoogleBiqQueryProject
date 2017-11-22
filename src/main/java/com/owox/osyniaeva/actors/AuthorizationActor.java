package com.owox.osyniaeva.actors;

import java.io.FileInputStream;
import java.io.IOException;


import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.sun.xml.internal.bind.XmlAccessorFactory;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;


public class AuthorizationActor extends AbstractActor {

	private final LoggingAdapter log = Logging.getLogger(context().system(), this);

	/**
	 * using Application Default Credentials for Authentication client service .
	 */

	public AuthorizationActor() {
		receive(ReceiveBuilder.
				match(AuthorizationMessage.class, message -> {
					sender().tell(createAuthorizedClient(message.getProjectID()), self());
				}).
				matchAny(o -> log.info("received unknown message")).build()
		);
	}

	public static class AuthorizationMessage {

		private String projectID;

		public AuthorizationMessage(String projectID) {
			this.projectID = projectID;
		}

		public String getProjectID() {
			return projectID;
		}
	}

	static Props props() {
		return Props.create(AuthorizationActor.class, () -> new AuthorizationActor());
	}

	private static BigQuery createAuthorizedClient(String projectId) throws IOException {

		return BigQueryOptions.newBuilder()
				.setCredentials(
						ServiceAccountCredentials.fromStream(new FileInputStream("credentials.json"))
				).setProjectId(projectId)
				.build().getService();
	}
}

