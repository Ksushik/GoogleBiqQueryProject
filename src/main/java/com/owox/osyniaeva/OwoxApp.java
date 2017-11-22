package com.owox.osyniaeva;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.owox.osyniaeva.actors.SupervisorActor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OwoxApp {

    public static void main(String ...args) throws Exception {
        SpringApplication.run(OwoxApp.class, args);
        final ActorSystem system = ActorSystem.create("MySystem");
        final ActorRef mainActor = system.actorOf(Props.create(SupervisorActor.class), "mainactor");
        mainActor.tell(new SupervisorActor.InitMessage(args),ActorRef.noSender());
    }
}
