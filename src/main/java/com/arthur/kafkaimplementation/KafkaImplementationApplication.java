package com.arthur.kafkaimplementation;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;

@QuarkusMain
public class KafkaImplementationApplication {

    public static void main(String... args) {
        Quarkus.run(args);
    }
}
