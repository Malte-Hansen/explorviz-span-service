package net.explorviz.span.kafka;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers a {@link StateListener} for the {@link KafkaStreams} object that is used as
 * ErrorStateListener. The Quarkus application does not shut down automatically if the Kafka Streams
 * thread goes into {@link State}.ERROR state. The Kafka streams ERROR state has no come back,
 * therefore the application is dead.
 */
@ApplicationScoped
public class ShutdownHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownHandler.class);

  @Inject
  KafkaStreams streams;

  void onStart(@Observes final StartupEvent ev) {
    this.streams.setStateListener(new ErrorStateListener());
  }

  void onStop(@Observes final ShutdownEvent ev) {
  }

  private static class ErrorStateListener implements StateListener {
    @Override
    public void onChange(final State newState, final State oldState) {
      if (newState == State.ERROR) {
        LOGGER.error("Kafka Streams thread died. "
            + "Are Kafka topic initialized? Quarkus application will shut down.");

        LOGGER.error("About to system exit due to Kafka Streams Error.");
        Quarkus.asyncExit(-1);
        Quarkus.waitForExit();
        System.exit(-1); // NOPMD
      }

    }
  }
}
