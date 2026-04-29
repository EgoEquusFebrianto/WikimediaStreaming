package kudadiri.DataEngineer.portofolio.producer;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaEventHandler implements BackgroundEventHandler {

    KafkaProducer<String, String> producer;
    String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaEventHandler.class.getName());

    public WikimediaEventHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        log.info("Program Started..");
    }

    @Override
    public void onClosed() throws Exception {
        log.info("Producer is Shutdown");
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());

        // asynchronous
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {
        // pass
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in Stream Reading", throwable);
    }
}
