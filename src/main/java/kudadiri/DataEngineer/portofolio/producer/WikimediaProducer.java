package kudadiri.DataEngineer.portofolio.producer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {
    public static void main(String[] args) throws InterruptedException {
        String servers = "172.25.5.7:9092";
        String serializer = StringSerializer.class.getName();
        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", servers);
        properties.put("key.serializer", serializer);
        properties.put("value.serializer", serializer);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        WikimediaEventHandler eventHandler = new WikimediaEventHandler(producer, topic);

        EventSource.Builder esBuilder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource.Builder backgroundBuilder = new BackgroundEventSource.Builder(eventHandler, esBuilder);

        BackgroundEventSource eventSource = backgroundBuilder.build();
        eventSource.start();
        TimeUnit.MINUTES.sleep(10);
    }
}