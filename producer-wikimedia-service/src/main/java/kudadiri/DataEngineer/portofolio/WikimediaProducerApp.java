package kudadiri.DataEngineer.portofolio;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducerApp {
    private static final Logger log = LoggerFactory.getLogger(WikimediaProducerApp.class);
    private static final Logger metricLogs = LoggerFactory.getLogger("metricsLogger");

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 3) {
            System.out.println("[IMPORTANT] need parameter <bootstrap-server> <topic> <wikimedia-stream-url>");
        } else {
            System.out.println("Consumer Started..");
        }

        String servers = args[0];
        String topic = args[1];
        String streamUrl = args[2];

//        String servers = "172.25.5.7:9092";
//        String topic = "wikimedia.recentChange";
//        String streamUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

        Properties configs = new Properties();
        configs.put("bootstrap.servers", servers);
        configs.put("key.serializer", StringSerializer.class.getName());
        configs.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(0, TimeUnit.SECONDS)
                .build();

        Request request = new Request.Builder()
                .url(streamUrl)
                .header("Accept", "text/event-stream")
                .header("User-Agent", "agent/1.0")
                .header("Cache-Control", "no-cache")
                .build();

        Logger[] sysLog = {log, metricLogs};
        WikimediaListener listener = new WikimediaListener(producer, topic, sysLog);
        EventSource.Factory factory = EventSources.createFactory(client);
        EventSource eventSource = factory.newEventSource(request, listener);

        TimeUnit.MINUTES.sleep(10);
        eventSource.cancel();
        producer.close();
    }
}