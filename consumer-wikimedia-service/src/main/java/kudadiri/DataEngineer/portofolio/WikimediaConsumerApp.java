package kudadiri.DataEngineer.portofolio;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class WikimediaConsumerApp {
    private static RestHighLevelClient createOpenSearchClient() {
        String opensearchConnection = "https://1c7fdb58c2:77a29dd0ee439c5ec3a9@serene-camphor-1pk1bptz.ap-southeast-2.bonsaisearch.net";

        RestHighLevelClient restHighLevelClient;
        URI uri = URI.create(opensearchConnection);
        String userInfo = uri.getUserInfo();

        if (userInfo == null) {
            HttpHost http = new HttpHost(uri.getHost(), uri.getPort(), "http");
            RestClientBuilder builder = RestClient.builder(http);

            restHighLevelClient = new RestHighLevelClient(builder);
        } else {
            String[] auth = userInfo.split(":");

            BasicCredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            HttpHost httpHost = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
            RestClientBuilder restClientBuilder = RestClient.builder(httpHost).setHttpClientConfigCallback(
                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
            );

            restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();

        props.put("bootstrap.servers", "172.25.5.7:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "latest");
        props.put("group.id", "consumer-openSearch-group");

        return new KafkaConsumer<>(props);
    }

    public static void main(String[] args) {
        final Logger log = LoggerFactory.getLogger(WikimediaConsumerApp.class.getSimpleName());

        // create OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create Kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer();

        try(openSearchClient; consumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

                log.info("The Wikimedia Index has been created!");
            } else {
                log.info("The Wikimedia Index already exists.");
            }

            consumer.subscribe(Collections.singletonList("wikimedia.recentChange"));

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Receive {} record(s)", recordCount);

                for (ConsumerRecord<String, String> record : records) {
                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON);

                    IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                    log.info(response.getId());
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}