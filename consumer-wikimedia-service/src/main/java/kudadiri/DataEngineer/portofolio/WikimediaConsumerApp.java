package kudadiri.DataEngineer.portofolio;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
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
    private static final ObjectMapper mapper = new ObjectMapper();

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

    private static KafkaConsumer<String, String> createConsumer(String servers,String group, String identifier) {
        Properties props = new Properties();
        String consumerId = group + "-" + identifier;

        props.put("bootstrap.servers", servers);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "latest");
        props.put("group.id", group);
        props.put("group.instance.id", consumerId);
        props.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        props.put("enable.auto.commit", "false");

        return new KafkaConsumer<>(props);
    }

    private static String extractId(String json) throws JsonProcessingException {
        return mapper.readTree(json)
                .get("meta")
                .get("id")
                .asText();
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("[IMPORTANT] need parameter <bootstrap-server> <consumer-group-id> <consumer-id>");
        } else {
            System.out.println("Consumer Started..");
        }

        String servers = args[0];
        String group = args[1];
        String identifier = args[2];

//        String servers = "172.25.5.7:9092";
//        String group = "consumer-openSearch-group";
//        String identifier = "1";

        final Logger log = LoggerFactory.getLogger(WikimediaConsumerApp.class.getSimpleName());

        // create OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create Kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer(servers, group, identifier);

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detect a shutdown, calling consumer.wakeup().");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

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
                System.out.printf("Receive %s record(s)\n", recordCount);

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    String id = extractId(record.value());

                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(id);

//                    IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
//                    log.info(response.getId());

                    bulkRequest.add(indexRequest);
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                    if (bulkResponse.hasFailures()) {
                        log.error("Bulk indexing contains failures.");

                        for (BulkItemResponse item : bulkResponse.getItems()) {
                            if (item.isFailed()) {
                                log.error(
                                        "Failed to index document id={}, reason={}",
                                        item.getId(),
                                        item.getFailureMessage()
                                );
                            }
                        }

                        continue;
                    }

                    System.out.println("inserted " + bulkResponse.getItems().length + " record(s).");

                    // Simulate slow downstream system
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    consumer.commitSync();
                    System.out.println("Offsets have been commited!");

//                    consumer.commitAsync((map, e) -> {
//                        if (e != null) {
//                            log.error("Failed to commit offset {}", map, e);
//                        } else {
//                            System.out.println("Offsets have been commited!");
//                        }
//                    });
                }
            }
        } catch(WakeupException e) {
            log.info("Consumer is starting to shutdown.");
        }  catch(Exception e) {
            log.error("Unexpected error in consumer", e);
        } finally {
//            consumer.close();
//            openSearchClient.close();
            log.info("The Consumer is now gracefully shutdown.");
        }
    }
}