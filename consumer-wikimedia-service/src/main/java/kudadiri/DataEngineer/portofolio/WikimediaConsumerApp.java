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
//        if (args.length < 3) {
//            System.out.println("[IMPORTANT] need parameter <bootstrap-server> <topic-name> <consumer-group-id> <consumer-id>");
//        } else {
//            System.out.println("Consumer Started..");
//        }
//
//        String servers = args[0];
//        String topic = args[1];
//        String group = args[2];
//        String identifier = args[3];

        String servers = "<IP_KAFKA>:<PORT_KAFKA>";
        String topic = "wikimedia.recentChange";
        String group = "consumer-openSearch-group";
        String identifier = "1";

        final Logger sysLog = LoggerFactory.getLogger(WikimediaConsumerApp.class.getSimpleName());
        final Logger metricsLog = LoggerFactory.getLogger("metricsLogs");

        // create OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create Kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer(servers, group, identifier);

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                sysLog.info("Detect a shutdown, calling consumer.wakeup().");
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

                sysLog.info("The Wikimedia Index has been created!");
            } else {
                sysLog.info("The Wikimedia Index already exists.");
            }

            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                // [METRICS 3] Mulai hitung total waktu satu siklus loop
                long startLoopTime = System.currentTimeMillis();

                BulkRequest bulkRequest = new BulkRequest();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
//                System.out.printf("Receive %s record(s)\n", recordCount);

                // [METRICS 1] Catat jumlah data yang didapat dari Kafka poll
                if (recordCount > 0) {
                    metricsLog.info("EVENT:KafkaPoll | Records:{} | Status:SUCCESS", recordCount);
                }

                for (ConsumerRecord<String, String> record : records) {
                    String id = extractId(record.value());

                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(id);

//                    IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
//                    sysLog.info(response.getId());

                    bulkRequest.add(indexRequest);
                }

                if (bulkRequest.numberOfActions() > 0) {
                    // [METRICS 2] Mulai hitung waktu proses ke OpenSearch
                    long startBulkTime = System.currentTimeMillis();

                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    long bulkDuration = System.currentTimeMillis() - startBulkTime;

                    if (bulkResponse.hasFailures()) {
                        sysLog.error("Bulk indexing contains failures.");

                        // [METRICS 2b] Catat jika bulk indexing gagal/error performanya
                        metricsLog.info("EVENT:OpenSearchBulk | Records:{} | DurationMs:{} | Status:FAILED",
                                bulkRequest.numberOfActions(), bulkDuration);

                        for (BulkItemResponse item : bulkResponse.getItems()) {
                            if (item.isFailed()) {
                                sysLog.error(
                                        "Failed to index document id={}, reason={}",
                                        item.getId(),
                                        item.getFailureMessage()
                                );
                            }
                        }

                        continue;
                    }

                    // [METRICS 2a] Catat jika bulk indexing sukses beserta durasinya
                    metricsLog.info("EVENT:OpenSearchBulk | Records:{} | DurationMs:{} | Status:SUCCESS",
                            bulkResponse.getItems().length, bulkDuration);

//                    System.out.println("inserted " + bulkResponse.getItems().length + " record(s).");

                    // Simulate slow downstream system
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    consumer.commitSync();
                    System.out.println("Offsets have been commited!");

                    // [METRICS 3] Catat total waktu siklus pemrosesan sampai commit selesai
                    long totalLoopDuration = System.currentTimeMillis() - startLoopTime;
                    metricsLog.info("EVENT:TotalPipelineCycle | Records:{} | DurationMs:{} | Status:SUCCESS",
                            recordCount, totalLoopDuration);

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
            sysLog.info("Consumer is starting to shutdown.");
        }  catch(Exception e) {
            sysLog.error("Unexpected error in consumer", e);
        } finally {
//            consumer.close();
//            openSearchClient.close();
            sysLog.info("The Consumer is now gracefully shutdown.");
        }
    }
}