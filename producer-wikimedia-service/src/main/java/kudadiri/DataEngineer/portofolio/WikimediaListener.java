package kudadiri.DataEngineer.portofolio;

import okhttp3.Response;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import java.time.Instant;

public class WikimediaListener extends EventSourceListener {

    private KafkaProducer<String, String> producer;
    private String topic;
    private Logger log;

    public WikimediaListener(KafkaProducer<String, String> producer, String topic, Logger log) {
        this.producer = producer;
        this.topic = topic;
        this.log = log;
    }

    @Override
    public void onClosed(@NotNull EventSource eventSource) {
        log.info("Koneksi Stream Ditutup pukul: {}...", Instant.now());
    }

    @Override
    public void onEvent(@NotNull EventSource eventSource, @Nullable String id, @Nullable String type, @NotNull String data) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, id, data);
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                log.error("Terjadi Kesalahan Saat mengirim data ke topic: {} partisi: {}", recordMetadata.topic(), recordMetadata.partition());
            } else {
                System.out.printf("Data %s, Berhasil Dikirim ke partisi %s\n", id, recordMetadata.partition());
            }
        });
    }

    @Override
    public void onFailure(@NotNull EventSource eventSource, @Nullable Throwable t, @Nullable Response response) {
        if (t != null) {
            log.error("Terjadi Error pada Stream: {}", t.getMessage());
        } else {
            System.out.println("Terjadi Error pada Stream: unknown cause (throwable is null)");
            if (response != null) {
                System.out.println("Response code: " + response.code());
            }
        }
    }

    @Override
    public void onOpen(@NotNull EventSource eventSource, @NotNull Response response) {
        log.info("Koneksi Stream Terbuka pukul: {}...", Instant.now());
    }
}
