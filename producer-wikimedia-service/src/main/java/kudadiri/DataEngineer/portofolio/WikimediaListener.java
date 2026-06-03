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
    private Logger[] log;

    public WikimediaListener(KafkaProducer<String, String> producer, String topic, Logger[] log) {
        this.producer = producer;
        this.topic = topic;
        this.log = log;
    }

    @Override
    public void onClosed(@NotNull EventSource eventSource) {
        log[0].info("Koneksi Stream Ditutup pukul: {}...", Instant.now());
    }

    @Override
    public void onEvent(@NotNull EventSource eventSource, @Nullable String id, @Nullable String type, @NotNull String data) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, id, data);
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                log[0].error("Terjadi Kesalahan Saat mengirim data ke topic: {} partisi: {}", recordMetadata.topic(), recordMetadata.partition());
            } else {
                log[1].info("Data {}, Berhasil Dikirim ke partisi {}}", id, recordMetadata.partition());
            }
        });
    }

    @Override
    public void onFailure(@NotNull EventSource eventSource, @Nullable Throwable t, @Nullable Response response) {
        if (t != null) {
            log[0].error("Terjadi Error pada Stream: {}", t.getMessage());
        } else {
            System.out.println("Terjadi Error pada Stream: unknown cause (throwable is null)");
            if (response != null) {
                log[0].error("Response code: {}", response.code());
            }
        }
    }

    @Override
    public void onOpen(@NotNull EventSource eventSource, @NotNull Response response) {
        log[0].info("Koneksi Stream Terbuka pukul: {}...", Instant.now());
    }
}
