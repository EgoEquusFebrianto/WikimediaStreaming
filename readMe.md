# Wikimedia Streaming

Real-Time Data Pipeline menggunakan Apache Kafka dan OpenSearch untuk mengonsumsi data perubahan artikel Wikimedia secara streaming, mendistribusikannya melalui Kafka, dan melakukan indexing ke OpenSearch untuk kebutuhan pencarian dan analisis data secara near real-time.

---

# Daftar Isi

* Arsitektur Sistem
* Teknologi yang Digunakan
* Alur Data
* Komponen Sistem
* Struktur Proyek
* Implementasi

    * Kafka Producer
    * Kafka Consumer
    * OpenSearch Integration
* Hasil Pengujian
* Cara Menjalankan
* Pengembangan Selanjutnya

---

# Arsitektur Sistem

![System Architecture](assets/wikimedia-architecture.png)

Gambar 1. Arsitektur keseluruhan sistem Wikimedia Streaming.

Diagram menunjukkan alur data mulai dari Wikimedia Event Stream → Kafka Producer → Kafka Topic → Kafka Consumer → OpenSearch.

---

# Teknologi yang Digunakan

| Teknologi         | Fungsi                             |
| ----------------- | ---------------------------------- |
| Java              | Bahasa pemrograman utama           |
| Apache Kafka      | Distributed Message Broker         |
| OkHttp3 SSE       | Mengonsumsi Wikimedia Event Stream |
| OpenSearch        | Penyimpanan dan pencarian data     |
| Bonsai OpenSearch | Managed OpenSearch Service         |
| Jackson           | JSON Processing                    |
| Maven             | Dependency Management              |

---

# Alur Data

![Data Flow](assets/alur-data.svg)

Gambar 2. Alur data dari Wikimedia Event Stream hingga tersimpan di OpenSearch.

Tahapan proses:

1. Producer membuka koneksi SSE ke Wikimedia.
2. Event perubahan artikel diterima secara real-time.
3. Producer mengirim event ke Kafka Topic.
4. Consumer membaca event dari Kafka.
5. Consumer melakukan parsing JSON.
6. Data di-index ke OpenSearch.
7. Data dapat dicari dan dianalisis melalui OpenSearch.

---

# Komponen Sistem

## Kafka Producer

![Kafka Producer Flow](assets/kafka-producer-flow.png)

Gambar 3. Alur kerja Kafka Producer.

Tanggung jawab:

* Membuka koneksi ke Wikimedia Event Stream.
* Mengonsumsi event menggunakan SSE.
* Mengirim event ke Kafka Topic.
* Menjaga aliran data secara kontinu.

---

## Kafka Consumer

![Kafka Consumer Flow](assets/kafka-consumer-flow.png)

Gambar 4. Alur kerja Kafka Consumer.

Tanggung jawab:

* Membaca event dari Kafka Topic.
* Melakukan parsing JSON.
* Mengekstrak informasi penting.
* Menyimpan dokumen ke OpenSearch.

---

## OpenSearch Integration

![OpenSearch Integration](assets/opensearch-integration.png)

Gambar 5. Integrasi Kafka Consumer dengan OpenSearch.

Fungsi:

* Penyimpanan dokumen hasil streaming.
* Near real-time indexing.
* Search dan analytics.

---

# Struktur Proyek

```text
wikimedia-streaming/
│
├── producer/
│   ├── WikimediaProducer.java
│   └── ...
│
├── consumer/
│   ├── WikimediaConsumer.java
│   └── ...
│
├── assets/
│   ├── system-architecture.png
│   ├── data-flow.png
│   ├── kafka-producer-flow.png
│   ├── kafka-consumer-flow.png
│   ├── opensearch-integration.png
│   ├── kafka-topic-message.png
│   ├── opensearch-dashboard.png
│   └── producer-consumer-logs.png
│
└── README.md
```

---

# Implementasi

## Kafka Producer

![Producer Log](assets/producer-log.png)

Gambar 6. Producer berhasil menerima event Wikimedia dan mengirimkannya ke Kafka.

Fitur utama:

* SSE Connection
* Event Streaming
* Kafka Producer API
* JSON Message Delivery

---

## Kafka Consumer

![Consumer Log](assets/consumer-log.png)

Gambar 7. Consumer berhasil membaca event dari Kafka dan memproses data.

Fitur utama:

* Kafka Consumer API
* JSON Processing
* OpenSearch Indexing

---

## Contoh Data Kafka

![Kafka Topic Message](assets/kafka-topic-message.png)

Gambar 8. Contoh pesan yang diterima Kafka Topic dari Wikimedia Stream.

---

## Data pada OpenSearch

![OpenSearch Dashboard](assets/opensearch-dashboard.png)

Gambar 9. Data hasil indexing yang tersimpan pada OpenSearch.

---

# Hasil Pengujian

![End-to-End Test](assets/end-to-end-test.png)

Gambar 10. Pengujian end-to-end pipeline berhasil memindahkan data dari Wikimedia Stream hingga OpenSearch.

Hasil:

* Producer menerima event secara real-time.
* Kafka berhasil mendistribusikan event.
* Consumer berhasil memproses event.
* OpenSearch berhasil menyimpan dokumen.

---

# Cara Menjalankan

## Menjalankan Kafka

```
docker compose up -d
```

## Menjalankan Producer

```bash
mvn exec:java
```

## Menjalankan Consumer

```bash
mvn exec:java
```

---

# Rencana Pengembangan Mendatang

* Docker Compose Deployment
* Kafka Connect Integration
* Schema Registry
* Elasticsearch/OpenSearch Dashboard
* Retry Mechanism
* Dead Letter Queue (DLQ)
* Monitoring menggunakan Prometheus dan Grafana
* Multi-partition Kafka Processing
