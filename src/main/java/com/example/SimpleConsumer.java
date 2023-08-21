package com.example;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.ArrayList;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
// 이거 바꿔야함 몽고 db에서 인플럭스 db로
public class SimpleConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";
    private final static String MONGODB_CONNECTION_URL = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.10.1";
    private final static String DATABASE_NAME = "mydb";
    private final static String COLLECTION_NAME = "articles";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        consumer.subscribe(Arrays.asList("topicA", "topicB")); // topicA와 topicB 구독

        // MongoDB 연결 설정
        try (MongoClient mongoClient = MongoClients.create(MONGODB_CONNECTION_URL)) {
            MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

            List<String[]> dataAList = new ArrayList<>();
            List<String[]> dataBList = new ArrayList<>();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Received record: {}", record);

                    // 데이터 분리
                    if (record.topic().equals("topicA")) {
                        String[] dataA = record.value().split(","); // topicA 데이터
                        dataAList.add(dataA);
                    } else if (record.topic().equals("topicB")) {
                        String[] dataB = record.value().split(","); // topicB 데이터
                        dataBList.add(dataB);
                    }

                    // topicA와 topicB 데이터가 모두 들어왔는지 확인
                    if (!dataAList.isEmpty() && !dataBList.isEmpty()) {
                        // 데이터 조합
                        String[] dataA = dataAList.remove(0);
                        String[] dataB = dataBList.remove(0);

                        String id = dataA[0];
                        String timestamp = dataA[1];
                        String name = dataA[2];
                        String password = dataB[0];
                        String gender = dataB[1];

                        // MongoDB에 데이터 삽입
                        Document document = new Document();
                        document.append("id", id)
                                .append("timestamp", timestamp)
                                .append("name", name)
                                .append("password", password)
                                .append("gender", gender);
                        collection.insertOne(document);
                        logger.info("Inserted document: {}", document);
                    }
                }
            }
        }
    }
}