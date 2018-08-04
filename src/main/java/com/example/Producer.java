package com.example;

import com.google.common.io.Resources;
import com.gurps.avro.Person;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.gurps.avro.Gender.MALE;

public class Producer {

    public static void main(String[] args) throws IOException {
        KafkaProducer<String, Person> producer;
        try (InputStream props = Resources.getResource("producer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        try {

            Person person = Person.newBuilder()
                                  .setId(1)
                                  .setFirstName("fred")
                                  .setLastName("flintstone")
                                  .setEmailAddress("fred.flintstone@bedrock.com")
                                  .setJoinDate("2017-06-01")
                                  .setBirthdate("1982-01-12")
                                  .setPhoneNumber("91822")
                                  .setMiddleName("bernard")
                                  .setSex(MALE)
                                  .setSiblings(2)
                                  .setUsername("fredflintstone")
                                  .build();

            final ProducerRecord<String, Person> record = new ProducerRecord<>(
                "my-new-topic", // topic
                "user_id_" + 2, // key
                person);

            producer.send(record, (metaData, exception) -> {
                if (metaData != null) {
                    System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d\n",
                                      record.key(), record.value(), metaData.partition(), metaData.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        } catch (Throwable throwable) {
            System.out.println(throwable.getStackTrace());
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
