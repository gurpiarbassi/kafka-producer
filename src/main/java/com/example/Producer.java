package com.example;

import com.google.common.io.Resources;
import com.gurps.avro.Person;
import com.namics.commons.random.RandomData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.gurps.avro.Gender.MALE;
import static java.time.format.DateTimeFormatter.ISO_DATE;

public class Producer {

    public static void main(String[] args) throws IOException {
        KafkaProducer<String, Person> producer;
        try (InputStream props = Resources.getResource("producer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        try {
            IntStream.range(1, 100).forEach(__ -> {

                Person person = Person.newBuilder()
                                      .setId(UUID.randomUUID().toString())
                                      .setFirstName(RandomData.firstname())
                                      .setLastName(RandomData.lastname())
                                      .setEmailAddress(RandomData.email())
                                      .setJoinDate(ISO_DATE.format(RandomData.random(LocalDate.class)))
                                      .setBirthdate(ISO_DATE.format(RandomData.random(LocalDate.class)))
                                      .setPhoneNumber(RandomData.random(String.class, "tel"))
                                      .setMiddleName(RandomData.lastname())
                                      .setSex(MALE)
                                      .setSiblings(RandomData.randomInteger())
                                      .setUsername(RandomData.randomString())
                                      .build();

                final ProducerRecord<String, Person> record = new ProducerRecord<>(
                    "people", // topic
                    person.getId(), // key
                    person);

                producer.send(record, (metaData, exception) -> {
                    if (metaData != null) {
                        System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d\n",
                                          record.key(), record.value(), metaData.partition(), metaData.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });
            });
        } catch (Throwable throwable) {
            System.out.println(throwable.getStackTrace());
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
