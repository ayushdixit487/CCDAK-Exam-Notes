import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerTest {

 public static void main(String[] args) throws InterruptedException, ExecutionException{
  //Create producer property
  String bootstrapServer = "localhost:9092";
  Properties properties = new Properties();
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
  properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  
  //Create safe producer
  properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
  properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
  properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
  properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
  
  //High throughput producer (at the expense of a bit of latency and CPU usage)
  properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
  properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //20ms wait time
  properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32KB batch size
  
  //Create producer
  KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
  
  //create a producer record
  ProducerRecord<String, String> record = new ProducerRecord<>("topicName", "firstRecord");
  //create producer record with key
  //new ProducerRecord<>("topicName", "MessageKey", "Message");
  //create producer record with key and partition number
  //new ProducerRecord<>("topicName", 1 /*partition number*/, "MessageKey", "Message");
  
  //send data - asynchronous
  //without callback
  //producer.send(record);
  //with callback
  producer.send(record, (recordMetadata, exception) -> {
   if(exception == null){
    System.out.println(recordMetadata.topic() + "+" + recordMetadata.partition() + "+" + recordMetadata.offset());
   }else{
    System.err.println(exception.getMessage());
   }
  });
  
  //send data - synchronous
  //without callback
  //producer.send(record).get(); //.get() make it synchronous call
  
  //flush data
  producer.flush();
  
  //flush and close producer
  producer.close();
 }
}
