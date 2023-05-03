package ru.mail.armmv.otus.dataengineer.hw06

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

import scala.jdk.CollectionConverters.{IterableHasAsJava, IterableHasAsScala}
import scala.collection.mutable

import java.time.Duration
import java.util.{Properties, UUID}

object Consumer {

  def consume(): Unit = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer(props, new LongDeserializer, new StringDeserializer)
    consumer.subscribe(List("books").asJavaCollection)

    val partitions = consumer.partitionsFor("books").asScala
    //println(partitions)
    val localStorage = new mutable.HashMap[Int, mutable.Queue[String]]
    partitions.foreach(
      partition => {
        localStorage.put(partition.partition(), new mutable.Queue[String]())
      }
    )

    try {
      consumer
        .poll(Duration.ofSeconds(1))
        .asScala
        .foreach { r => {
          val msg = s"${r.partition()} - ${r.key()} - ${r.value()}"
          val queue = localStorage(r.partition())
          if (queue.knownSize == 5) {
            queue.dequeue
          }
          //println(msg)
          queue.enqueue(msg)
        }
        }
    } finally {
      consumer.close()
    }

    println("\n\n----------\nИтог\n----------\n")
    localStorage.foreach(
      data => {
        data._2.foreach(
          r => println(r)
        )
      }
    )
  }
}
