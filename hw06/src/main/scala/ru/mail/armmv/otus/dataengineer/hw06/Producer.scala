package ru.mail.armmv.otus.dataengineer.hw06

import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

import scala.io.Source

import java.util.Properties

object Producer {

  def produce(): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:29092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer(props, new LongSerializer, new StringSerializer)

    val reader = Source.fromResource("bestsellers.xls").reader()
    val books = CSVParser.parse(reader, CSVFormat.DEFAULT.withHeader()).getRecords

    try {
      books.forEach(
        book => {
          val jsonMsg: String =
            s"""
              |{
              | "name": "${book.get(0)}",
              | "author": "${book.get(1)}",
              | "userRating": "${book.get(2)}",
              | "reviews": "${book.get(3)}",
              | "price": "${book.get(4)}",
              | "year": "${book.get(5)}",
              | "genre": "${book.get(6)}"
              |}
              """.stripMargin
          println(s"${book.getRecordNumber} $jsonMsg")
          producer.send(new ProducerRecord("books", book.getRecordNumber, jsonMsg))
        }
      )
    } finally {
      producer.flush()
      producer.close()
    }
  }
}
