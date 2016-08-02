package com.softwaremill.its

import java.time.{Instant, ZoneOffset}
import java.{lang, util}
import java.util.Properties

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier, TimestampExtractor}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Try

object KafkaHotspotDetector {
  var maxDate = 0L

  def main(args: Array[String]): Unit = {
    implicit val config = EmbeddedKafkaConfig(9092, 2181, Map("num.partitions" -> "8"))
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("csv")
    EmbeddedKafka.createCustomTopic("grid-boxes", partitions = 8)

    daemonThread {
      produceCsv("csv")
    }

    daemonThread {
      detectHotspots("csv", "results")
    }

    daemonThread {
      printResults("results")
    }

    scala.io.StdIn.readLine()

    EmbeddedKafka.stop()
  }

  val CsvRecordKey = "csv-key"

  def produceCsv(to: String): Unit = {
    val producer = new KafkaProducer[String, String](producerProps)

    val groupSize = 10000
    var groupCounter = 0
    Source
      .fromFile(Config.csvFileName)
      .getLines()
      .grouped(groupSize)
      .foreach { group =>
        group.map(el => producer.send(new ProducerRecord(to, CsvRecordKey, el)))
          .foreach(_.get)
        groupCounter += 1
        if (groupCounter % 10 == 0) println(s"Produced ${groupCounter*groupSize} lines")
      }

    producer.close()

    println(s"Produced whole CSV to topic: $to")
  }

  def detectHotspots(from: String, to: String) = {
    val builder = new KStreamBuilder()

    val HotspotStoreName = "hotspots"

    builder.addStateStore(Stores.create(HotspotStoreName).withStringKeys().withStringValues().inMemory().build())

    builder
      .stream(Serdes.String(), Serdes.String(), from)
      .flatMap(new KeyValueMapper[String, String, lang.Iterable[KeyValue[GridBox, GridBoxCountsWithTimestamp]]] {
        override def apply(key: String, value: String) = {
          Try(value)
            .map(_.split(","))
            .flatMap(a => if (Config.Green) Trip.parseGreen(a) else Trip.parseYellow(a))
            .toOption
            .filter(_.isValid) match {

            case Some(trip) =>
              GridBoxCounts
                .forTrip(trip)
                .map(gbc => new KeyValue(gbc.gb, GridBoxCountsWithTimestamp(gbc, trip.dropoffTime.toEpochSecond * 1000L)))
                .asJava

            case None =>
              util.Collections.emptyList()
          }
        }
      })
      .through(gridBoxSerde, gridBoxCountsTimestampSerde, "grid-boxes") // distribute work
      .aggregateByKey(
      new Initializer[GridBoxCounts] {
        override def apply() = GridBoxCounts.forGridBox(GridBox(0, 0), 0)
      },
      new Aggregator[GridBox, GridBoxCountsWithTimestamp, GridBoxCounts] {
        override def apply(aggKey: GridBox, value: GridBoxCountsWithTimestamp, aggregate: GridBoxCounts) =
          value.gbc.add(aggregate)
      },
      TimeWindows
        .of("grid-box-windows", WindowBounds.WindowLengthMinutes*60*1000L)
        .advanceBy(WindowBounds.StepLengthMinutes*60*1000L),
      gridBoxSerde,
      gridBoxCountsSerde
    )
      .toStream
      .transform(new TransformerSupplier[Windowed[GridBox], GridBoxCounts, KeyValue[String, Option[String]]] {
        override def get() = new Transformer[Windowed[GridBox], GridBoxCounts, KeyValue[String, Option[String]]] {
          var ctx: ProcessorContext = _
          var store: KeyValueStore[String, String] = _

          override def init(context: ProcessorContext) = {
            ctx = context
            store = ctx.getStateStore(HotspotStoreName).asInstanceOf[KeyValueStore[String, String]]
          }

          override def punctuate(timestamp: Long) = null

          override def transform(key: Windowed[GridBox], value: GridBoxCounts) = {
            val start = Instant.ofEpochMilli(key.window().start()).atOffset(ZoneOffset.UTC)
            val end = Instant.ofEpochMilli(key.window().end()).atOffset(ZoneOffset.UTC)

            if (key.window().end() > maxDate) {
              maxDate = key.window().end()
              println("WINDOW MAX: " + end)
            }

            val c = AddedCountsWithWindow(key.key(), value.counts, WindowBounds(start, end))
            val storeKey = start.toString + key.key().serialize
            val inStore = Option(store.get(storeKey))

            (c.detectHotspot, inStore) match {
              case (None, None) => new KeyValue("1", None)
              case (None, Some(_)) =>
                store.delete(storeKey)
                new KeyValue("1", Some("RMV HOTSPOT " + start + ", " + key.key()))
              case (Some(hs), None) =>
                store.put(storeKey, "yes")
                new KeyValue("1", Some("ADD HOTSPOT " + hs))
              case (Some(hs), Some(_)) =>
                new KeyValue("1", Some("UPD HOTSPOT " + hs))
            }
          }

          override def close() = { }
        }
      }, HotspotStoreName)
      .flatMap(new KeyValueMapper[String, Option[String], lang.Iterable[KeyValue[String, String]]] {
        override def apply(key: String, value: Option[String]) = {
          value match {
            case None => util.Collections.emptyList()
            case Some(v) => util.Collections.singletonList(new KeyValue(key, v))
          }
        }
      })
//      .flatMap(new KeyValueMapper[Windowed[GridBox], GridBoxCounts, lang.Iterable[KeyValue[String, String]]] {
//        override def apply(key: Windowed[GridBox], value: GridBoxCounts) = {
//          val end = Instant.ofEpochMilli(key.window().end()).atOffset(ZoneOffset.UTC)
//
//          if (key.window().end() > maxDate) {
//            maxDate = key.window().end()
//            println("WINDOW MAX: " + end)
//          }
//
//          val c = AddedCountsWithWindow(key.key(), value.counts, WindowBounds(
//            Instant.ofEpochMilli(key.window().start()).atOffset(ZoneOffset.UTC),
//            end
//          ))
//
//          c.detectHotspot match {
//            case None => util.Collections.emptyList()
//            case Some(hs) => util.Collections.singletonList(new KeyValue("1", hs.toString))
//          }
//        }
//      })
      .to(to)

    val streams = new KafkaStreams(builder, streamProps)
    streams.start()

    println("Streams started")
  }

  def printResults(from: String): Unit = {
    val consumer = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(util.Arrays.asList(from))
    val start = System.currentTimeMillis()
    while (true) {
      val records = consumer.poll(60000L)
      val now = System.currentTimeMillis()
      println(s"[${(now-start)/1000}] Poll done")
      val recordIt = records.iterator()
      while (recordIt.hasNext) {
        val record = recordIt.next()
        println(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}")
      }
    }
  }

  def producerProps = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", Integer.valueOf(0))
    props.put("batch.size", Integer.valueOf(16384))
    props.put("linger.ms", Integer.valueOf(1))
    props.put("buffer.memory", Integer.valueOf(33554432))
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props
  }

  def streamProps = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "its")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[HostpotDetectorTimestampExtractor].getName)
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "8")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props
  }

  def consumerProps = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "false")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props
  }

  val gridBoxSerde = Serdes.serdeFrom(new Serializer[GridBox] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
    override def serialize(topic: String, data: GridBox) = data.serialize.getBytes("UTF-8")
    override def close() = {}
  }, new Deserializer[GridBox] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
    override def close() = {}
    override def deserialize(topic: String, data: Array[Byte]) = {
      GridBox.deserialize(new String(data, "UTF-8")).get
    }
  })

  val gridBoxCountsTimestampSerde = Serdes.serdeFrom(new Serializer[GridBoxCountsWithTimestamp] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
    override def serialize(topic: String, data: GridBoxCountsWithTimestamp) = data.serialize.getBytes("UTF-8")
    override def close() = {}
  }, new Deserializer[GridBoxCountsWithTimestamp] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
    override def close() = {}
    override def deserialize(topic: String, data: Array[Byte]) = {
      GridBoxCountsWithTimestamp.deserialize(new String(data, "UTF-8")).get
    }
  })

  val gridBoxCountsSerde = Serdes.serdeFrom(new Serializer[GridBoxCounts] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
    override def serialize(topic: String, data: GridBoxCounts) = data.serialize.getBytes("UTF-8")
    override def close() = {}
  }, new Deserializer[GridBoxCounts] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
    override def close() = {}
    override def deserialize(topic: String, data: Array[Byte]) = {
      GridBoxCounts.deserialize(new String(data, "UTF-8")).get
    }
  })

  def daemonThread(block: => Unit): Unit = {
    val t = new Thread(new Runnable {
      override def run() = block
    })
    t.setDaemon(true)
    t.start()
  }
}

case class GridBoxCountsWithTimestamp(gbc: GridBoxCounts, ts: Long) {
  def serialize = gbc.serialize + "!" + ts
}

object GridBoxCountsWithTimestamp {
  def deserialize(d: String) = Try(d.split("!")).flatMap { a =>
    GridBoxCounts.deserialize(a(0)).map(gbc => GridBoxCountsWithTimestamp(gbc, a(1).toLong))
  }
}

class HostpotDetectorTimestampExtractor extends TimestampExtractor {
  override def extract(record: ConsumerRecord[AnyRef, AnyRef]) = {
    if (record.key() == KafkaHotspotDetector.CsvRecordKey) {
      Try(record.value().asInstanceOf[String])
        .map(_.split(","))
        .flatMap(a => if (Config.Green) Trip.parseGreen(a) else Trip.parseYellow(a))
        .toOption
        .filter(_.isValid)
        .map(_.dropoffTime.toEpochSecond*1000L)
        .getOrElse(0L)
    } else {
      record.value() match {
        case GridBoxCountsWithTimestamp(_, ts) => ts
        case _ => throw new RuntimeException(s"Called for $record")
      }
    }
  }
}