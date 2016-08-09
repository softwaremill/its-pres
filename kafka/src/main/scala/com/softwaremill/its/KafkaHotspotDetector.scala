package com.softwaremill.its

import java.time.{Instant, ZoneOffset}
import java.util.Properties
import java.{lang, util}

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.{ProcessorContext, TimestampExtractor}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}

import scala.collection.JavaConverters._
import scala.io.Source

object KafkaHotspotDetector {
  var maxDate = 0L

  def main(args: Array[String]): Unit = {
    implicit val config = EmbeddedKafkaConfig(9092, 2181, Map("num.partitions" -> "8"))
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("csv")
    EmbeddedKafka.createCustomTopic("windowed-trips", partitions = 8)

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
      .flatMap(new KeyValueMapper[String, String, lang.Iterable[KeyValue[WindowBounds, Trip]]] {
        override def apply(key: String, value: String) = {
          Trip.parseOrDiscard(value).headOption match {
            case Some(t) =>
              WindowBounds
                .boundsFor(t.dropoffTime)
                .map(wb => new KeyValue(wb, t))
                .asJava

            case None =>
              util.Collections.emptyList()
          }
        }
      })
      .through(wbSerde, tripSerde, "windowed-trips") // distribute work
      .aggregateByKey(
      new Initializer[Window] {
        override def apply() = Window.Empty
      },
      new Aggregator[WindowBounds, Trip, Window] {
        override def apply(aggKey: WindowBounds, value: Trip, aggregate: Window) =
          aggregate.copy(bounds = aggKey).addTrip(value)
      },
      TimeWindows
        .of("windowed-trips-windows", WindowBounds.WindowLengthMinutes*60*1000L)
        .advanceBy(WindowBounds.StepLengthMinutes*60*1000L),
      wbSerde,
      windowSerde
    )
      .toStream
      .transform(new TransformerSupplier[Windowed[WindowBounds], Window, KeyValue[String, Option[String]]] {
        override def get() = new Transformer[Windowed[WindowBounds], Window, KeyValue[String, Option[String]]] {
          var ctx: ProcessorContext = _
          var store: KeyValueStore[String, String] = _

          override def init(context: ProcessorContext) = {
            ctx = context
            store = ctx.getStateStore(HotspotStoreName).asInstanceOf[KeyValueStore[String, String]]
          }

          override def punctuate(timestamp: Long) = null

          override def transform(key: Windowed[WindowBounds], value: Window) = {
            val start = Instant.ofEpochMilli(key.window().start()).atOffset(ZoneOffset.UTC)
            val end = Instant.ofEpochMilli(key.window().end()).atOffset(ZoneOffset.UTC)

            if (key.window().end() > maxDate) {
              maxDate = key.window().end()
              println("WINDOW MAX: " + end)
            }

            val storeKey = key.window().start().toString
            val inStore = Option(store.get(storeKey)).map(_.toInt)
            val hotspots = value.close()

            (hotspots.size, inStore) match {
              case (0, None) => new KeyValue("1", None)
              case (x, None) =>
                store.put(storeKey, x.toString)
                new KeyValue("1", Some(s"Hotspots from: $start->$end: $x"))
              case (x, Some(y)) if x == y => new KeyValue("1", None)
              case (x, Some(y)) =>
                if (x == 0) store.delete(storeKey)
                else store.put(storeKey, x.toString)
                new KeyValue("1", Some(s"Hotspots from: $start->$end: $x->$y"))
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

  val wbSerde = Serdes.serdeFrom(new Serializer[WindowBounds] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
    override def serialize(topic: String, data: WindowBounds) = data.serialize.getBytes("UTF-8")
    override def close() = {}
  }, new Deserializer[WindowBounds] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
    override def close() = {}
    override def deserialize(topic: String, data: Array[Byte]) = {
      WindowBounds.deserialize(new String(data, "UTF-8")).get
    }
  })

  val tripSerde = Serdes.serdeFrom(new Serializer[Trip] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
    override def serialize(topic: String, data: Trip) = data.serialize.getBytes("UTF-8")
    override def close() = {}
  }, new Deserializer[Trip] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
    override def close() = {}
    override def deserialize(topic: String, data: Array[Byte]) = {
      Trip.deserialize(new String(data, "UTF-8")).get
    }
  })

  val windowSerde = Serdes.serdeFrom(new Serializer[Window] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
    override def serialize(topic: String, data: Window) = data.serialize.getBytes("UTF-8")
    override def close() = {}
  }, new Deserializer[Window] {
    override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
    override def close() = {}
    override def deserialize(topic: String, data: Array[Byte]) = {
      Window.deserialize(new String(data, "UTF-8")).get
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

class HostpotDetectorTimestampExtractor extends TimestampExtractor {
  override def extract(record: ConsumerRecord[AnyRef, AnyRef]) = {
    if (record.key() == KafkaHotspotDetector.CsvRecordKey) {
      Trip.parseOrDiscard(record.value().asInstanceOf[String])
        .map(_.dropoffTime.toEpochSecond*1000L)
        .headOption
        .getOrElse(0L)
    } else {
      record.value() match {
        case t: Trip => t.dropoffTime.toEpochSecond*1000L
        case _ => throw new RuntimeException(s"Called for $record")
      }
    }
  }
}