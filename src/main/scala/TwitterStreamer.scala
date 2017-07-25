import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model._
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.hunorkovacs.koauth.domain.KoauthRequest
import com.hunorkovacs.koauth.service.consumer.DefaultConsumerService
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object TwitterStreamer extends App {
  val HBaseTableName = "tweets"
  val KafkaTopic = HBaseTableName
  val HBaseColumnFamily = "tweets_by_lat_long_date"
  val conf = ConfigFactory.load()

  //Get your credentials from https://apps.twitter.com and replace the values below
  private val url = "https://stream.twitter.com/1.1/statuses/filter.json"

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val formats = DefaultFormats

  private val consumer = new DefaultConsumerService(system.dispatcher)

  //Location boundary box for the united states
  val body = "locations=-124.848974,24.396308,-66.885444,49.384358"

  //Create Oauth 1a header
  val oauthHeader: Future[String] = consumer.createOauthenticatedRequest(
    KoauthRequest(
      method = "POST",
      url = url,
      authorizationHeader = None,
      body = Some(body)
    ),
    conf.getString("consumerKey"),
    conf.getString("consumerSecret"),
    conf.getString("accessToken"),
    conf.getString("accessTokenSecret")
  ) map (_.header)

  val hbaseConf = HBaseConfiguration create

  hbaseConf.set("hbase.zookeeper.quorum", "localhost:2222")
  initializeHBase()

  oauthHeader.onComplete {
    case Success(header) =>
      val httpHeaders: List[HttpHeader] = List(
        HttpHeader.parse("Authorization", header) match {
          case ParsingResult.Ok(h, _) => Some(h)
          case _ => None
        },
        HttpHeader.parse("Accept", "*/*") match {
          case ParsingResult.Ok(h, _) => Some(h)
          case _ => None
        }
      ).flatten
      val httpRequest: HttpRequest = HttpRequest(
        method = HttpMethods.POST,
        uri = Uri(url),
        headers = httpHeaders,
        entity = HttpEntity(contentType = MediaTypes.`application/x-www-form-urlencoded`.withCharset(HttpCharsets.`UTF-8`), string = body)
      )
      val request = Http().singleRequest(httpRequest)
      request.flatMap { response =>
        if (response.status.intValue() != 200) {
          println(response.entity.dataBytes.runForeach(_.utf8String))
          Future(Unit)
        } else {
          response.entity.dataBytes
            .scan("")((acc, curr) => if (acc.contains("\r\n")) curr.utf8String else acc + curr.utf8String)
            .filter(_.contains("\r\n"))
            .map(json => Try(parse(json).extract[Tweet]))
            .runForeach {
              case Success(tweet) =>
                processTweet(tweet)
              case Failure(e) =>
                print("!")
            }
        }
      }
              case Failure(failure) => println(failure.getMessage)
  }

  private def processTweet(tweet: Tweet) = {
    tweet.coordinates match {
      case Some(coordinates) =>
        println("-----")
        println(tweet.text + " with coordinates " + coordinates)
        val longitude = coordinates.coordinates(0).toDouble
        val latitude = coordinates.coordinates(1).toDouble

        writeToHBase(longitude, latitude, tweet.created_at, tweet.text)
        writeToKafka(longitude, latitude, tweet.created_at, tweet.text)
      case None =>
        print(".")
    }
  }

  def initializeHBase() : Unit = {
    val admin = new HBaseAdmin(hbaseConf)

    if (!admin.tableExists("tweets")) {
      val tableDesc = new HTableDescriptor(Bytes.toBytes(HBaseTableName))
      val idsColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes(HBaseColumnFamily))
      tableDesc.addFamily(idsColumnFamilyDesc)
      admin.createTable(tableDesc)
    }
  }

  def writeToHBase(longitude: Double, latitude: Double, created_at: String, text: String) : Unit = {
    val key = create_key(latitude, longitude, created_at)

    val table = new HTable(hbaseConf, HBaseTableName)
    val hbasePut= new Put(Bytes.toBytes(key))

    hbasePut.add(Bytes.toBytes(HBaseColumnFamily),Bytes.toBytes("longitude"),Bytes.toBytes(longitude))
    hbasePut.add(Bytes.toBytes(HBaseColumnFamily),Bytes.toBytes("latitude"),Bytes.toBytes(latitude))
    hbasePut.add(Bytes.toBytes(HBaseColumnFamily),Bytes.toBytes("created_at"),Bytes.toBytes(created_at))
    hbasePut.add(Bytes.toBytes(HBaseColumnFamily),Bytes.toBytes("text"),Bytes.toBytes(text))
    table.put(hbasePut)
  }

  def writeToKafka(longitude: Double, latitude: Double, created_at: String, text: String) : Unit = {
    val key = create_key(latitude, longitude, created_at)
    val json = ("latitude" -> latitude) ~ ("longitude" -> longitude) ~ ("created_at" -> created_at) ~ ("text" -> text)
    val kafkaMessage: String = compact(render(json))
    println("Writing to kafka topic " + KafkaTopic + " the key " + key + " and value " + kafkaMessage)

    val sourceForKafka = Source.single(kafkaMessage)
    val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer).withBootstrapServers("localhost:9092")
    sourceForKafka.map{ msg =>
     ProducerMessage.Message(new ProducerRecord[Array[Byte], String](KafkaTopic, 0, null, msg), msg)}
      .via(Producer.flow(producerSettings))
      .runWith(Sink.ignore)
  }

  def create_key(latitude: Double, longitude: Double, created_at: String): String = latitude + "-" + longitude + "-" + created_at
}
