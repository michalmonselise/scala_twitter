import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.render._
import geotrellis.vector._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable

object KafkaReadToGeotrellisPlotCoordinates extends App {
  val KafkaTopic = "tweets"
  val extent = Extent(-124.848974,24.396308,-66.885444,49.384358) // Extent World
  val config = ConfigFactory.load()
  implicit val system = ActorSystem.create("akka-stream-kafka-getting-started", config)
  implicit val mat = ActorMaterializer()
  implicit val formats = DefaultFormats

  //creating a point feature with lat, lon and constant weights
  def tweetToPoint(lat:Double, lon:Double): PointFeature[Double] = {
    Feature(Point(lon, lat), 1)
  }

  val points = new mutable.MutableList[PointFeature[Double]];

  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  Consumer.committableSource(consumerSettings, Subscriptions.topics(KafkaTopic))
    .map(msg => {
      val json = parse(msg.record.value)
      println(json)

      val twitterMap = json.extract[Map[String, Any]]
      val longitude: Double = twitterMap.get("longitude").get.asInstanceOf[Double]
      val latitude: Double = twitterMap.get("latitude").get.asInstanceOf[Double]

      points += tweetToPoint(latitude, longitude)
      if (points.length % 100 == 0) {
        createPngOfMappedTweets(points)
      }
    })
    .runWith(Sink.ignore)


  private def createPngOfMappedTweets(pts: mutable.MutableList[PointFeature[Double]]) = {
    val kernelWidth: Int = 9

    // Gaussian kernel with std. deviation 1.5, amplitude 25 */
    val kern: Kernel = Kernel.gaussian(kernelWidth, 1.5, 25)

    val kde: Tile = pts.kernelDensity(kern, RasterExtent(extent, 700, 400))

    val colorMap = ColorMap(
      (0 to kde.findMinMax._2 by 4).toArray,
      ColorRamps.HeatmapBlueToYellowToRedSpectrum
    )


    kde.renderPng(colorMap).write("/tmp/tweetsMapped" + System.currentTimeMillis() + ".png")
  }
}
