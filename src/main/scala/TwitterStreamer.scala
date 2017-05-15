/**
 * This is a fully working example of Twitter's Streaming API client.
 * NOTE: this may stop working if at any point Twitter does some breaking changes to this API or the JSON structure.
 */

import javax.swing.JFrame

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import com.hunorkovacs.koauth.domain.KoauthRequest
import com.hunorkovacs.koauth.service.consumer.DefaultConsumerService
import com.typesafe.config.ConfigFactory
import org.jfree.chart.plot.{FastScatterPlot, PlotOrientation}
import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.data.xy.XYSeriesCollection

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.sameersingh.scalaplot.Implicits._
import org.sameersingh.scalaplot.{XYChart, XYPlotStyle}
import org.sameersingh.scalaplot.jfreegraph.JFGraphPlotter

import scala.collection.mutable.ListBuffer

object TwitterStreamer extends App {

  val conf = ConfigFactory.load()

  //Get your credentials from https://apps.twitter.com and replace the values below
  private val consumerKey = "H5n7EhpmyX7gMtwYFfGtU0NGo"
  private val consumerSecret = "Erfk0Is29vseSBy0SETJRWcin8rnS0TpLbYxEz8H3dFYTKkwl0"
  private val accessToken = "14311701-kHi0uAs9VLHfmbf9ITZpA4yrTfT57XqdqXXTdwUBC"
  private val accessTokenSecret = "S1mkQCiye1Anc8f6duMIh2kRk5gdb1wwTG3Kgaow9ZFTA"
  private val url = "https://stream.twitter.com/1.1/statuses/filter.json"

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val formats = DefaultFormats

  private val consumer = new DefaultConsumerService(system.dispatcher)

  //Filter tweets by a term "london"
  //Location boundary box for the united states
//	val body = "locations=-74,40,-73,41"
  val body = "locations=-124.848974,24.396308,-66.885444,49.384358"
  val source = Uri(url)

  //Create Oauth 1a header
  val oauthHeader: Future[String] = consumer.createOauthenticatedRequest(
    KoauthRequest(
      method = "POST",
      url = url,
      authorizationHeader = None,
      body = Some(body)
    ),
    consumerKey,
    consumerSecret,
    accessToken,
    accessTokenSecret
  ) map (_.header)

  var longitudes: ListBuffer[Double] = new ListBuffer[Double]()
  var latitudes: ListBuffer[Double] = new ListBuffer[Double]()
  val gpl = new JFGraphPlotter(xyChart(longitudes.toList -> latitudes.toList))
  val jchart: JFreeChart = gpl.plotXYChart(xyChart(longitudes.toList -> latitudes.toList))
  val chartPanel: ChartPanel = new ChartPanel(jchart)
  val frame = new JFrame()
  frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
  frame.setSize(1200, 800)

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
        uri = source,
        headers = httpHeaders,
        entity = HttpEntity(contentType = ContentType(MediaTypes.`application/x-www-form-urlencoded`), string = body)
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
                tweet.coordinates match {
                  case Some(coordinates) =>
                    println("-----")
                    println(tweet.text + " with coordinates " + coordinates)
                    longitudes += coordinates.coordinates(0).toDouble
                    latitudes += coordinates.coordinates(1).toDouble
                    displayJChart(getJFreeChart)
                  case None =>
                    print(".")
                }
              case Failure(e) =>
                print("!")
            }
        }
      }
    case Failure(failure) => println(failure.getMessage)
  }

  private def getJFreeChart: JFreeChart = {
    val chart: XYChart = xyChart(longitudes.toList -> Y(latitudes.toList, style = XYPlotStyle.Dots))
    val jfGraphPlotterData: XYSeriesCollection = JFGraphPlotter.xyCollection(chart.data)
    val jchart = ChartFactory.createScatterPlot(chart.title.getOrElse(""), chart.x.label, chart.y.label, jfGraphPlotterData, PlotOrientation.VERTICAL, false, false, false)
    jchart
  }

  def displayJChart(jchart: JFreeChart): Unit = {
    chartPanel.setChart(jchart)
    chartPanel.repaint();
    frame.add(chartPanel)
    frame.pack()
    frame.setVisible(true)
  }
}