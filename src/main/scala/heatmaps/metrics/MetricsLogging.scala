package heatmaps.metrics

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.{ConfigLoader, MetricsConfig}
import metrics_influxdb.{HttpInfluxdbProtocol, InfluxdbReporter}
import nl.grons.metrics.scala.{Counter, DefaultInstrumented, Meter, MetricName}

object MetricsLogging extends StrictLogging with DefaultInstrumented {

  override lazy val metricBaseName: MetricName = MetricName("")

  val metricsConfig: MetricsConfig = ConfigLoader.defaultConfig.metricsConfig

  def setUpReporter = {
    logger.info("Setting up metrics reporter")
    InfluxdbReporter.forRegistry(metricRegistry)
      .protocol(new HttpInfluxdbProtocol(metricsConfig.host, metricsConfig.port, metricsConfig.dbName))
      .tag("hostname", InetAddress.getLocalHost.getHostName)
      .convertRatesTo(TimeUnit.MINUTES)
      .build().start(10, TimeUnit.SECONDS)
  }

  setUpReporter


  val radarSearchRequestsMeter: Meter = metrics.meter("radar-search-requests")
  def incrRadarSearchRequests = radarSearchRequestsMeter.mark()

  val detailsSearchRequestsMeter: Meter = metrics.meter("details-search-requests")
  def incrDetailsSearchRequests = detailsSearchRequestsMeter.mark()
}