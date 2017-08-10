package heatmaps.metrics

import java.net.InetAddress

import com.codahale.metrics.MetricRegistry
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.MetricsConfig
import metrics_influxdb.HttpInfluxdbProtocol
import java.util.concurrent.TimeUnit
import metrics_influxdb.InfluxdbReporter

trait MetricsLogging extends StrictLogging {

  private val metricRegistry = new MetricRegistry()

  val metricsConfig: MetricsConfig

  val metricsGroupName: String

  lazy val reporter = InfluxdbReporter.forRegistry(metricRegistry)
    .protocol(new HttpInfluxdbProtocol(metricsConfig.host, metricsConfig.port, metricsConfig.dbName))
    .tag("group", metricsGroupName)
    .tag("hostname", InetAddress.getLocalHost.getHostName)
    .convertDurationsTo(TimeUnit.SECONDS)
    .build().start(10, TimeUnit.SECONDS)

  def incrMetricsCounter(series: String) = {
    reporter //initialises the lazy val after fields have been set
    metricRegistry.counter(series).inc()
  }
}