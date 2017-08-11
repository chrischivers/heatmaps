package heatmaps.metrics

import java.net.InetAddress

import com.codahale.metrics.MetricRegistry
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.{ConfigLoader, MetricsConfig}
import metrics_influxdb.HttpInfluxdbProtocol
import java.util.concurrent.TimeUnit
import metrics_influxdb.InfluxdbReporter
import scala.concurrent.{ExecutionContext, Future}

object MetricsLogging extends StrictLogging {

  private val metricRegistry = new MetricRegistry()

  val metricsConfig: MetricsConfig = ConfigLoader.defaultConfig.metricsConfig

  val reporter = InfluxdbReporter.forRegistry(metricRegistry)
    .protocol(new HttpInfluxdbProtocol(metricsConfig.host, metricsConfig.port, metricsConfig.dbName))
    .tag("hostname", InetAddress.getLocalHost.getHostName)
    .convertDurationsTo(TimeUnit.SECONDS)

    reporter.build().start(30, TimeUnit.SECONDS)

  def incrMetricsCounter(series: String, increaseBy: Int = 1) = {
    metricRegistry.counter(s"$series-count").inc(increaseBy)
    metricRegistry.meter(s"$series-meter").mark(increaseBy)
  }
}