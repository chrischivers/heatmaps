package heatmaps.config

import com.typesafe.config.ConfigFactory
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.collection.JavaConverters._

case class PostgresDBConfig(host: String, port: Int, username: String, password: String, dbName: String) extends DBConfig
case class CacheConfig(timeToLive: Duration)
case class PlacesApiConfig(apiKeys: List[String], returnLimt: Int)
case class MetricsConfig(host: String, port: Int, dbName: String, enabled: Boolean)
case class MapsConfig(minZoom: Int, maxZoom: Int)

case class Config(dBConfig: DBConfig, cacheConfig: CacheConfig, placesApiConfig: PlacesApiConfig, metricsConfig: MetricsConfig, mapsConfig: MapsConfig)
sealed trait DBConfig {
  val host: String
  val port: Int
  val username: String
  val password: String
  val dbName: String
}

object ConfigLoader {

  implicit def asFiniteDuration(d: java.time.Duration): FiniteDuration =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)

  private val defaultConfigFactory = ConfigFactory.load()

  val defaultConfig: Config = {
    val postgresDBParamsPrefix = "db.postgres."
    val cacheDBParamsPrefix = "db.cache."
    Config(
      PostgresDBConfig(
        defaultConfigFactory.getString(postgresDBParamsPrefix + "host"),
        defaultConfigFactory.getInt(postgresDBParamsPrefix + "port"),
        defaultConfigFactory.getString(postgresDBParamsPrefix + "username"),
        defaultConfigFactory.getString(postgresDBParamsPrefix + "password"),
        defaultConfigFactory.getString(postgresDBParamsPrefix + "dbName")
      ),
      CacheConfig(
        defaultConfigFactory.getDuration(cacheDBParamsPrefix + "time-to-live")
      ),
      PlacesApiConfig(
        defaultConfigFactory.getStringList("placesApi.keys").asScala.toList,
        defaultConfigFactory.getInt("placesApi.returnLimit")
      ),
      MetricsConfig(
        defaultConfigFactory.getString("metrics.host"),
        defaultConfigFactory.getInt("metrics.port"),
        defaultConfigFactory.getString("metrics.dbName"),
        defaultConfigFactory.getBoolean("metrics.enabled")
      ),
      MapsConfig(
        defaultConfigFactory.getInt("maps.minZoom"),
        defaultConfigFactory.getInt("maps.maxZoom")
      )
    )
  }
}