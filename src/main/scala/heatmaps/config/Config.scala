package heatmaps.config

import com.typesafe.config.ConfigFactory
import scala.concurrent.duration.{Duration, FiniteDuration}

case class PostgresDBConfig(host: String, port: Int, username: String, password: String, dbName: String) extends DBConfig
case class CacheConfig(timeToLive: Duration)
case class PlacesApiConfig(apiKey: String, returnLimt: Int)

case class Config(dBConfig: DBConfig, cacheConfig: CacheConfig, placesApiConfig: PlacesApiConfig)
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
        defaultConfigFactory.getString("placesApi.key"),
        defaultConfigFactory.getInt("placesApi.returnLimit")
      )
    )
  }
}