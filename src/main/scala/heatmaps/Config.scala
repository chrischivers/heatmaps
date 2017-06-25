package heatmaps

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.{Duration, FiniteDuration}


case class Config(postgresDBConfig: PostgresDBConfig, cacheConfig: CacheConfig, placesApiKey: String)
case class PostgresDBConfig(host: String, port: Int, username: String, password: String, dbName: String)
case class CacheConfig(timeToLive: Duration)

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
      defaultConfigFactory.getString("placesApi.key")
    )
  }
}