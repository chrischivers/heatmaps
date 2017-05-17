package heatmaps

import com.typesafe.config.ConfigFactory

case class Config(dBConfig: DBConfig, placesApiKey: String)

case class DBConfig(host: String, port: Int, username: String, password: String, dbName: String)
object ConfigLoader {

  private val defaultConfigFactory = ConfigFactory.load()

  val defaultConfig: Config = {
    val dbParamsPrefix = "heatmaps.db."
    Config(
      DBConfig(
        defaultConfigFactory.getString(dbParamsPrefix + "host"),
        defaultConfigFactory.getInt(dbParamsPrefix + "port"),
        defaultConfigFactory.getString(dbParamsPrefix + "username"),
        defaultConfigFactory.getString(dbParamsPrefix + "password"),
        defaultConfigFactory.getString(dbParamsPrefix + "dbName")
      ),
      defaultConfigFactory.getString("placesApi.key")
    )
  }
}