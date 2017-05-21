package heatmaps

import com.typesafe.config.ConfigFactory

case class Config(postgresDBConfig: postgresDBConfig, fusionDBConfig: FusionDBConfig, placesApiKey: String)
case class postgresDBConfig(host: String, port: Int, username: String, password: String, dbName: String)
case class FusionDBConfig(clientSecretsFileName: String, numberRetries: Int)

object ConfigLoader {

  private val defaultConfigFactory = ConfigFactory.load()

  val defaultConfig: Config = {
    val postgresDBParamsPrefix = "db.postgres."
    val fusionDBParamsPrefix = "db.fusion."
    Config(
      postgresDBConfig(
        defaultConfigFactory.getString(postgresDBParamsPrefix + "host"),
        defaultConfigFactory.getInt(postgresDBParamsPrefix + "port"),
        defaultConfigFactory.getString(postgresDBParamsPrefix + "username"),
        defaultConfigFactory.getString(postgresDBParamsPrefix + "password"),
        defaultConfigFactory.getString(postgresDBParamsPrefix + "dbName")
      ),
      FusionDBConfig(
        defaultConfigFactory.getString(fusionDBParamsPrefix + "client-secrets-file-name"),
        defaultConfigFactory.getInt(fusionDBParamsPrefix + "number-of-retries")
      ),
      defaultConfigFactory.getString("placesApi.key")
    )
  }
}