package heatmaps.db

import com.github.mauricio.async.db.{Configuration, Connection, QueryResult}
import com.github.mauricio.async.db.pool.{ConnectionPool, ObjectFactory, PoolConfiguration}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import com.typesafe.scalalogging.StrictLogging
import heatmaps.DBConfig

import scala.concurrent.Future

trait DB[T <: Connection] extends StrictLogging {

  val connectionConfiguration: Configuration

  val connectionPoolConfig: PoolConfiguration

  val connection: ObjectFactory[T]

  val connectionPool: ConnectionPool[T]

  def connectToDB: Future[Connection] = connectionPool.connect

  def disconnect: Future[Connection] = connectionPool.disconnect


}

class PostgresDB(dBConfig: DBConfig) extends DB[PostgreSQLConnection] {
  logger.info(s"Setting up DB: ${dBConfig.dbName}")

  override val connectionConfiguration = Configuration(
  username = dBConfig.username,
  password = Some(dBConfig.password),
  host = dBConfig.host,
  port = dBConfig.port,
  database = Some(dBConfig.dbName))

  override val connectionPoolConfig: PoolConfiguration = new PoolConfiguration(maxObjects = 5, maxIdle = 5000, maxQueueSize = 100000)
  override val connection: ObjectFactory[PostgreSQLConnection] = new PostgreSQLConnectionFactory(connectionConfiguration)
  override val connectionPool: ConnectionPool[PostgreSQLConnection] = new ConnectionPool[PostgreSQLConnection](connection, connectionPoolConfig)
}
