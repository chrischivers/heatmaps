package heatmaps.db

import com.github.mauricio.async.db.pool.{ConnectionPool, PoolConfiguration}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import com.github.mauricio.async.db.{Configuration, QueryResult}
import com.google.maps.model.{LatLng, PlacesSearchResult}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.{City, postgresDBConfig}

import scala.concurrent.{ExecutionContext, Future}


class PostgresqlDB(dBConfig: postgresDBConfig, val schema: PlaceTableSchema)(implicit ec: ExecutionContext) extends PlacesDatabase {

  private val connectionConfiguration = Configuration(
    username = dBConfig.username,
    password = Some(dBConfig.password),
    host = dBConfig.host,
    port = dBConfig.port,
    database = Some(dBConfig.dbName))

  private val connectionPoolConfig = new PoolConfiguration(maxObjects = 5, maxIdle = 5000, maxQueueSize = 10000)

  private val connection: PostgreSQLConnectionFactory = new PostgreSQLConnectionFactory(connectionConfiguration)

  private val connectionPool: ConnectionPool[PostgreSQLConnection] = new ConnectionPool[PostgreSQLConnection](connection, connectionPoolConfig)

  private def connectToDB = connectionPool.connect

//  private def disconnectFromDB = connectionPool.disconnect

  def createPlacesTableIfNotExisting: Future[QueryResult] = {
    logger.info(s"Creating Places Table if not existing")
    for {
      _ <- connectToDB
      queryResult <- connectionPool.sendQuery(
        s"""
           |CREATE TABLE IF NOT EXISTS
           |${schema.tableName} (
           |    ${schema.placeId} varchar NOT NULL,
           |    ${schema.placeType} varchar NOT NULL,
           |    ${schema.placeName} varchar,
           |    ${schema.cityName} varchar,
           |    ${schema.lat} real NOT NULL,
           |    ${schema.lng} real NOT NULL,
           |    ${schema.lastUpdated} timestamp NOT NULL,
           |    PRIMARY KEY(${schema.primaryKey.mkString(",")})
           |);
        """.stripMargin)
    //  _ <- disconnectFromDB
    } yield queryResult
  }

  override def insertPlaces(placeSearchResults: List[PlacesSearchResult], city: City, placeType: String): Future[Unit] = {
    logger.info(s"Inserting places into DB: $placeSearchResults")
    for {
      _ <- connectToDB
      _ <- createPlacesTableIfNotExisting
      result <- Future.sequence(placeSearchResults.map(result => insertPlace(result, city, placeType)))
    //  _ <- disconnectFromDB
    } yield result.map(_ => ())
  }

  private def insertPlace(placeSearchResult: PlacesSearchResult, city: City, placeType: String): Future[QueryResult] = {
    val statement =
      s"""
         |
        |INSERT INTO ${schema.tableName} (${schema.placeId}, ${schema.placeType}, ${schema.cityName}, ${schema.lat}, ${schema.lng}, ${schema.lastUpdated})
         |    VALUES (?,?,?,?,?,'now');
         |
      """.stripMargin

    connectionPool.sendPreparedStatement(statement, List(placeSearchResult.placeId, placeType, city.name, placeSearchResult.geometry.location.lat, placeSearchResult.geometry.location.lng))
  }

  override def getPlacesForCity(city: City): Future[List[Place]] = {
    logger.info(s"getting places for city: $city")
    val query = s"SELECT * FROM ${schema.tableName} WHERE ${schema.cityName} = ?"
    for {
      _ <- connectToDB
      queryResult <- connectionPool.sendPreparedStatement(query, List(city.name))
     // _ <- disconnectFromDB
    } yield {
      queryResult.rows match {
        case Some(resultSet) => resultSet.map(res => {
          Place(
            res.apply(schema.placeId).asInstanceOf[String],
            res.apply(schema.placeType).asInstanceOf[String],
            new LatLng(res.apply(schema.lat).asInstanceOf[Float].toDouble, res.apply(schema.lng).asInstanceOf[Float].toDouble)
          )
        }).toList
        case None => List.empty
      }
    }
  }

  override def dropPlacesTable: Future[Unit] = {
    logger.info("dropping places table")
    val query = s"DROP TABLE IF EXISTS ${schema.tableName}"
    connectionPool.sendQuery(query).map(_ => ())
  }
}