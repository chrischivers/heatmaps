package heatmaps.db

import com.github.mauricio.async.db.pool.{ConnectionPool, PoolConfiguration}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import com.github.mauricio.async.db.{Configuration, QueryResult}
import com.google.maps.model.{LatLng, PlacesSearchResult}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.{City, PostgresDBConfig}
import scala.concurrent.duration._

import scala.concurrent.{Await, ExecutionContext, Future}


class PostgresqlDB(dBConfig: PostgresDBConfig, val schema: PlaceTableSchema, recreateTableIfExists: Boolean = false)(implicit ec: ExecutionContext) extends PlacesDatabase {

  private val connectionConfiguration = Configuration(
    username = dBConfig.username,
    password = Some(dBConfig.password),
    host = dBConfig.host,
    port = dBConfig.port,
    database = Some(dBConfig.dbName))

  private val connectionPoolConfig = new PoolConfiguration(maxObjects = 5, maxIdle = 5000, maxQueueSize = 100000)

  private val connection: PostgreSQLConnectionFactory = new PostgreSQLConnectionFactory(connectionConfiguration)

  private val connectionPool: ConnectionPool[PostgreSQLConnection] = new ConnectionPool[PostgreSQLConnection](connection, connectionPoolConfig)

  private def connectToDB = connectionPool.connect

//  private def disconnectFromDB = connectionPool.disconnect

  if (recreateTableIfExists) {
   logger.info("Recreating Table If Exists")
    Await.result({
      for {
        _ <- dropPlacesTable
        _ <- createTable
      } yield ()
    }, 30 seconds)
  }

  private def createTable: Future[QueryResult] = {
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
    logger.info(s"Inserting ${placeSearchResults.size} places into DB for city $city: ")
    for {
      _ <- connectToDB
      result <- Future.sequence(placeSearchResults.zipWithIndex.map{ case (result, index)  => {
          logger.info(s"Inserting place $index of ${placeSearchResults.size} into DB")
          insertPlace(result, city, placeType)
        }
      })
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
    logger.info(s"getting places for city from DB: $city")
    val query = s"SELECT * FROM ${schema.tableName} WHERE ${schema.cityName} = ?"
    for {
      _ <- connectToDB
      queryResult <- connectionPool.sendPreparedStatement(query, List(city.name))
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

  def dropPlacesTable: Future[Unit] = {
    logger.info("Dropping places table")
    val query = s"DROP TABLE IF EXISTS ${schema.tableName}"
    connectionPool.sendQuery(query).map(_ => ())
  }
}