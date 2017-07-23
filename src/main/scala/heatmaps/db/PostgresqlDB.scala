package heatmaps.db

import com.github.mauricio.async.db.pool.{ConnectionPool, PoolConfiguration}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import com.github.mauricio.async.db.{Configuration, Connection, QueryResult}
import com.google.maps.model.{LatLng, PlaceType, PlacesSearchResult}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.{City, LatLngRegion, PostgresDBConfig}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}


class PostgresqlDB(dBConfig: PostgresDBConfig, val schema: PlaceTableSchema, recreateTableIfExists: Boolean = false)(implicit ec: ExecutionContext) extends PlacesDatabase {

  private val connectionConfiguration = Configuration(
    username = dBConfig.username,
    password = Some(dBConfig.password),
    host = dBConfig.host,
    port = dBConfig.port,
    database = Some(dBConfig.dbName))

  logger.info(s"Using DB: ${dBConfig.dbName}")

  private val connectionPoolConfig = new PoolConfiguration(maxObjects = 5, maxIdle = 5000, maxQueueSize = 100000)

  private val connection: PostgreSQLConnectionFactory = new PostgreSQLConnectionFactory(connectionConfiguration)

  private val connectionPool: ConnectionPool[PostgreSQLConnection] = new ConnectionPool[PostgreSQLConnection](connection, connectionPoolConfig)

  private def connectToDB = connectionPool.connect


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
           |    ${schema.latLngRegion} varchar,
           |    ${schema.lat} real NOT NULL,
           |    ${schema.lng} real NOT NULL,
           |    ${schema.lastUpdated} timestamp NOT NULL,
           |    PRIMARY KEY(${schema.primaryKey.mkString(",")})
           |);
        """.stripMargin)
    } yield queryResult
  }

  override def insertPlaces(placeSearchResults: List[PlacesSearchResult], latLngRegion: LatLngRegion, placeType: PlaceType): Future[Unit] = {
    logger.info(s"Inserting ${placeSearchResults.size} places into DB for latLngRegion $latLngRegion and type $placeType ")
    for {
      _ <- connectToDB
      result <- Future.sequence(placeSearchResults.zipWithIndex.map{ case (result, index)  => {
          logger.info(s"Inserting place $index of ${placeSearchResults.size} into DB")
          insertPlace(result, latLngRegion, placeType.name())
        }
      })
    } yield result.map(_ => ())
  }

  private def insertPlace(placeSearchResult: PlacesSearchResult, latLngRegion: LatLngRegion, placeType: String): Future[QueryResult] = {
    val statement =
      s"""
         |
        |INSERT INTO ${schema.tableName} (${schema.placeId}, ${schema.placeType}, ${schema.latLngRegion}, ${schema.lat}, ${schema.lng}, ${schema.lastUpdated})
         |    VALUES (?,?,?,?,?,'now');
         |
      """.stripMargin

    connectionPool.sendPreparedStatement(statement, List(placeSearchResult.placeId, placeType, latLngRegion.toString, placeSearchResult.geometry.location.lat, placeSearchResult.geometry.location.lng))
  }

  override def getPlacesForLatLngRegion(latLngRegion: LatLngRegion, placeType: PlaceType): Future[List[Place]] = {
    logger.info(s"getting places for latLngRegin $latLngRegion from DB")
    val query = s"SELECT * " +
      s"FROM ${schema.tableName} " +
      s"WHERE ${schema.latLngRegion} = ? " +
      s"AND ${schema.placeType} = ?"
    for {
      _ <- connectToDB
      queryResult <- connectionPool.sendPreparedStatement(query, List(latLngRegion.toString, placeType.name()))
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

  def disconnectFromDB: Future[Connection] = {
    connectionPool.disconnect
  }
}