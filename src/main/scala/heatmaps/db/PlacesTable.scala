package heatmaps.db

import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException
import com.google.maps.model.{LatLng, PlaceType, PlacesSearchResult}
import heatmaps.config.{ConfigLoader, MetricsConfig}
import heatmaps.models.{LatLngRegion, Place, PlaceSubType}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}


class PlacesTable(val db: DB[PostgreSQLConnection], val schema: PlaceTableSchema, createNewTable: Boolean = false)(implicit ec: ExecutionContext) extends Table[PostgreSQLConnection] {

  if (createNewTable) {
    Await.result({
      logger.info(s"Creating new table ${schema.tableName}")
      for {
        _ <- dropTable
        newTable <- createTable
      } yield newTable
    }, 1 minute) //Blocks while table created

  }

  override def createTable: Future[QueryResult] = {
    logger.info(s"Creating Table ${schema.tableName}")
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendQuery(
        s"""
           |CREATE TABLE IF NOT EXISTS
           |${schema.tableName} (
           |    ${schema.placeId} varchar NOT NULL,
           |    ${schema.placeType} varchar NOT NULL,
           |    ${schema.placeName} varchar,
           |    ${schema.placeSubType} varchar,
           |    ${schema.latLngRegion} varchar,
           |    ${schema.lat} real NOT NULL,
           |    ${schema.lng} real NOT NULL,
           |    ${schema.lastUpdated} timestamp NOT NULL,
           |    PRIMARY KEY(${schema.primaryKey.mkString(",")})
           |);
        """.stripMargin)
    } yield queryResult
  }

  def insertPlaces(placeSearchResults: List[PlacesSearchResult], latLngRegion: LatLngRegion, placeType: PlaceType): Future[Unit] = {
    logger.info(s"Inserting ${placeSearchResults.size} places into DB for latLngRegion $latLngRegion and type $placeType ")
    for {
      _ <- db.connectToDB
      result <- Future.sequence(placeSearchResults.zipWithIndex.map { case (result, index) => {
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

    db.connectionPool.sendPreparedStatement(statement, List(placeSearchResult.placeId, placeType, latLngRegion.toString, placeSearchResult.geometry.location.lat, placeSearchResult.geometry.location.lng))
    .recoverWith {
      case ex: GenericDatabaseException if ex.errorMessage.message.contains("duplicate key value") =>
        logger.error("Database exception - already exists. Ignoring. Error message: " +  ex.errorMessage, ex)
        Future.successful(new QueryResult(0, "")) // Ignores and returns an empty query result
      case ex =>
        logger.info(s"Unhandled exception on insert place.")
        Future.failed(ex)
    }
  }

  def getPlacesForLatLngRegions(latLngRegions: List[LatLngRegion], placeType: PlaceType): Future[List[Place]] = {
    if (latLngRegions.isEmpty) Future(List.empty)
    else {
      logger.info(s"getting places for latLngRegions $latLngRegions from DB")
      val query =
        s"SELECT * " +
        s"FROM ${schema.tableName} " +
        s"WHERE ${schema.latLngRegion} IN (${latLngRegions.map(str => s"'${str.toString}'").mkString(",")}) " +
        s"AND ${schema.placeType} = ?"
      for {
        _ <- db.connectToDB
        queryResult <- db.connectionPool.sendPreparedStatement(query, List(placeType.name()))
      } yield {
        queryResult.rows match {
          case Some(resultSet) => resultSet.map(res => {
            val latLngRegStr = res.apply(schema.latLngRegion).asInstanceOf[String].split(",")
            Place(
              res.apply(schema.placeId).asInstanceOf[String],
              Option(res.apply(schema.placeName).asInstanceOf[String]),
              res.apply(schema.placeType).asInstanceOf[String],
              Option(res.apply(schema.placeSubType).asInstanceOf[String]),
              new LatLng(res.apply(schema.lat).asInstanceOf[Float].toDouble, res.apply(schema.lng).asInstanceOf[Float].toDouble),
              LatLngRegion(latLngRegStr(0).toInt, latLngRegStr(1).toInt)
            )
          }).toList
          case None => List.empty
        }
      }
    }
  }

  def countPlacesForLatLngRegion(latLngRegion: LatLngRegion, placeType: PlaceType): Future[Long] = {
    logger.info(s"getting count of places for latLngRegions $latLngRegion from DB")
    val query =
      s"SELECT COUNT(*) " +
        s"FROM ${schema.tableName} " +
        s"WHERE ${schema.latLngRegion} = ? " +
        s"AND ${schema.placeType} = ?"
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendPreparedStatement(query, List(latLngRegion.toString, placeType.name()))
    } yield {
      queryResult.rows match {
        case Some(resultSet) => resultSet.map(res => {
         res.apply("count").asInstanceOf[Long]
        }).toList.headOption.fold(throw new RuntimeException("Unable to get count of places from DB"))(identity)
        case None => throw new RuntimeException("Unable to get count of places from DB")
      }
    }
  }

  def updatePlace(placeID: String, placeType: String, name: String): Future[QueryResult] = {
    logger.info(s"updating place $placeID, $name in DB")
    val statement =
      s"""
         |
         |UPDATE ${schema.tableName}
         |SET ${schema.placeName} = ?, ${schema.lastUpdated} = 'now'
         |WHERE ${schema.placeId} = ?
         |AND ${schema.placeType} = ?
         |
      """.stripMargin

    db.connectionPool.sendPreparedStatement(statement, List(name, placeID, placeType))
  }

  def getLatLngRegionsContainingNullPlaceNames(placeType: PlaceType): Future[List[LatLngRegion]] = {
    logger.info(s"getting latLngRegions containing null place names")
    val query = s"SELECT DISTINCT(${schema.latLngRegion}) " +
      s"FROM ${schema.tableName} " +
      s"WHERE ${schema.placeType} = ? " +
      s"AND ${schema.placeName} IS NULL " +
      s"ORDER BY ${schema.latLngRegion} ASC"
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendPreparedStatement(query, List(placeType.name()))
    } yield {
      queryResult.rows match {
        case Some(resultSet) => resultSet.map(res => {
          val regionStr = res.apply(schema.latLngRegion).asInstanceOf[String]
          LatLngRegion(
            regionStr.split(",")(0).toInt,
            regionStr.split(",")(1).toInt
          )
        }).toList
        case None => List.empty
      }
    }
  }

  def getActiveLatLngRegions: Future[Set[LatLngRegion]] = {
    logger.info(s"getting latLngRegions")
    val query =
      s"SELECT DISTINCT(${schema.latLngRegion}) " +
      s"FROM ${schema.tableName} " +
      s"ORDER BY ${schema.latLngRegion} ASC"
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendPreparedStatement(query)
    } yield {
      queryResult.rows match {
        case Some(resultSet) => resultSet.map(res => {
          val regionStr = res.apply(schema.latLngRegion).asInstanceOf[String]
          LatLngRegion(
            regionStr.split(",")(0).toInt,
            regionStr.split(",")(1).toInt
          )
        }).toSet
        case None => Set.empty
      }
    }
  }

  def updateSubtypes(subType: PlaceSubType): Future[List[QueryResult]] = {
    logger.info(s"Updating subtype ${subType.name} in place type ${subType.parentType} for places starting with ${subType.searchMatches}")
    val statement =
      s"""
         |
         |UPDATE ${schema.tableName}
         |SET ${schema.placeSubType} = ?, ${schema.lastUpdated} = 'now'
         |WHERE ${schema.placeType} = ?
         |AND UPPER(${schema.placeName}) LIKE ?
         |
      """.stripMargin

    Future.sequence(subType.searchMatches.map { searchMatch =>
      db.connectionPool.sendPreparedStatement(statement, List(subType.name, subType.parentType.name(), searchMatch.toUpperCase + "%"))
    })
  }
}