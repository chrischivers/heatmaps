package heatmaps.db

import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException
import com.google.maps.model.{LatLng, PlacesSearchResult}
import heatmaps.models._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}


class PlacesTable(val db: DB[PostgreSQLConnection], val schema: PlaceTableSchema, createNewTable: Boolean = false)(implicit ec: ExecutionContext) extends Table[PostgreSQLConnection] {

  if (createNewTable) {
    Await.result({
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
           |    ${schema.category} varchar NOT NULL,
           |    ${schema.placeName} varchar,
           |    ${schema.company} varchar,
           |    ${schema.latLngRegion} varchar,
           |    ${schema.lat} real NOT NULL,
           |    ${schema.lng} real NOT NULL,
           |    ${schema.minZoomLevel} integer,
           |    ${schema.lastUpdated} timestamp NOT NULL,
           |    PRIMARY KEY(${schema.primaryKey.mkString(",")})
           |);
        """.stripMargin)
    } yield queryResult
  }

  def insertPlaces(placeSearchResults: List[PlacesSearchResult], latLngRegion: LatLngRegion, category: Category): Future[Unit] = {
    logger.info(s"Inserting ${placeSearchResults.size} places into DB for latLngRegion $latLngRegion and type $category ")
    for {
      _ <- db.connectToDB
      result <- Future.sequence(placeSearchResults.zipWithIndex.map { case (result, index) => {
        logger.info(s"Inserting place $index of ${placeSearchResults.size} into DB")
        insertPlace(result, latLngRegion, category)
      }
      })
    } yield result.map(_ => ())
  }

  private def insertPlace(placeSearchResult: PlacesSearchResult, latLngRegion: LatLngRegion, category: Category): Future[QueryResult] = {
    val statement =
      s"""
         |
         |INSERT INTO ${schema.tableName} (${schema.placeId}, ${schema.category}, ${schema.latLngRegion}, ${schema.lat}, ${schema.lng}, ${schema.lastUpdated})
         |    VALUES (?,?,?,?,?,'now');
         |
      """.stripMargin

    db.connectionPool.sendPreparedStatement(statement, List(placeSearchResult.placeId, category.id, latLngRegion.toString, placeSearchResult.geometry.location.lat, placeSearchResult.geometry.location.lng))
      .recoverWith {
        case ex: GenericDatabaseException if ex.errorMessage.message.contains("duplicate key value") =>
          logger.error("Database exception - already exists. Ignoring. Error message: " + ex.errorMessage, ex)
          Future.successful(new QueryResult(0, "")) // Ignores and returns an empty query result
        case ex =>
          logger.info(s"Unhandled exception on insert place.")
          Future.failed(ex)
      }
  }

  def getPlacesForLatLngRegions(latLngRegions: List[LatLngRegion], placeType: PlaceType, zoom: Option[Int] = None): Future[List[Place]] = {
    if (latLngRegions.isEmpty) Future(List.empty)
    else {
      logger.info(s"getting places for latLngRegions $latLngRegions from DB with placeType $placeType and zoom $zoom")
      val query =
        s"SELECT * " +
          s"FROM ${schema.tableName} " +
          s"WHERE ${schema.latLngRegion} IN (${latLngRegions.map(str => s"'${str.toString}'").mkString(",")}) " +
          s"AND ${getWhereFieldFor(placeType)} = '${placeType.id}' " +
          zoom.fold("")(zoom => s"AND ${schema.minZoomLevel} <= ${zoom.toString}")
      for {
        _ <- db.connectToDB
        queryResult <- db.connectionPool.sendPreparedStatement(query)
      } yield {
        queryResult.rows match {
          case Some(resultSet) =>
            logger.info(s"${resultSet.size} results returned from DB for query: $query")
            resultSet.map(res => {
              val latLngRegStr = res.apply(schema.latLngRegion).asInstanceOf[String].split(",")
              Place(
                res.apply(schema.placeId).asInstanceOf[String],
                Option(res.apply(schema.placeName).asInstanceOf[String]),
                res.apply(schema.category).asInstanceOf[String],
                Option(res.apply(schema.company).asInstanceOf[String]),
                Option(res.apply(schema.minZoomLevel).asInstanceOf[Int]),
                new LatLng(res.apply(schema.lat).asInstanceOf[Float].toDouble, res.apply(schema.lng).asInstanceOf[Float].toDouble),
                LatLngRegion(latLngRegStr(0).toInt, latLngRegStr(1).toInt)
              )
            }).toList
          case None =>
            logger.info(s"No results returned from DB for query: $query")
            List.empty
        }
      }
    }
  }

  def countPlacesForLatLngRegion(latLngRegion: LatLngRegion, placeType: PlaceType): Future[Long] = {
    logger.info(s"getting count of places for latLngRegions $latLngRegion and place type ${placeType.id} from DB")
    val query =
      s"SELECT COUNT(*) " +
        s"FROM ${schema.tableName} " +
        s"WHERE ${schema.latLngRegion} = ? " +
        s"AND ${getWhereFieldFor(placeType)} = ?"
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendPreparedStatement(query, List(latLngRegion.toString, placeType.id))
    } yield {
      queryResult.rows match {
        case Some(resultSet) => resultSet.map(res => {
          res.apply("count").asInstanceOf[Long]
        }).toList.headOption.fold(throw new RuntimeException("Unable to get count of places from DB"))(identity)
        case None => throw new RuntimeException("Unable to get count of places from DB")
      }
    }
  }

  def countPlacesForPlaceType(placeType: PlaceType): Future[Long] = {
    logger.info(s"getting count of places for place type ${placeType.id} from DB")
    val query =
      s"SELECT COUNT(*) " +
        s"FROM ${schema.tableName} " +
        s"WHERE ${getWhereFieldFor(placeType)} = ?"
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendPreparedStatement(query, List(placeType.id))
    } yield {
      queryResult.rows match {
        case Some(resultSet) => resultSet.map(res => {
          res.apply("count").asInstanceOf[Long]
        }).toList.headOption.fold(throw new RuntimeException("Unable to get count of places from DB"))(identity)
        case None => throw new RuntimeException("Unable to get count of places from DB")
      }
    }
  }

  def updatePlace(placeID: String, category: Category, name: String, zoom: Int): Future[QueryResult] = {
    logger.info(s"updating placeId: $placeID, name: $name in DB")
    val statement =
      s"""
         |
         |UPDATE ${schema.tableName}
         |SET ${schema.placeName} = ?, ${schema.minZoomLevel} = ?, ${schema.lastUpdated} = 'now'
         |WHERE ${schema.placeId} = ?
         |AND ${schema.category} = ?
         |
      """.stripMargin

    db.connectionPool.sendPreparedStatement(statement, List(name, zoom, placeID, category.id))
  }

  def getLatLngRegionsContainingNullPlaceNames(category: Category): Future[Option[List[LatLngRegion]]] = {
    logger.info(s"getting latLngRegions containing null place names")
    val query = s"SELECT DISTINCT(${schema.latLngRegion}) " +
      s"FROM ${schema.tableName} " +
      s"WHERE ${schema.category} = ? " +
      s"AND ${schema.placeName} IS NULL"
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendPreparedStatement(query, List(category.id))
    } yield {
      queryResult.rows match {
        case Some(resultSet) if resultSet.nonEmpty =>
          Some(resultSet.map(res => {
            val regionStr = res.apply(schema.latLngRegion).asInstanceOf[String]
            LatLngRegion(
              regionStr.split(",")(0).toInt,
              regionStr.split(",")(1).toInt
            )
          }).toList)
        case Some(resultSet) if resultSet.isEmpty => None
        case None => throw new RuntimeException("Unable to get next latLngRegion (null place names) from DB")
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

  def updateCompany(company: Company): Future[List[QueryResult]] = {
    logger.info(s"Updating company ${company.id} in category ${company.parentCategoryId} for places starting with ${company.searchMatches}")
    val statement =
      s"UPDATE ${schema.tableName} " +
        s"SET ${schema.company} = ?, ${schema.lastUpdated} = 'now' " +
        s"WHERE ${schema.category} = ? " +
        s"AND UPPER(${schema.placeName}) LIKE ?"

    Future.sequence(company.searchMatches.map { searchMatch =>
      logger.info(s"Updating ${company.id} for search match $searchMatch")
      db.connectionPool.sendPreparedStatement(statement, List(company.id, company.parentCategoryId, searchMatch.toUpperCase + "%"))
    })
  }

  private def getWhereFieldFor(placeType: PlaceType): String = {
    placeType match {
      case _: Category => schema.category
      case _: Company => schema.company
    }
  }
}