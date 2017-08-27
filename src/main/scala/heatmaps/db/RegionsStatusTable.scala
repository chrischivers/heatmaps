package heatmaps.db

import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.google.maps.model.{LatLng, PlaceType}
import heatmaps.models.LatLngRegion
import org.joda.time.LocalDateTime

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class RegionsStatusTable(val db: DB[PostgreSQLConnection], val schema: RegionsStatusTableSchema, createNewTable: Boolean = false)(implicit ec: ExecutionContext) extends Table[PostgreSQLConnection] {

  if (createNewTable) {
    Await.result({
      logger.info(s"Creating new table ${schema.tableName}")
      for {
        _ <- dropTable
        newTable <- createTable
      } yield newTable
    }, 1 minute) //Blocks while table created

  }

  override protected def createTable: Future[QueryResult] = {
    logger.info(s"Creating Table ${schema.tableName}")
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendQuery(
        s"""
           |CREATE TABLE IF NOT EXISTS
           |${schema.tableName} (
           |    ${schema.regionName} varchar NOT NULL,
           |    ${schema.placeType} varchar NOT NULL,
           |    ${schema.lastScanStarted} timestamp,
           |    ${schema.lastScanCompleted} timestamp,
           |    ${schema.numberPlaces} integer,
           |    PRIMARY KEY(${schema.primaryKey.mkString(",")})
           |);
        """.stripMargin)
    } yield queryResult
  }

  def bulkInsertRegionsForPlaceType(regions: List[LatLngRegion], placeType: String) = {
    logger.info(s"Inserting regions for placetype $placeType")
    Future.sequence(regions.map { region =>
      insertRegion(region, placeType)
    })
  }


  def insertRegion(latLngRegion: LatLngRegion, placeType: String): Future[QueryResult] = {
    val statement =
      s"""
         |
        |INSERT INTO ${schema.tableName} (${schema.regionName}, ${schema.placeType})
         |    VALUES (?,?);
         |
      """.stripMargin

    db.connectionPool.sendPreparedStatement(statement, List(latLngRegion.toString, placeType))
  }

  def getRegionsFor(placeType: String): Future[List[LatLngRegion]] = {
    val query =
      s"SELECT * " +
        s"FROM ${schema.tableName} " +
        s"WHERE ${schema.placeType} = ?"
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendPreparedStatement(query, List(placeType))
    } yield {
      queryResult.rows match {
        case Some(resultSet) => resultSet.map(res => {
          val latLngRegStr = res.apply(schema.regionName).asInstanceOf[String].split(",")
          LatLngRegion(latLngRegStr(0).toInt, latLngRegStr(1).toInt)
        }).toList
        case None => List.empty
      }
    }
  }

  def updateRegionScanStarted(latLngRegion: LatLngRegion, placeType: String) = {
    logger.info(s"updating region $latLngRegion to set scan started")
    val statement =
      s"""
         |
         |UPDATE ${schema.tableName}
         |SET ${schema.lastScanStarted} = 'now'
         |WHERE ${schema.regionName} = ?
         |AND ${schema.placeType} = ?
         |
      """.stripMargin

    db.connectionPool.sendPreparedStatement(statement, List(latLngRegion.toString, placeType))
  }

  def updateRegionScanCompleted(latLngRegion: LatLngRegion, placeType: String, numberPlaces: Int) = {
    logger.info(s"updating region $latLngRegion to set scan completed")
    val statement =
      s"""
         |
         |UPDATE ${schema.tableName}
         |SET ${schema.lastScanCompleted} = 'now', ${schema.numberPlaces} = ?
         |WHERE ${schema.regionName} = ?
         |AND ${schema.placeType} = ?
         |
      """.stripMargin

    db.connectionPool.sendPreparedStatement(statement, List(numberPlaces, latLngRegion.toString, placeType))
  }


  def getNextRegionToProcess: Future[(LatLngRegion, PlaceType)] = {
    logger.info(s"getting next region to process from RegionsStatus DB")
    val updateAndQuery =
      s"UPDATE ${schema.tableName} " +
        s"SET ${schema.lastScanStarted} = 'now' " +
        s"WHERE (${schema.regionName}, ${schema.placeType}) IN " +
            s"(SELECT p.${schema.regionName}, p.${schema.placeType} " +
            s"FROM ${schema.tableName} p " +
            s"WHERE p.${schema.lastScanStarted} IS NULL " +
            s"ORDER BY p.${schema.placeType} ASC, p.${schema.regionName} ASC " +
            s"LIMIT 1) " +
            s"RETURNING *"
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.inTransaction { c =>
        c.sendPreparedStatement(s"LOCK TABLE ${schema.tableName}")
          .flatMap(_ => c.sendPreparedStatement(updateAndQuery))
      }
    } yield {
      val results = queryResult.rows match {
        case Some(resultSet) => resultSet.map(res => {
          val latLngRegion = res.apply(schema.regionName).asInstanceOf[String].split(",")
          val placeType = res.apply(schema.placeType).asInstanceOf[String]
          (LatLngRegion(latLngRegion(0).toInt, latLngRegion(1).toInt), PlaceType.valueOf(placeType))
        })
        case None => throw new RuntimeException("Unable to get next region to process. Empty results set returned")
      }
      assert(results.size == 1)
      results.headOption.getOrElse(throw new RuntimeException("Empty results set returned from query"))
    }
  }
}