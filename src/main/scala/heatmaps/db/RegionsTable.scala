package heatmaps.db

import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.google.maps.model.PlaceType
import heatmaps.models.LatLngRegion
import org.joda.time.LocalDateTime

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class RegionsTable(val db: DB[PostgreSQLConnection], val schema: RegionsTableSchema, createNewTable: Boolean = false)(implicit ec: ExecutionContext) extends Table[PostgreSQLConnection] {

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
           |        ${schema.placeType} varchar NOT NULL,
           |    ${schema.lastScanned} timestamp NOT NULL,
           |    PRIMARY KEY(${schema.primaryKey.mkString(",")})
           |);
        """.stripMargin)
    } yield queryResult
  }

  def insertRegion(latLngRegion: LatLngRegion, placeType: String): Future[QueryResult] = {
    val statement =
      s"""
         |
        |INSERT INTO ${schema.tableName} (${schema.regionName}, ${schema.placeType}, ${schema.lastScanned})
         |    VALUES (?,?,'now');
         |
      """.stripMargin

    db.connectionPool.sendPreparedStatement(statement, List(latLngRegion.toString, placeType))
  }

  def getRegions(placeType: PlaceType): Future[Map[LatLngRegion, LocalDateTime]] = {
    logger.info(s"getting regions for placeType: $placeType from Regions DB")
    val query = s"SELECT * " +
      s"FROM ${schema.tableName} " +
      s"WHERE ${schema.placeType} = ? " +
      s"ORDER BY ${schema.regionName} ASC"
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendPreparedStatement(query, List(placeType.name()))
    } yield {
      queryResult.rows match {
        case Some(resultSet) => resultSet.map(res => {
          val latLngRegion = res.apply(schema.regionName).asInstanceOf[String].split(",")
          val dateTime = res.apply(schema.lastScanned).asInstanceOf[LocalDateTime]
          LatLngRegion(latLngRegion(0).toInt, latLngRegion(1).toInt) -> dateTime
        }).toMap
        case None => Map.empty
      }
    }
  }
}