package heatmaps.db

import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.google.maps.model.PlaceType
import heatmaps.config.{ConfigLoader, MetricsConfig}
import heatmaps.models.LatLngRegion
import org.joda.time.LocalDateTime

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class InProgressTable(val db: DB[PostgreSQLConnection], val schema: InProgressTableSchema, createNewTable: Boolean = false)(implicit ec: ExecutionContext) extends Table[PostgreSQLConnection] {

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
           |    ${schema.started} timestamp NOT NULL,
           |    PRIMARY KEY(${schema.primaryKey.mkString(",")})
           |);
        """.stripMargin)
    } yield queryResult
  }

  def insertRegionInProgress(latLngRegion: LatLngRegion, placeType: String): Future[QueryResult] = {
    val statement =
      s"""
         |
         |INSERT INTO ${schema.tableName} (${schema.regionName}, ${schema.placeType}, ${schema.started})
         |    VALUES (?,?,'now');
         |
      """.stripMargin

    db.connectionPool.sendPreparedStatement(statement, List(latLngRegion.toString, placeType))
  }

  def deleteRegionInProgress(latLngRegion: LatLngRegion, placeType: String): Future[QueryResult] = {
    val statement =
      s"""
         |
         |DELETE FROM ${schema.tableName}
         |WHERE ${schema.regionName} = ?
         |AND ${schema.placeType} = ?
         |
      """.stripMargin

    db.connectionPool.sendPreparedStatement(statement, List(latLngRegion.toString, placeType))
  }

  def getRegionsInProgress(placeType: PlaceType): Future[List[LatLngRegion]] = {
    logger.info(s"getting regions in progress for placeType: $placeType from DB")
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
          LatLngRegion(latLngRegion(0).toInt, latLngRegion(1).toInt)}).toList
        case None => List.empty
      }
    }
  }
}