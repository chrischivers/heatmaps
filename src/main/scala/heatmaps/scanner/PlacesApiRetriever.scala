package heatmaps.scanner

import java.util.concurrent.atomic.AtomicInteger

import com.google.maps.errors.{NotFoundException, OverDailyLimitException}
import com.google.maps.model.{LatLng, PlaceType, PlacesSearchResult}
import com.google.maps.{GeoApiContext, PlacesApi}
import com.typesafe.scalalogging.StrictLogging
import googleutils.SphericalUtil
import heatmaps.config.Config
import heatmaps.metrics.MetricsLogging
import nl.grons.metrics.scala.{DefaultInstrumented, Meter}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class PlacesApiRetriever(config: Config)(implicit val executionContext: ExecutionContext) extends StrictLogging {

  private val apiKeys = Random.shuffle(config.placesApiConfig.apiKeys)
  private val activeApiKeyIndex = new AtomicInteger(0)
  val context: GeoApiContext = new GeoApiContext().setApiKey(apiKeys(activeApiKeyIndex.get()))

  private def updateExpiredApiKey(expiredKeyIndex: Int): Unit = {
    val activeKeyIndex = activeApiKeyIndex.get()
    if (activeKeyIndex == expiredKeyIndex) {
      if (activeKeyIndex + 1 < apiKeys.size) {
        activeApiKeyIndex.set(activeKeyIndex + 1)
        context.setApiKey(apiKeys(activeKeyIndex + 1))
      } else {
        logger.info("Reached end of API keys. Waiting for 1 hour before trying from beginning")
        activeApiKeyIndex.set(0)
        context.setApiKey(apiKeys.head)
        Thread.sleep(60000 * 60)
      }
    }
  }

  private val returnLimit = config.placesApiConfig.returnLimt

  def getPlaces(latLng: LatLng, radius: Int, placeType: PlaceType): Future[List[PlacesSearchResult]] = {
    for {
      placesFromApi <- getPlacesFromApi(latLng, radius, placeType)
      result <- if (placesFromApi.length > returnLimit) {
        logger.info(s"Size of response for $latLng with radius $radius exceeded $returnLimit. Size = ${placesFromApi.length}. Breaking down...")
        getPlacesFromApiIfLimitReached(latLng, radius / 2, placeType)
      } else Future(placesFromApi)
    } yield result
  }

  def getDetailsForPlaceId(placeId: String): Future[String] = {
    val apiKeyIndexInUse = activeApiKeyIndex.get()
    Future(PlacesApi.placeDetails(context, placeId).await().name)
      .recoverWith {
        case _: NotFoundException =>
          logger.error(s"Place $placeId not found in API")
          Future("NOT_FOUND")
        case _: OverDailyLimitException =>
          logger.info("Over daily limit. Changing API key")
          updateExpiredApiKey(apiKeyIndexInUse)
          Thread.sleep(2000)
          getDetailsForPlaceId(placeId)
        case ex =>
          logger.error("Unknown exception thrown", ex)
          throw ex
      }
      .map(result => {
      MetricsLogging.incrDetailsSearchRequests
      result
    })
  }

  private def getPlacesFromApi(latLng: LatLng, radius: Int, placeType: PlaceType): Future[List[PlacesSearchResult]] = {
    val apiKeyIndexInUse = activeApiKeyIndex.get()
    Future(PlacesApi.radarSearchQuery(context, latLng, radius).`type`(placeType).await().results.toList)
      .recoverWith {
        case _: OverDailyLimitException =>
          logger.info("Over daily limit. Changing API key")
          updateExpiredApiKey(apiKeyIndexInUse)
          getPlacesFromApi(latLng, radius, placeType)
        case ex =>
          logger.error("Unknown exception thrown", ex)
          throw ex
      }
      .map {list =>
      MetricsLogging.incrRadarSearchRequests
      list
    }
  }

  private def getPlacesFromApiIfLimitReached(latLng: LatLng, newRadius: Int, placeType: PlaceType): Future[List[PlacesSearchResult]] = {
    if (newRadius <= 0) Future(List.empty[PlacesSearchResult])
    else {
      val angle = 60
      val distanceToShift = newRadius / Math.sin(angle / 2)
      getPlaces(latLng, newRadius, placeType).flatMap(places => {
        Future.sequence(List(0, 60, 120, 180, 240, 300).map(angle => {
          getPlaces(SphericalUtil.computeOffset(latLng, distanceToShift, angle), newRadius, placeType)
        })).map(_.flatten ++ places)
      })
    }
  }
}