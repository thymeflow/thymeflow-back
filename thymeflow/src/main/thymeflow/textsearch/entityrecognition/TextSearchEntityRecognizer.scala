package thymeflow.textsearch.entityrecognition

import com.typesafe.scalalogging.StrictLogging
import thymeflow.textsearch.TextSearch
import thymeflow.utilities.Memoize
import scala.concurrent.{ExecutionContext, Future}

/**
 * @author  David Montoya
 */
trait TextSearchEntityRecognizer[ENTITY] extends EntityRecognizer[ENTITY] with StrictLogging{

  def textSearch: TextSearch[ENTITY]
  implicit def executionContext: ExecutionContext


  private val defaultSearchDepth = 3
  private val matchPercent = 80
  private val cacheSize = 1000
  private val memoizedSearch = Memoize.concurrentFifoCache(cacheSize, (x:String) => textSearch.search(x, matchPercent))

  private def search(content: Seq[String])(position: ContentPosition) = {
    val query = position.mkString(content)
    memoizedSearch(query).map {
      case result => (position, result)
    }
  }

  override def recognizeEntities(tokens: Seq[String], searchDepth: Int = defaultSearchDepth, clearDuplicateNestedResults: Boolean = false): Future[Seq[(ContentPosition, Seq[(ENTITY, Float)])]] = {
    val tokensLength = tokens.length

    def searchRecursive(queries: Seq[ContentPosition]) : Future[Seq[(ContentPosition, Seq[(ENTITY, Float)])]] = {

      if(queries.isEmpty){
        Future.successful(Vector())
      }else{
        Future.sequence(queries.map{
          case contentPosition =>
            search(tokens)(contentPosition)
        }).flatMap{
          case queryResults =>
            val filteredQueryResults = queryResults.filter{
              case (_, results) => results.nonEmpty
            }

            searchRecursive(filteredQueryResults.collect{
              case (position, _) if position.count < searchDepth && position.index + position.count + 1 <= tokensLength =>
                ContentPosition(position.index, position.count + 1)
            }).map{
              case nestedResult =>
                if(clearDuplicateNestedResults){
                  val nestedEntityPositions = nestedResult.flatMap{
                    case (position, entities) =>
                      entities.map(x => (x._1, position))
                  }.groupBy(_._1)
                  filteredQueryResults.flatMap{
                    case (position, entities) =>
                      val filteredEntities = entities.filter{
                        case (entity, _)  =>
                          nestedEntityPositions.get(entity).map{
                            case (positions) =>
                              !positions.exists{
                                case (_, nestedEntityPosition) =>
                                  (nestedEntityPosition.index <= position.index) && (position.index + position.count <= nestedEntityPosition.index + nestedEntityPosition.count)
                              }
                          }.getOrElse(true)
                      }
                      if(filteredEntities.nonEmpty){
                        Some((position, filteredEntities))
                      }else{
                        None
                      }
                  } ++ nestedResult
                }else{
                  filteredQueryResults ++ nestedResult
                }
            }
        }
      }
    }

    searchRecursive(tokens.indices.map(ContentPosition(_, 1)))
  }
}

object TextSearchEntityRecognizer{
  def apply[ENTITY](textSearch: TextSearch[ENTITY])(implicit executionContext: ExecutionContext) = {
    val _textSearch = textSearch
    val _executionContext = executionContext
    new TextSearchEntityRecognizer[ENTITY]{
      override def textSearch: TextSearch[ENTITY] = _textSearch
      override implicit def executionContext: ExecutionContext = _executionContext
    }
  }
}