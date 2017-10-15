package las

import org.apache.spark.sql.sources.{ BaseRelation, DataSourceRegister, RelationProvider }
import org.apache.spark.sql.SQLContext

class LASDataSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = "las"

  override def createRelation(sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation =
    new LASRelation(sqlContext, parameters("path"))
}