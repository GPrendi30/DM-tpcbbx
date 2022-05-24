package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}

class Q12(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val fewStoreSales = storeSales.limit(1000)
    fewStoreSales
  }
}
