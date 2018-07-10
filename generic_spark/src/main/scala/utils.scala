package generic_spark

import org.apache.spark.sql._

object utils {

  trait SparkLocal extends Silent {
    protected lazy val spark: SparkSession = {
      val s = SparkSession.builder().master("local").getOrCreate()
      s.sql("set spark.sql.caseSensitive=true")
      s.sql("set spark.sql.parquet.filterPushdown=true")
      s.sql("set spark.ui.enabled=false")
      s
    }
  }

  trait Silent {
    import org.apache.log4j
    log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)
    log4j.Logger.getLogger("akka").setLevel(log4j.Level.ERROR)
  }

  implicit class DatasetOps[A](val ds: Dataset[A]) extends Joiner[A]

  trait Joiner[A] {
    type LeftJoinResult[L, R] = (L, Option[R])

    val ds: Dataset[A]

    def leftJoin[B, K](
      other: Dataset[B]
    )(
      leftKey: A => K,
      rightKey: B => K
    )(implicit
      e1: Encoder[(K, A)],
      e2: Encoder[(K, B)],
      e4: Encoder[LeftJoinResult[A, B]]
    ): Dataset[LeftJoinResult[A, B]] = {
      // Attach the keys to both sides
      val left = ds.map(r => (leftKey(r), r))
      val right = other.map(r => (rightKey(r), r))

      left
        .joinWith(right, left("_1") === right("_1"), "left")
        .map { rs =>
          (rs._1._2, Option(rs._2).map(_._2))
        }
    }
  }
}
