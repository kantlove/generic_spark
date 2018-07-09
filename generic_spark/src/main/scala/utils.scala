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
    type JoinResult[L, R] = (Option[L], Option[R])
    type LeftJoinResult[L, R] = (L, Option[R])

    val ds: Dataset[A]

    /**
      * Do left join on this Dataset. For more details, please refer to [[common.spark.Joiner#safeJoin]]
      */
    def leftJoin[B, K](other: Dataset[B])(leftKey: A => K, rightKey: B => K)
      (implicit e1: Encoder[(K, A)], e2: Encoder[(K, B)], e3: Encoder[JoinResult[A, B]], e4: Encoder[LeftJoinResult[A, B]])
    : Dataset[LeftJoinResult[A, B]] = {
      safeJoin(other, "left")(leftKey, rightKey).map(rs => (rs._1.get, rs._2))
    }

    /**
      * Same as `joinWith` but allows a safer join condition because we don't have to refer to
      * column names. For now, only equi-join is supported, for non-equi-join (e.g. >=, <), please use
      * `joinWith` or `join` (from DataFrame).
      *
      * Example:
      * {{{
      *   import spark.implicits._ // for the implicit Encoders
      *   users.safeJoin(purchases, "left")(user => user.id, purchase => purchase.buyerId)
      * }}}
      *
      * Please use [[common.spark.Joiner#innerJoin]] and [[common.spark.Joiner#leftJoin]] method for more convenient syntax.
      *
      * @param leftKey  a function used to convert rows of the left table to a type that can be compared.
      * @param rightKey a function used to convert rows of the right table to a type that can be compared.
      * @tparam K type of the "common" key that will be used in join condition.
      */
    private def safeJoin[B, K]
    (other: Dataset[B], joinType: String)
      (leftKey: A => K, rightKey: B => K)
      (implicit e1: Encoder[(K, A)], e2: Encoder[(K, B)], e3: Encoder[JoinResult[A, B]])
    : Dataset[JoinResult[A, B]] = {
      // Attach the keys to both sides
      val left = ds.map(r => (leftKey(r), r))
      val right = other.map(r => (rightKey(r), r))

      left
        .joinWith(right, left("_1") === right("_1"), joinType)
        .map { rs => (Option(rs._1).map(_._2), Option(rs._2).map(_._2)) } // rows that satisfy the join condition from both sides
    }
  }
}
