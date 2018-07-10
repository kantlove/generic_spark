package generic_spark

import org.apache.spark.sql.{Dataset, Encoder}
import shapeless._
import shapeless.poly._
import utils._

import scala.reflect.runtime.universe._

/**
  * Scenario:
  * - Given table of users and a table of transactions. Compute total revenue of each user.
  * - Do the same thing for each user in each country. Must re-use the code implemented above.
  *
  * Targets:
  * - abstract over join/groupBy keys
  * - abstract over return types, how to build the return row
  */

trait HasAmount {
  def amount: Int
}

case class User(name: String)

case class Txn(user_name: String, amount: Int) extends HasAmount

case class Revenue(user_name: String, revenue: Int)

object Main extends SparkLocal {

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val users = Seq(User("tom"), User("bob")).toDS

    val txns = Seq(
      Txn("tom", 2),
      Txn("tom", 1)
    ).toDS

    getUserRevenue(users, txns).show()

    spark.stop()
  }

  object getUserRevenue {

    object getKey extends Poly1 {
      implicit val caseUser: Case.Aux[User, Tuple1[String]] =
        at(u => Tuple1(u.name))
      implicit val caseTxn: Case.Aux[Txn, Tuple1[String]] =
        at(t => Tuple1(t.user_name))
    }

    def apply(users: Dataset[User], txns: Dataset[Txn]): Dataset[Revenue] = {
      getUserRevenueGeneric(users, txns, getKey) { (user, revenue) =>
        Revenue(user.name, revenue)
      }
    }
  }

  def getUserRevenueGeneric[U <: Product, T <: Product with HasAmount, K <: Product, Out <: Product](
    users: Dataset[U],
    txns: Dataset[T],
    getKey: Poly1
  )(
    output: (U, Int) => Out
  )(implicit
    // requires that `getKey` can handle `U` and `T`
    caseU: Case.Aux[getKey.type, U :: HNil, K],
    caseT: Case.Aux[getKey.type, T :: HNil, K],
    // requirements from Spark API
    e0: Encoder[K],
    t0: TypeTag[U],
    t1: TypeTag[T],
    t2: TypeTag[Out],
    t3: TypeTag[K]
  ): Dataset[Out] = {

    users
//      .joinWith[T](txns, $"name" === $"user_name", "left") // Dataset[(U, T)]
      .leftJoin(txns)(u => getKey(u), t => getKey(t)) // Dataset[(U, T)]
      .groupByKey { case (u, _) => getKey(u) }
      .flatMapGroups {
        case (_, iter) =>
          val group = iter.toSeq
          val _users = group.map(_._1)
          val maybeTxns = group.map(_._2)

          val revenue = maybeTxns.flatten.map(_.amount).sum

          Seq(output(_users.head, revenue))
      }
  }
}
