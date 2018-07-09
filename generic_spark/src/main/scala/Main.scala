package generic_spark

import org.apache.spark.sql.Dataset
import shapeless._
import utils._

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

    getUserRevenue2(users, txns).show()

    spark.stop()
  }

  object getUserRevenue2 {
    object getKey extends Poly1 {
      implicit val userCase: Case.Aux[User, String] = at((u: User) => u.name)
      implicit val txnCase: Case.Aux[Txn, String] = at((t: Txn) => t.user_name)
    }

    def apply(users: Dataset[User], txns: Dataset[Txn]): Dataset[Revenue] =
      getUserRevenueGeneric(users, txns, getKey) { (user, revenue) =>
        Revenue(user.name, revenue)
      }
  }

  def getUserRevenueGeneric[U, T <: HasAmount, Out]
  (users: Dataset[U], txns: Dataset[T], getKey: Poly)
  (output: (U, Int) => Out)
  : Dataset[Out] = {
    users
      .leftJoin(txns)(u => getKey(u), t => getKey(t)) // returns Dataset[(U, Option[T])]

      .groupByKey { case (u, t) => getKey(u) }
      .flatMapGroups { case (_, iter) =>
        val group = iter.toSeq
        val _users = group.map(_._1)
        val maybeTxns = group.map(_._2)

        val revenue = maybeTxns.flatten.map(_.amount).sum

        Seq(output(_users.head, revenue))
      }
  }

  def getUserRevenue(users: Dataset[User], txns: Dataset[Txn]): Dataset[Revenue] = {
    users
      .leftJoin(txns)(_.name, _.user_name) // returns Dataset[(User, Option[Txn])]

      .groupByKey { case (u, t) => (u.name) }
      .flatMapGroups { case (_, iter) =>
        val group = iter.toSeq
        val _users = group.map(_._1)
        val maybeTxns = group.map(_._2)

        val revenue = maybeTxns.flatten.map(_.amount).sum

        Seq(Revenue(_users.head.name, revenue))
      }
  }
}
