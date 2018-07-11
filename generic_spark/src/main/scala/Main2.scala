package generic_spark

import shapeless._
import shapeless.poly._
import org.apache.spark.sql.{Dataset, Encoder}
import shapeless.ops.hlist.Tupler
import utils._

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._


case class User(name: String)

case class Txn(user_name: String, amount: Int)

object Main extends SparkLocal {

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val users = Seq(User("tom"), User("bob")).toDS

    val txns = Seq(
      Txn("tom", 2),
      Txn("tom", 1)
    ).toDS

    getUserRevenue(users, txns)
  }

  object getUserRevenue {

    object getKey extends Poly1 {
      implicit val caseUser =
        at[User](u => u.name)
      implicit val caseTxn =
        at[Txn](t => (t.user_name, t.amount))
    }

    def apply(users: Dataset[User], txns: Dataset[Txn]) = {
//      f(users, getKey)
//      f(txns, getKey)
      map[User, String](users, getKey).show()
      map[Txn, (String, Int)](txns, getKey).show()

      map2(txns, getKey).as[(String, Int)].show()
      map3(txns, getKey).as[(String, Int)].show()
    }
  }

  case class map3[A, P <: Poly](ds: Dataset[A], p: P) extends Serializable {
    def as[R : Encoder](implicit cse: Case.Aux[p.type, A :: HNil, R]): Dataset[R] = {
      ds.map(u => p(u))
    }
  }

  object map2 {

    def apply2[A, P <: Poly](ds: Dataset[A], p: P) = ???

    def apply[A, P <: Poly](ds: Dataset[A], p: P) = new Runner(ds, p)

    class Runner[A, P <: Poly](ds: Dataset[A], p: P) extends Serializable {
      def as[R : Encoder](implicit cse: Case.Aux[p.type, A :: HNil, R]): Dataset[R] = {
        ds.map(u => p(u))
      }
    }
  }

  def map[U, R : TypeTag : Encoder](
    users: Dataset[U],
    p: Poly1
  )(implicit
    cse: Case.Aux[p.type, U :: HNil, R]
  ): Dataset[R] = {
    users.map(u => p(u))
  }

  def f[A, R](ds: Dataset[A], p: Poly1)(implicit cs: Case.Aux[p.type, A :: HNil, R]) = {
    ds.collect().foreach { a =>
      println(p(a))
    }
  }
}
