package com.example

import com.example.spark.SparkSessionLocal
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Employee(name: String, role: String, salary: Integer)

class DatasetEncodersTest extends AnyFlatSpec with Matchers  with SparkSessionLocal {

  behavior of "using encoders"

  import DataFixture._

  it should "use explicit encoder" in  {
    val employeeEncoder = Encoders.product[Employee]
    val employeeEncoder1 = ExpressionEncoder[Employee]()

    val df = spark.createDataset(data)(employeeEncoder)
    df.show()
  }

  it should "use implicit encoder" in {
    import spark.implicits.newProductEncoder
    val df = spark.createDataset(data)
    df.show()
  }

  it should "use localSeqToDatasetHolder" in  {
    import spark.implicits.localSeqToDatasetHolder
    implicit val employeeEncoder: Encoder[Employee] = Encoders.product[Employee]
    val df = localSeqToDatasetHolder(data).toDS()
    df.show()
  }

  it should "use custom implicit" in {
    import implicitFunctions._
    data.show()
  }
}

object DataFixture {
  val data = Seq(Employee("John", "Data scientist", 4500),
    Employee("James", "Data engineer", 3200),
    Employee("Laura", "Data scientist", 4100),
    Employee("Ali", "Data engineer", 3200),
    Employee("Steve", "Developer", 3600))
}

object implicitFunctions extends SparkSessionLocal {
  implicit def toDataset(dt : Seq[Employee]): Dataset[Employee] = {
    val ee = Encoders.product[Employee]
    spark.createDataset(dt)(ee)
  }
}
