package com.example

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class MyClass(name: String, id: Int, role: String)

class TungstenModelTest extends AnyFlatSpec with Matchers{

  behavior of "using tungsten"
  it should "serialize the case class" in {
    val myClassEncoder = ExpressionEncoder[MyClass]()

    val toRow = myClassEncoder.createSerializer()

    val myObj = MyClass("John", 7, "developer")
    val internalRow = toRow(myObj)

    val unsafeRow = internalRow.asInstanceOf[UnsafeRow]
    val temp = unsafeRow.getBytes.toList

    println(myClassEncoder)
    println(internalRow)
    println(temp)
  }
}
