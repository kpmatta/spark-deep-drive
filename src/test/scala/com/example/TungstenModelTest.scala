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
    println(myClassEncoder)
    // class[name[0]: string, id[0]: int, role[0]: string]

    val toRow = myClassEncoder.createSerializer()

    val myObj = MyClass("John", 7, "developer")
    val internalRow = toRow(myObj)
    println(internalRow)
    // [0,2000000004,7,2800000009,6e686f4a,65706f6c65766564,72]

    val unsafeRow = internalRow.asInstanceOf[UnsafeRow]
    val byteArr = unsafeRow.getBytes.toList
    println(byteArr)
    // |<-- 8 bits null bit -->|<-- Fixed length -->|<-- Variable length values region -->|
    // List(
    // 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 32, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0,
    // 9, 0, 0, 0, 40, 0, 0, 0, 74, 111, 104, 110, 0, 0, 0, 0, 100, 101, 118, 101, 108, 111, 112, 101,
    // 114, 0, 0, 0, 0, 0, 0, 0
    // )
  }
}
