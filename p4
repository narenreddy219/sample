package com.citi.icg.gru.quickrec.common.utils

import com.citi.icg.gru.quickrec.common.domain.jobs.fileTransform.AdvanceTransformation
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object AdvanceUdfUtilTest extends App {

  private val schema = StructType(
    Seq(
      StructField("VALUE_AMOUNT", StringType, nullable = true)
    )
  )

  /*
   * Keep [VALUE_AMOUNT].
   * AdvanceUdfUtil extracts and replaces this UI column placeholder.
   */
  private val uiExpression =
    """![VALUE_AMOUNT].trim().equals("")
      |&& ![VALUE_AMOUNT].trim().equalsIgnoreCase("null")
      |? new java.math.BigDecimal(
      |    [VALUE_AMOUNT]
      |      .replace("$", "")
      |      .replace(",", "")
      |      .trim()
      |  ).setScale(2, java.math.RoundingMode.HALF_UP)
      |: """"".stripMargin.replace("\n", " ")

  private def createRow(value: String): GenericRowWithSchema = {
    new GenericRowWithSchema(
      Array[Any](value),
      schema
    )
  }

  // Initialize values required internally by AdvanceUdfUtil.
  AdvanceUdfUtil.advanceTransformation = new AdvanceTransformation()
  AdvanceUdfUtil.columns = null
  AdvanceUdfUtil.specialDate = null
  AdvanceUdfUtil.feedFileName = "AdvanceUdfUtilTest"

  val extractedColumns =
    AdvanceUdfUtil.extractColumnNames(uiExpression)

  assert(
    extractedColumns.contains("VALUE_AMOUNT"),
    "AdvanceUdfUtil did not extract VALUE_AMOUNT"
  )

  val testCases = Seq(
    ("$1,234.56",       "1234.56"),
    (" $7,890.12 ",     "7890.12"),
    ("$0.00",           "0.00"),
    ("  $0.00  ",       "0.00"),
    ("$123456789.99",   "123456789.99"),
    (" $987654321.01 ", "987654321.01"),
    ("$-1234.56",       "-1234.56"),
    (" $-7890.12 ",     "-7890.12"),
    ("$-0.00",          "0.00"),
    ("",                 "")
  )

  testCases.foreach {
    case (inputValue, expectedValue) =>

      val row = createRow(inputValue)

      val actualValue =
        AdvanceUdfUtil.getEvalExpression(
          row,
          uiExpression,
          "AdvanceUdfUtilTest",
          false
        )

      assert(
        actualValue == expectedValue,
        s"FAILED: input=[$inputValue], expected=[$expectedValue], actual=[$actualValue]"
      )

      println(
        s"PASSED: input=[$inputValue], result=[$actualValue]"
      )
  }

  println("All AdvanceUdfUtil test cases passed.")
}
