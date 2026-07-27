package com.citi.icg.gru.quickrec.common.utils

import com.citi.icg.gru.quickrec.common.domain.jobs.fileTransform.AdvanceTransformation
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSuite

class AdvanceUdfUtilTest extends FunSuite {

  private val schema = StructType(
    Seq(
      StructField("VALUE_AMOUNT", StringType, nullable = true)
    )
  )

  /*
   * Keep [VALUE_AMOUNT] in the expression.
   * AdvanceUdfUtil extracts bracketed column names and replaces them
   * with the value from the Spark Row before BeanShell evaluation.
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

  private def initializeAdvanceUdfUtil(): Unit = {
    /*
     * getEvalExpression calls setJavaTypedExpression, which accesses
     * advanceTransformation.isExpressionTyped.
     */
    AdvanceUdfUtil.advanceTransformation =
      new AdvanceTransformation()

    AdvanceUdfUtil.columns = null
    AdvanceUdfUtil.specialDate = null
    AdvanceUdfUtil.feedFileName = "AdvanceUdfUtilTest"
  }

  test("extract VALUE_AMOUNT from the UI expression") {
    val extractedColumns =
      AdvanceUdfUtil.extractColumnNames(uiExpression)

    assert(extractedColumns.contains("VALUE_AMOUNT"))
    assert(extractedColumns.size == 1)
  }

  test("evaluate VALUE_AMOUNT through the actual AdvanceUdfUtil") {
    initializeAdvanceUdfUtil()

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
      ("",                 ""),
      (null,               "")
    )

    testCases.foreach {
      case (inputValue, expectedValue) =>

        val row = createRow(inputValue)

        /*
         * This is the important call.
         * It executes the same method used by advanceExpressionUDF.
         */
        val actualValue =
          AdvanceUdfUtil.getEvalExpression(
            row = row,
            inputExpr = uiExpression,
            feedFileName = "AdvanceUdfUtilTest",
            isFileBaseFilter = false
          )

        assert(
          actualValue == expectedValue,
          s"""AdvanceUdfUtil test failed:
             |Input    : ${Option(inputValue).getOrElse("null")}
             |Expected : $expectedValue
             |Actual   : $actualValue
             |""".stripMargin
        )

        println(
          s"PASSED: input=[${Option(inputValue).getOrElse("null")}], " +
            s"result=[$actualValue]"
        )
    }
  }
}
