package com.citi.icg.gru.quickrec.common.utils

import bsh.Interpreter
import com.citi.icg.gru.quickrec.common.domain.jobs.fileTransform.AdvanceTransformation
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object AdvanceUdfBeforeAfterTest extends App {

  private val schema = StructType(
    Seq(
      StructField("VALUE_AMOUNT", StringType, nullable = true)
    )
  )

  /*
   * Use the exact same expression for both tests.
   * [VALUE_AMOUNT] is a UI placeholder.
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
      |: ""
      |""".stripMargin.replace("\n", " ")

  private def createRow(value: String): GenericRowWithSchema = {
    new GenericRowWithSchema(
      Array[Any](value),
      schema
    )
  }

  private def separator(): Unit = {
    println("--------------------------------------------------")
  }

  val inputValue = "$1,234.56"

  println("Input value   : " + inputValue)
  println("UI expression : " + uiExpression)
  separator()

  // ============================================================
  // BEFORE: Simulate the Production-style failure
  // ============================================================

  println("BEFORE TEST")
  println("Passing unresolved expression directly to BeanShell")

  var expectedFailureOccurred = false

  try {
    val interpreter = new Interpreter()

    /*
     * No placeholder replacement is performed here.
     * BeanShell receives [VALUE_AMOUNT] literally.
     */
    interpreter.eval(uiExpression)

    println("UNEXPECTED: BeanShell evaluation succeeded")
  } catch {
    case ex: Exception =>
      expectedFailureOccurred = true

      println("BEFORE RESULT : FAILED AS EXPECTED")
      println("Exception type: " + ex.getClass.getName)
      println("Error message : " + ex.getMessage)
  }

  assert(
    expectedFailureOccurred,
    "Expected BeanShell to fail on unresolved [VALUE_AMOUNT]"
  )

  separator()

  // ============================================================
  // AFTER: Call the actual application utility
  // ============================================================

  println("AFTER TEST")
  println("Passing the same expression through AdvanceUdfUtil")

  /*
   * Use the same initialization that worked in your previous test.
   */
  AdvanceUdfUtil.advanceTransformation = new AdvanceTransformation()
  AdvanceUdfUtil.columns = null
  AdvanceUdfUtil.specialDate = null
  AdvanceUdfUtil.feedFileName = "AdvanceUdfBeforeAfterTest"

  try {
    val row = createRow(inputValue)

    /*
     * This is the real application call.
     *
     * It should:
     * 1. Find [VALUE_AMOUNT]
     * 2. Read VALUE_AMOUNT from the Spark row
     * 3. Replace the placeholder
     * 4. Evaluate the resolved expression
     */
    val actualResult = AdvanceUdfUtil.getEvalExpression(
      row,
      uiExpression,
      "AdvanceUdfBeforeAfterTest",
      false
    )

    val expectedResult = "1234.56"

    println("AFTER RESULT  : SUCCESS")
    println("Input value   : " + inputValue)
    println("Expected value: " + expectedResult)
    println("Actual value  : " + actualResult)

    assert(
      actualResult != null,
      "Expected a non-null result"
    )

    assert(
      actualResult.toString == expectedResult,
      "Expected [" + expectedResult +
        "] but received [" + actualResult + "]"
    )

    println("Validation    : PASSED")
  } catch {
    case ex: Exception =>
      println("AFTER RESULT  : FAILED")
      println("Exception type: " + ex.getClass.getName)
      println("Error message : " + ex.getMessage)

      throw ex
  }

  separator()
  println("BEFORE: BeanShell received unresolved [VALUE_AMOUNT] and failed.")
  println("AFTER : AdvanceUdfUtil resolved [VALUE_AMOUNT] and returned 1234.56.")
  println("Before-and-after simulation completed.")
}
