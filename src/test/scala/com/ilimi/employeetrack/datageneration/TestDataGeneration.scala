package com.ilimi.employeetrack.datageneration

import org.scalatest.FlatSpec
import scala.io.Source
import com.ilimi.employeetrack.utils.PropertyReader

class TestDataGeneration extends FlatSpec {

  "Employee Id" should "match/present" in {
    val filename = PropertyReader.getProperty("fileNameTest")

    val lines = Source.fromFile(filename).getLines
    val empiId = lines.map { x => x(0) }
    assert(empiId.toList.contains('5') == true)
  }

}