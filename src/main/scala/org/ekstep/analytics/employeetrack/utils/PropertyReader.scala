package org.ekstep.analytics.employeetrack.utils

import java.io.File
import java.io.FileInputStream
import java.util.Properties

object PropertyReader {

  val file = new File("resources/config.properties");
  val fileInput = new FileInputStream(file);
  val properties = new Properties();
  properties.load(fileInput);
  fileInput.close();

  def getProperty(key: String): String = {
    return properties.getProperty(key);
  }
}