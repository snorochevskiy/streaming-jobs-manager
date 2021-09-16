package util.jar

import java.io.InputStream
import java.util.jar.{JarEntry, JarInputStream}

import scala.util.Using

object JarHelper {

  /**
   * Read text files in given folder inside of JAR.
   * WARNING: we search only for files directly in a given folder, not in nested sub-folders
   * @param inputStream raw input stream made from JAR, will be closed by the function
   * @param subfolder nested in JAR folder
   * @return
   */
  def readFilesInFolder(inputStream: InputStream, subfolder: String): List[(String,String)] =
    Using.resource(new JarInputStream(inputStream)) { jis =>
      readFilesInFolder(jis, subfolder)
    }


  def readFilesInFolder(jis: JarInputStream, subfolder: String): List[(String,String)]= {
    var entry: JarEntry = jis.getNextJarEntry

    // Searching for entries in a desired directory
    while (entry.ne(null) && !(entry.getName.startsWith(subfolder) && !entry.isDirectory)) {
      entry = jis.getNextJarEntry
    }

    var result: List[(String,String)] = Nil
    // Iterating over files in a found directory and read text content of each of them
    while (entry.ne(null) && entry.getName.startsWith(subfolder)) {
      val sb = new StringBuilder
      val buffer = new Array[Byte](1024)
      var readBytes = 0
      while ( {()=> readBytes = jis.read(buffer, 0, 1024); readBytes}.apply() > 0) {
        sb.append(new String(buffer, 0, readBytes))
      }
      result = (entry.getName -> sb.toString) :: result
      entry = jis.getNextJarEntry
    }
    result
  }

  def readFileInFolder(jis: JarInputStream, subfolder: String, file: String): Option[String] = {
    var entry: JarEntry = jis.getNextJarEntry

    // Searching for entries in a desired directory
    while (entry.ne(null) && !(entry.getName.startsWith(subfolder) && !entry.isDirectory)) {
      entry = jis.getNextJarEntry
    }

    while (entry.ne(null) && entry.getName.startsWith(subfolder)) {
      if (entry.getName endsWith file) {
        val sb = new StringBuilder
        val buffer = new Array[Byte](1024)
        var readBytes = 0
        while ( { () => readBytes = jis.read(buffer, 0, 1024); readBytes }.apply() > 0) {
          sb.append(new String(buffer, 0, readBytes))
        }
        return Some(sb.toString)
      }
      entry = jis.getNextJarEntry
    }
    None
  }

  val FileNameRe = """.*/(.+)""".r

  def listFilesInFolder(jis: JarInputStream, subfolder: String): List[String]= {
    var entry: JarEntry = jis.getNextJarEntry

    // Searching for entries in a desired directory
    while (entry.ne(null) && !(entry.getName.startsWith(subfolder) && !entry.isDirectory)) {
      entry = jis.getNextJarEntry
    }

    var result: List[String] = Nil
    // Iterating over files in a found directory and read text content of each of them
    while (entry.ne(null) && entry.getName.startsWith(subfolder)) {
      val FileNameRe(fileName) = entry.getName
      result = fileName :: result
      entry = jis.getNextJarEntry
    }
    result
  }

}
