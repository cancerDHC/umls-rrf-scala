package org.renci.umls.rrf

import java.io.File

import scala.io.Source

class RRFDir(dir: File) {
  def getFile(filename: String): File = {
    val file = new File(dir, filename)

    if (!file.exists()) throw new RuntimeException(s"Directory ${dir.getCanonicalPath} does not contain expected file $filename.")

    file
  }

  val releaseInfo: String = Source.fromFile(getFile("release.dat")).mkString
}
