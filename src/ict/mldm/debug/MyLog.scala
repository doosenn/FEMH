package ict.mldm.debug

import java.io.{File, FileWriter}
import java.util.Date

/**
  * Created by Zorro on 16/6/16.
  */
class MyLog {
  private var file: String = null

  def this(file : String) = {
    this()
    this.file = file
    val f = new File(this.file)
    if(!f.exists())
      f.createNewFile()
  }

  def info(message : Any) = {
    val fw = new FileWriter(new File(file), true)
    val time = new Date().toString
    fw.write(time+"\t"+message+"\n")
    fw.flush()
  }

  def write(message : Any) = {
    val fw = new FileWriter(new File(file), true)
    fw.write(message+"\n")
    fw.flush()
  }
}
