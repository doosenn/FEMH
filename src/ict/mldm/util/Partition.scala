package ict.mldm.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Zorro on 2016/4/11.
  */
class Partition {
  private var pivot = 0
  private val ts = new ArrayBuffer[Transaction]()

  def this(p : Int) = {
    this()
    pivot = p
  }

  def addTran(t: Transaction) = {
    ts += t
  }

  def getPivot = this.pivot

  def getTrans = this.ts

}
