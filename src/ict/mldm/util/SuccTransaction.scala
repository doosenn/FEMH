package ict.mldm.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zuol on 16-7-2.
  */
class SuccTransaction {
  private var pivot : Int = 0
  private var seq : ArrayBuffer[(Int, ArrayBuffer[Int])] = null

  def getPivot = pivot

  def getSeq = seq

  def setPivot(pivot : Int) = {
    this.pivot = pivot
  }

  def setSeq(seq : ArrayBuffer[(Int, ArrayBuffer[Int])]) = {
    this.seq = seq
  }

  def this(pivot: Int) = {
    this()
    this.pivot = pivot
  }

  def this(pivot: Int, seq : ArrayBuffer[(Int, ArrayBuffer[Int])]) = {
    this()
    this.pivot = pivot
    this.seq = seq
  }

  def clear() = {
    this.pivot = 0
    this.seq.clear()
  }
}
