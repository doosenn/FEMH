package ict.mldm.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zuol on 16-7-2.
  */
class SuccTransaction {
  private var pivot : Int = 0
  private var pivots : ArrayBuffer[Int] = null
  private var seq : ArrayBuffer[(Int, Int)] = null

  def getPivot = pivot

  def getPivots = pivots

  def getSeq = seq

  def setPivot(pivot : Int) = {
    this.pivot = pivot
  }

  def setPivots(pivots : ArrayBuffer[Int]) = {
    this.pivots = pivots
  }

  def setSeq(seq : ArrayBuffer[(Int, Int)]) = {
    this.seq = seq
  }

  def this(pivot: Int) = {
    this()
    this.pivot = pivot
  }

  def this(pivot: Int, pivots : ArrayBuffer[Int], seq : ArrayBuffer[(Int, Int)]) = {
    this()
    this.pivot = pivot
    this.pivots = pivots
    this.seq = seq
  }

  def clearSeq() = {
    this.seq.clear()
  }

  def clearPivots() = {
    this.pivot = 0
    this.pivots.clear()
  }

  def clear() = {
    clearSeq()
    clearPivots()
  }
}
