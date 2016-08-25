package ict.mldm.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zuol on 16-7-2.
  */
class Transaction {
  private var pivot : Int = 0
  private var seq : ArrayBuffer[(Int, ArrayBuffer[Int])] = null
  private var pivotPos : Array[Int] = null

  def getPivot = pivot

  def getSeq = seq
  
  def getPivotPos = pivotPos

  def setPivot(pivot : Int) = {
    this.pivot = pivot
  }

  def setSeq(seq : ArrayBuffer[(Int, ArrayBuffer[Int])]) = {
    this.seq = seq
  }
  
  def setPivotPos(pivotPos : Array[Int]) = {
    this.pivotPos = pivotPos
  }

  def this(pivot: Int) = {
    this()
    this.pivot = pivot
  }

  def this(pivot: Int, seq : ArrayBuffer[(Int, ArrayBuffer[Int])], pivotPos : Array[Int]) = {
    this()
    this.pivot = pivot
    this.seq = seq
    this.pivotPos = pivotPos
  }

  def clear() = {
    this.pivot = 0
    this.seq.clear()
  }
}
