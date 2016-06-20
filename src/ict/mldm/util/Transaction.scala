package ict.mldm.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Zorro on 16/6/13.
  */
class Transaction {
  private var pivot : Int = 0
  private var time : Int = 0
  private var seq : ArrayBuffer[(Int, Int)] = null

  def getPivot = pivot

  def getTime = time

  def getSeq = seq


  def setPivot(pivot : Int) = {
    this.pivot = pivot
  }

  def setTime(time : Int) = {
    this.time = time
  }

  def setSeq(seq : ArrayBuffer[(Int, Int)]) = {
    this.seq = seq
  }

  def this(pivot: Int) = {
    this()
    this.pivot = pivot
  }

  def this(pivot: Int, time : Int) = {
    this()
    this.pivot = pivot
    this.time = time
  }

}
