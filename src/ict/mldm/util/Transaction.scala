package ict.mldm.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zuol on 16-7-2.
  */
class Transaction(ppivot : Int, pseq : ArrayBuffer[(Long, ArrayBuffer[Int])]){
  private val pivot : Int = ppivot
  private val seq = pseq

  def getPivot = pivot

  def getSeq = seq

  def this(seq : ArrayBuffer[(Long, ArrayBuffer[Int])]) = this(0, seq)

}