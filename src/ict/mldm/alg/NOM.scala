package ict.mldm.alg

import ict.mldm.util.SuccTransaction
import scala.collection.mutable.ArrayBuffer
/**
  * Created by zuol on 16-7-6.
  */
class NOM {
  private var maxLen = 0
  private var mtd = 0

  def this(mtd : Int, maxLen : Int) = {
    this()
    this.mtd = mtd
    this.maxLen = maxLen
  }

  def mine(t : SuccTransaction) = {
    val seq = t.getSeq
    val pivot = t.getPivot

  }

  def combntns(seq : ArrayBuffer[(Int, Array[Int])]) = {
    val  result = new ArrayBuffer[(String, String)]()
    for(i <- 1 to this.maxLen){
      result ++= combs(seq, i)
    }
    result
  }

  def combs(seq : ArrayBuffer[(Int, Array[Int])], m : Int):ArrayBuffer[(String, String)] = {
    val P = new ArrayBuffer[(String, String)]()
    val n = seq.length
    if(m == 1) {
      val ones = seq.flatMap(x => {
        val temp = new ArrayBuffer[(String, String)]()
        for(i <- x._2) {
          temp += ((x._1.toString, i.toString+":"+i.toString))
        }
        temp
      })
      P ++= ones
    }
    else if(m <= n) {
      for(k <- 0 to n-m) {
        val start = seq(k)._1
        val suffix = combs(seq.slice(k+1, n), m-1)
        for(p <- seq(k)._2) {
          for(s <- suffix) {
            val end = s._2.split(":")(1).toInt
            if(end - start <= this.mtd) {
              P += ((p+"->"+s._1, start+":"+end))
            }
          }
        }
      }
    }
    P
  }

}
