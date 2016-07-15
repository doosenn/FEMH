package ict.mldm.alg

import ict.mldm.util.Transaction
import org.apache.commons.lang.StringUtils

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Zorro on 16/6/12.
  */
class DMO {
  private var mtd : Int = 0
  private var maxLen = 0
  private val result = new ArrayBuffer[(String, Array[String])]()

  def this(mtd : Int, maxIter : Int) = {
    this()
    this.mtd = mtd
    this.maxLen = maxIter
  }

  def mine(t : Transaction) = {
    val seq = t.getSeq.toArray
    val time = t.getTime

    calOccs(seq)
    val eps = this.result.map(x => {
      val keep = new ArrayBuffer[String]()
      for(occ <- x._2) {
        val splits = StringUtils.split(occ, ":")
        if(splits.contains(time.toString)) {
          keep += ((splits(0)+":"+splits.last))
        }
      }
      (x._1, keep.toArray)
    }).filter(_._2.length > 0)
    eps
  }

  def calOccs(seq : Array[(Int, Int)]) = {
    val f1 = seq.groupBy(_._1).
      map(x => (x._1.toString, for(e <- x._2) yield e._2.toString)).toArray
    this.result ++= f1

    for(e <- f1) {
      this.result ++= recursivefn(e._1, e._2, f1)
    }
  }

  def recursivefn(ap: String, mo_ap: Array[String], f1: Array[(String, Array[String])]): Array[(String, Array[String])] = {
    val aps = new ArrayBuffer[(String, Array[String])]()
    if(ap.split("->").length < this.maxLen) {
      for(one <- f1) {
        val a = ap +"->"+one._1
        val mo_a = computeMO(mo_ap, one._2)
        if(mo_a.length > 0) {
          aps += ((a, mo_a))
          aps ++= recursivefn(a, mo_a, f1)
        }
      }
    }
    aps.toArray
  }

  def computeMO(mo_ap: Array[String], mo_E: Array[String]) = {
    val mo_a = new ArrayBuffer[String]()
    var i = 0
    var j = 0
    var tE:Long = 0
    while(i < mo_ap.length && j < mo_E.length){
      val ts = mo_ap(i).split(":")(0).toLong
      val te = mo_ap(i).split(":").last.toLong
      var continue = true
      while(continue && j < mo_E.length) {
        tE = mo_E(j).split(":")(0).toLong
        if(tE <= te)
          j+=1
        else
          continue = false
      }
      if(tE-ts <= mtd && j < mo_E.length) {
        if(i + 1 < mo_ap.length){
          val te1 = mo_ap(i+1).split(":").last.toLong
          if(te1 >= tE) {
            mo_a += (mo_ap(i)+":"+tE)
          }
        }else if(i + 1 == mo_ap.length){
          mo_a += (mo_ap(i)+":"+tE)
        }
      }
      i+=1
    }
    mo_a.toArray
  }
}
