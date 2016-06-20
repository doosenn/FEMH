package ict.mldm.alg

import ict.mldm.util.Transaction
import org.apache.commons.lang.StringUtils

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Zorro on 16/6/12.
  */
class DMO {
  private var mtd : Int = 0
  private var minSupport = 0
  private var maxIter = 0

  def this(mtd : Int, minSupport : Int, maxIter : Int) = {
    this()
    this.mtd = mtd
    this.minSupport = minSupport
    this.maxIter = maxIter
  }

  def mine(t : Transaction) = {
    val seq = t.getSeq
    val time = t.getTime

    val occs = calOccs(seq.toArray)
    val eps = occs.map(x => {
      val keep = new ArrayBuffer[String]()
      for(se <- x._2){
        val splits = StringUtils.split(se, ":")
        if(splits(0).toInt <= time && splits(1).toInt >= time)
          keep += se
      }
      (x._1, keep.toArray)
    })
    eps
  }

  def calOccs(seq : Array[(Int, Int)]) = {
    val occs = new ArrayBuffer[(String, Array[String])]()
    val f1 = seq.groupBy(_._1).
      map(x => (x._1.toString, for(e <- x._2) yield e._2+":"+e._2)).
      map(x => (x._1, x._2)).toArray
    var fn = f1
    for(i <- 2 to maxIter) {
      val temp = new ArrayBuffer[(String, Array[String])]
      for(e <- fn) {
        val ep = noRecursivefn(e._1, e._2, f1)
        occs ++= ep
        temp ++= ep
      }
      fn = temp.toArray
    }
    occs
  }

  def noRecursivefn(ap: String, mo_ap: Array[String], f1: Array[(String, Array[String])]) = {
    val aps = new ArrayBuffer[(String, Array[String])]()
    if(ap.split("->").length < maxIter) {
      for(one <- f1){
        val a = ap +"->"+one._1
        val mo_a = computeMO(mo_ap, one._2,a)
        if(mo_a.length > 0)
          aps += ((a, mo_a))
      }
    }
    aps.toArray
  }

  def computeMO(mo_ap: Array[String], mo_E: Array[String],a:String) = {
    val mo_a = new ArrayBuffer[String]()
    var i = 0
    var j = 0
    var tE:Long = 0
    while(i < mo_ap.length && j < mo_E.length){
      val ts = mo_ap(i).split(":")(0).toLong
      val te = mo_ap(i).split(":")(1).toLong
      var continue = true
      while(continue && j < mo_E.length){
        tE = mo_E(j).split(":")(0).toLong
        if(tE <= te)
          j+=1
        else
          continue = false
      }
      if(tE-ts <= mtd && j < mo_E.length) {
        if(i + 1 < mo_ap.length){
          val te1 = mo_ap(i+1).split(":")(1).toLong
          if(te1 >= tE) {
            mo_a += (ts+":"+tE)
          }
        }else if(i + 1 == mo_ap.length){
          mo_a += (ts+":"+tE)
        }
      }
      i+=1
    }
    mo_a.toArray
  }
}
