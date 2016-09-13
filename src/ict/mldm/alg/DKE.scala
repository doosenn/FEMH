package ict.mldm.alg

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Created by Zorro on 16/6/12.
  */
class DKE (pseq : ArrayBuffer[(Long, ArrayBuffer[Int])], ppivot : Int, pmtd : Int) {
  private val pivot = ppivot
  private val seq = pseq
  private val mtd = pmtd
  private val result = new ArrayBuffer[(String, Array[String])]()

  def this(seq : ArrayBuffer[(Long, ArrayBuffer[Int])], mtd : Int) = {
    this(seq, 0, mtd)
  }
  
  def mine() = {
    calOccs()
    this.result.filter(_._1.split("->").contains(pivot)).map(ep => (ep._1, ep._2.length))
  }
  
  def mine2() = {
    calOccs()
    this.result.flatMap{case (ep, occs) => for(occ <- occs) yield (ep, occ)}
  }
  
  def calOccs() = {
    val f1 = this.seq.flatMap(x=>{
      val temp = new ArrayBuffer[(Int, Long)]()
      for(item <- x._2) {
        temp += ((item, x._1))
      }
      temp
    }).groupBy(_._1).map(x => {
      val occs = for(e <- x._2) yield e._2+":"+e._2
      (x._1.toString, occs.toArray)
    })
    
    for(one <- f1) {
      this.result += one
      this.result ++= recursivefn(one._1, one._2, f1.toArray)
    }
  }

  def recursivefn(ap: String, mo_ap: Array[String], f1: Array[(String, Array[String])]): Array[(String, Array[String])] = {
    val aps = new ArrayBuffer[(String, Array[String])]()
    if(ap.split("->").length < this.mtd) {
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
  
  def computeMO(mo_ap : Array[String], mo_E : Array[String]) = {
    val mo_a = new ArrayBuffer[String]()
    var i = 0
    var j = 0
    while(i < mo_ap.length && j < mo_E.length) {
      val ts = mo_ap(i).split(":")(0).toLong
      val te = mo_ap(i).split(":")(1).toLong
      var tE = mo_E(j).split(":")(1).toLong

      while(tE <= te && j < mo_E.length) {
        tE = mo_E(j).split(":")(1).toLong
        j+=1
      }

      var i1 = -1
      if(tE > te && tE - ts <= this.mtd) {
        i1 = i + 1
        breakable{
          while(i1 < mo_ap.length) {
            val te1 = mo_ap(i1).split(":")(1).toLong
            if(te1 >= tE) {
              break
            }
            i1 += 1
          }
        }
        if(i1 != -1) {
          val ts2 = mo_ap(i1-1).split(":")(0).toLong
          mo_a += (ts2+":"+tE)
        }
      }
      if(i1 != -1) {
        i = i1
      }
      else {
        i += 1
        if(j > 0) {
          j -= 1
        }
      }
    }
    mo_a.toArray
  }
  
}
