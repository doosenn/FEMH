package ict.mldm.alg

import ict.mldm.util.Transaction
import org.apache.commons.lang.StringUtils

import scala.collection.mutable.{ArrayBuffer, HashSet}
/**
  * Created by Zorro on 16/6/27.
  */
class PRMOD {
  private val LEFT = true
  private val RIGHT = false
  private var mtd : Int = 0
  private var maxLen : Int = 0

  def this(mtd : Int, maxLen : Int) = {
    this()
    this.mtd = mtd
    this.maxLen = maxLen
  }

  def mine(t : Transaction) = {
    val pivot = t.getPivot
    val time = t.getTime
    val seq = t.getSeq.toArray

    val occs = new ArrayBuffer[(String, (Int, Int))]()
    occs ++= expand(pivot.toString, (time, time), seq, RIGHT)
    occs ++= expand(pivot.toString, (time, time), seq, LEFT)
    occs
  }

  /*
    This method has a disadvantege, when mining a transaction like {(1,1), (2,2), (2,3), (3,4)},whose pivot is
     3 in the last index, after left-expanding we get minimal occurrence 2->3 whose window is [3, 4], while window
     [2,4] is not minimal and we abandon it. Contineously, left-expanding 2->3 [3, 4] we get 1->2->3, whose window is
      [1,3,4], this brings a bug. In traditional way, the minimal window of 1->2->3 is [1, 2, 4].
   */
  def expand(fix : String, interval : (Int, Int), array : Array[(Int, Int)], dir : Boolean):ArrayBuffer[(String, (Int, Int))] = {
    val expanded = new ArrayBuffer[(String, (Int, Int))]()
    if(StringUtils.split(fix, "->").length < maxLen){
      if(dir == LEFT) {
        val tmpSet = new HashSet[String]()
        val _left = array.filter(x => {interval._2 - x._2 <= mtd && x._2 < interval._1})
        val _right = array.filter(x => {x._2 - interval._1 <= mtd && x._2 > interval._2})
        for(t <- _left.reverse) {
          val ex = t._1 + "->" + fix
          if(!tmpSet.contains(ex)) {
            val tmp = (ex, (t._2, interval._2))
            expanded += tmp
            expanded ++= expand(tmp._1, tmp._2, array, LEFT) //para[2] must be array, not _left
            expanded ++= expand(tmp._1, tmp._2, _right, RIGHT)
          }
          tmpSet.add(ex)
        }
      }
      else {
        val tmpSet = new HashSet[String]()
        val _right = array.filter(x => {x._2 - interval._1 <= mtd && x._2 > interval._2})
        for(t <- _right) {
          val ex = fix + "->" + t._1
          if(!tmpSet.contains(ex)) {
            val tmp = (ex, (interval._1, t._2))
            expanded += tmp
            expanded ++= expand(tmp._1, tmp._2, _right, RIGHT)
          }
          tmpSet.add(ex)
        }
      }
    }
    expanded
  }

}
