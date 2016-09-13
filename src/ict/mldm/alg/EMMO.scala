package ict.mldm.alg

import ict.mldm.util.TreeNode
import scala.collection.{mutable => m}

/**
  * Created by zuol on 16-7-3.
  */
class EMMO (pseq : m.ArrayBuffer[(Long, m.ArrayBuffer[Int])], ppivot : Int, pmtd : Int, pflist_keys : Array[Int]){
  private val LEFT = true
  private val RIGHT = false

  private val mtd = pmtd
  private val flist_keys : Array[Int] = pflist_keys
  private val pivot = ppivot
  private val treeList = new m.ArrayBuffer[m.ArrayBuffer[TreeNode]]()
  private val container = new m.ArrayBuffer[(String, Int)]()
  private val seq : m.ArrayBuffer[(Long, m.ArrayBuffer[Int])] = pseq


  def mine() = {
    for((time, items) <- this.seq.reverse) leftExpand(items, time)

    for((time, items) <- this.seq) rightExpand(items, time)
    
    for(tree <- this.treeList) calTree(tree)
    
    this.container
  }

  def leftExpand(items : m.ArrayBuffer[Int], time : Long) = {
    val Q = new m.HashSet[String]()
    val itemsContainPivot = items.contains(this.pivot)
    if(itemsContainPivot)
      Q += pivot.toString
    for(i <- this.treeList.indices.reverse) {
      val curTree = this.treeList(i)
      val rootStart = curTree(0).getStart
      if(time < rootStart && rootStart - time < this.mtd) { //  update the whole tree
        for(node <- curTree; if node.isMO) {
          val episode = node.getEpisode
          for(item <- items) {
            val indexInFlist = this.flist_keys.indexOf(item)
            if(!node.isLeftContained(indexInFlist)) {
              node.setExists(indexInFlist, LEFT)
              val end = node.getEnd
              val newEpisode = item+"->"+episode
              val newNode = new TreeNode(newEpisode, time, end, this.flist_keys.length)
              curTree += newNode
              Q += newEpisode
            }
          }
          if(Q.contains(episode)) {
            node.setIsMO(false)
          }
        }
      }
    }

    if(itemsContainPivot) {
      val newNode = new TreeNode(this.pivot.toString, time, time, this.flist_keys.length)
      this.treeList += m.ArrayBuffer[TreeNode](newNode)
    }
  }
  
  def rightExpand(items : m.ArrayBuffer[Int], time : Long) = {
    val Q = new m.HashSet[String]()
    if(items.contains(this.pivot)) {
        Q += this.pivot.toString
    }
    for(i <- this.treeList.indices) {
      val tree = this.treeList(i)
      val rootEnd = tree(0).getEnd
      if(time > rootEnd && time - rootEnd < this.mtd) {  //update tree
        for(node <- tree) {
          val start = node.getStart
          val end = node.getEnd
          val episode = node.getEpisode
          val _items = items.filter(_ != this.pivot)
          if(time > end && time - start < this.mtd) {
            for(item <- _items) {
              val newEpisode = episode + "->" + item
              val indexInFlist = this.flist_keys.indexOf(item)
              if(!Q.contains(newEpisode) && !node.isRightContained(indexInFlist)) {
                val newNode = new TreeNode(newEpisode, start, time, this.flist_keys.length)
                tree += newNode
                Q += newEpisode
              }
              node.setExists(indexInFlist, RIGHT)
            }
          }
        }
      }
    }
  }

  def calTree(tree : m.ArrayBuffer[TreeNode]) =
    tree.foreach(node => this.container += ((node.getEpisode, 1)))

}
