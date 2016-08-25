package ict.mldm.alg

import ict.mldm.util.{Transaction, TreeNode}
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}

/**
  * Created by zuol on 16-7-3.
  */
class EMMO {
  private val LEFT = true
  private val RIGHT = false

  private var mtd = 0
  private var maxLen = 0
  private var flist_keys : Array[Int] = null
  private var pivot = 0
  private var treeList = new ArrayBuffer[ArrayBuffer[TreeNode]]()
  private val container = new ArrayBuffer[(String, String)]()
  private var episodes = new HashSet[(String, String)]()
  
  private var seq : ArrayBuffer[(Int, ArrayBuffer[Int])] = null

  def this(mtd : Int, maxLen : Int, flist_keys : Array[Int]) = {
    this()
    this.mtd = mtd
    this.maxLen = maxLen
    this.flist_keys = flist_keys
  }

  def mine(t : Transaction) = {
    val pivot = t.getPivot
    this.pivot = pivot
    val seq = t.getSeq
    this.seq = seq
    
    for((time, items) <- this.seq.reverse) {
      leftExpand(items, time)
    }
    
    for((time, items) <- this.seq) {
      rightExpand(items, time)
    }
    
    for(tree <- this.treeList) {
      calTree(tree)
    }
    
    this.container
  }

  def leftExpand(items : ArrayBuffer[Int], time : Int) = {
    val Q = new HashSet[String]()
    if(items.contains(this.pivot))
      Q += pivot.toString
    for(i <- this.treeList.indices.reverse) {
      val tmpList = this.treeList(i)
      val rootStart = tmpList(0).getStart
      val rootEnd = rootStart
//      if(rootEnd - time > this.mtd) {
//        calLTree(tmpList)
//        this.treeList.remove(i)
//      }
//      else if(time < rootStart) { //  update the whole tree
      if(time < rootStart && rootStart - time <= this.mtd) { //  update the whole tree
        for(node <- tmpList) {
          val episode = node.getEpisode
          for(item <- items) {
            if(node.isMO() && node.getEpisodeLen < this.maxLen) {
              val indexInFlist = this.flist_keys.indexOf(item)
              if(!node.isLeftContained(indexInFlist)) {
                node.setExists(indexInFlist, LEFT)
                val end = node.getEnd
                val newEpisode = item+"->"+episode
                val newNode = new TreeNode(newEpisode, time, end, this.flist_keys.length)
                tmpList += newNode
                Q += newEpisode
              }
            }
          }
          if(Q.contains(episode)) {
            node.setIsMO(false)
          }
        }
      }
    }

    if(items.contains(this.pivot)) {
      val newNode = new TreeNode(this.pivot.toString, time, time, this.flist_keys.length)
      this.treeList += ArrayBuffer[TreeNode](newNode)
    }
  }
  
  def rightExpand(items : ArrayBuffer[Int], time : Int) = {
    val Q = new HashSet[String]()
    if(items.contains(this.pivot)) {
        Q += this.pivot.toString
    }
    for(i <- this.treeList.indices) {
      val tree = this.treeList(i)
      val rootEnd = tree(0).getEnd
      if(time > rootEnd && time - rootEnd <= this.mtd) {  //update tree
        for(node <- tree) {
          val start = node.getStart
          val end = node.getEnd
          val episode = node.getEpisode
          val episodeLen = node.getEpisodeLen
          val _items = items.filter(_ != this.pivot)
          if(episodeLen < this.maxLen && time > end && time - start <= this.mtd) {
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

  def calTree(tree : ArrayBuffer[TreeNode]) = {
    for(node <- tree) {
      val episode = node.getEpisode
      val window = node.getStart + ":" + node.getEnd
      container += ((episode, window))
    }
  }

}
