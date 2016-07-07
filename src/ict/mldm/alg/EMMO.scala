package ict.mldm.alg

import ict.mldm.util.{SuccTransaction, TreeNode}
import scala.collection.mutable.{ArrayBuffer, HashSet}

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
  private var container = new ArrayBuffer[(String, String)]()
  private var result = new ArrayBuffer[(String, Int)]()
  private var rightExpandedNode = new HashSet[String]()
//  private var leftExpandedNode = new HashSet[String]()
  private var seq : ArrayBuffer[(Int, ArrayBuffer[Int])] = null

  def this(mtd : Int, maxLen : Int, flist_keys : Array[Int]) = {
    this()
    this.mtd = mtd
    this.maxLen = maxLen
    this.flist_keys = flist_keys
  }

  def mine(t : SuccTransaction) = {
    val pivot = t.getPivot
    this.pivot = pivot
    val seq = t.getSeq
    this.seq = seq
    //right expand process
    for((time, items) <- seq) {
      rightExpand(items, time)
    }
    //unfinished right-expand-trees but the seq has ended
    for(tree <- treeList) {
      calTree(tree)
    }

    //clean the treeList
    treeList.clear()

    //left expand process
    for((time, items) <- seq.reverse) {
      leftExpand(items, time)
//      if(time == 1) {
//        println("-------------")
//        for(tree <- treeList) {
//          for(node <- tree){
//            println(node.getNodeMsg+"\t"+node.isMO())
//          }
//          println("-------------")
//        }
//      }
    }
    //unfinished left-expand-trees but the seq has ended
    for(tree <- treeList) {
      calTree2(tree)
    }

    val d = container.groupBy(_._1)
    for(dd <- d){
      print(dd._1+":\t")
      for(occ <- dd._2)
        print(occ._2+"\t")
      println
    }
  }

  def rightExpand(items : ArrayBuffer[Int], time : Int) = {
    val Q = new HashSet[String]()
    if(items.contains(this.pivot))
      Q += pivot.toString
    for(i <- this.treeList.indices.reverse) {
      val tmpList = this.treeList(i)
      val rootStart = tmpList(0).getStart
      val rootEnd = rootStart
      if(time - rootStart > this.mtd) {  //  when newly-come item is beyond mtd from root pivot
        calTree(tmpList)
        this.treeList.remove(i)
      }
      else if(time > rootEnd) { //  update the whole tree
        for(node <- tmpList) {
          val episode = node.getEpisode
          for(item <- items) {
            if(node.isMO() && node.getEpisodeLen < this.maxLen) {
              val indexInFlist = this.flist_keys.indexOf(item)
              if(!node.isRightContained(indexInFlist)) {
                node.setExists(indexInFlist, RIGHT)
                val newNode = new TreeNode(item, this.flist_keys.length, indexInFlist)
                val newEpisode = episode+"->"+item
                newNode.setWindow(node.getStart, time)
                newNode.setEpisode(newEpisode)
                tmpList += newNode
                Q += newEpisode
                this.rightExpandedNode += newNode.getNodeMsg
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
      val newNode = new TreeNode(this.pivot, this.flist_keys.length, this.flist_keys.indexOf(this.pivot))
      newNode.setWindow(time, time)
      newNode.setEpisode(pivot.toString)
      this.treeList += ArrayBuffer[TreeNode](newNode)
      this.rightExpandedNode += newNode.getNodeMsg
    }
  }

  def leftExpand(items : ArrayBuffer[Int], time : Int) = {
    val Q = new HashSet[String]()
    if(items.contains(this.pivot))
      Q += pivot.toString
    for(i <- this.treeList.indices.reverse) {
      val tmpList = this.treeList(i)
      val rootStart = tmpList(0).getStart
      val rootEnd = rootStart
      if(rootEnd - time > this.mtd) {
        calTree2(tmpList)
        this.treeList.remove(i)
      }
      else if(time < rootStart) { //  update the whole tree
        for(node <- tmpList) {
          val episode = node.getEpisode
          for(item <- items) {
            if(node.isMO() && node.getEpisodeLen < this.maxLen) {
              val indexInFlist = this.flist_keys.indexOf(item)
              if(!node.isLeftContained(indexInFlist)) {
                node.setExists(indexInFlist, LEFT)
                val newNode = new TreeNode(item, this.flist_keys.length, indexInFlist)
                val newEpisode = item+"->"+episode
                newNode.setWindow(time, node.getEnd)
                newNode.setEpisode(newEpisode)
                tmpList += newNode
                Q += newEpisode
                if(!this.rightExpandedNode.contains(newNode.getNodeMsg)) {
                  rightExpandANodeInLeftBranch(newNode)
                }
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
      val newNode = new TreeNode(this.pivot, this.flist_keys.length, this.flist_keys.indexOf(this.pivot))
      newNode.setWindow(time, time)
      newNode.setEpisode(pivot.toString)
      this.treeList += ArrayBuffer[TreeNode](newNode)
      this.rightExpandedNode += newNode.getNodeMsg
    }
  }

  def rightExpandANodeInLeftBranch(root : TreeNode) = {
    val start = root.getStart
    val end = root.getEnd
    val _right = seq.filter(x => (x._1 - start <= this.mtd && x._1 > end))
    val tree = ArrayBuffer[TreeNode](root)
    for(ses <- _right) {
      val time = ses._1
      for(node <- tree; if node.getEpisodeLen < this.maxLen) {
        val start = node.getStart
        val end = node.getEnd
        val episode = node.getEpisode
        for(item <- ses._2) {
          val indexInFlist = this.flist_keys.indexOf(item)
          if (!node.isRightContained(indexInFlist)) {
            val newEpisode = episode + "->" + item
            val newNode = new TreeNode(item, this.flist_keys.length, indexInFlist)
            newNode.setEpisode(newEpisode)
            newNode.setWindow(start, time)
            val newNodeMsg = newNode.getNodeMsg
            if (newNodeMsg.equals("1->3->3@3:7"))
              println(root.getNodeMsg)
            if (!this.rightExpandedNode.contains(newNodeMsg)) {
              node.setExists(indexInFlist, RIGHT)
              this.rightExpandedNode += newNodeMsg
              tree += newNode
            }
          }
        }
      }
    }
    tree.remove(0)
//    println("Right expand in left branch:")
//    for(node <- tree)
//      println(node.getNodeMsg)
//    println("---------------")
    calTree(tree)
  }

  def calTree(tree : ArrayBuffer[TreeNode]) = {
    for(node <- tree) {
      val episode = node.getEpisode
      val start = node.getStart
      val end = node.getEnd
      this.container += ((episode, start.toString+":"+end))
    }
  }

  def calTree2(tree : ArrayBuffer[TreeNode]) = {
    for(node <- tree) {
      val episode = node.getEpisode
      val start = node.getStart
      val end = node.getEnd
      val nodeMsg = node.getNodeMsg
      if(!this.rightExpandedNode.contains(nodeMsg)) {
        this.container += ((episode, start.toString+":"+end))
      }
    }
  }

  def getResult() = {
    val temp = this.container.groupBy(_._1).map(x => (x._1, x._2.length)).toArray
    this.result.clear
    this.result ++= temp
    this.result
  }

}
