package ict.mldm.alg

import ict.mldm.util.{SuccTransaction, TreeNode}
import org.apache.commons.lang.StringUtils
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
  private val _right = new ArrayBuffer[(Int, Int)]()

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

    for((item, time) <- seq) {
      rightExpand(item, time)
    }
    treeList.clear()
    for((item, time) <- seq.reverse) {
      leftExpand(item, time)
      this._right.insert(0, (item, time))
    }

    val episodes = getResult()
    episodes
  }

  def leftExpand(item : Int, time : Int) = {
    val tmpSet = new HashSet[String]()
    val indexOfFlist = this.flist_keys.indexOf(item)
    for(i <- this.treeList.indices.reverse) { //  note reverse because of minimal occurrence
    val tmpList = this.treeList(i)
      val rootWindow = tmpList(0).getWindow
      if(rootWindow._2 - time > this.mtd) {  //  when newly-come item is beyond mtd from root pivot
        calTree(tmpList)
        this.treeList.remove(i)
      }
      else if(time < rootWindow._1) { //  update the whole tree
        for(node <- tmpList) {
          val episode = node.getEpisode
          if(StringUtils.split(episode, "->").length < this.maxLen) {
            val expand_eps = item + "->" + episode
            if(!tmpSet.contains(expand_eps) && !node.isLeftContained(indexOfFlist)) {
              node.setExists(indexOfFlist, LEFT)
              val newNode = new TreeNode(item, time, this.flist_keys.length, this.flist_keys.indexOf(item), LEFT)
              newNode.setEpisode(expand_eps)
              newNode.setEnd(rootWindow._2)
              tmpList += newNode
              node.addChild(newNode)
              rightExpandANodeInLeftBranch(newNode, _right)
              tmpSet += expand_eps
            }
          }
        }
      }
    }

    if(item == this.pivot) {  //  build a new tree
    val newNode = new TreeNode(item, time, this.flist_keys.length, this.flist_keys.indexOf(this.pivot), "root")
      newNode.setEpisode(item.toString)
      val newTree = ArrayBuffer[TreeNode](newNode)
      this.treeList += newTree
    }
  }

  def rightExpandANodeInLeftBranch(node : TreeNode, rightArray : ArrayBuffer[(Int, Int)]) = {
    val episode = node.getEpisode
    val start = node.getStart
    val end = node.getEnd
    val nodeMsg = episode + "@" + start + ":" + end
    val rightExpandSubtree = new ArrayBuffer[TreeNode]()
    if(!this.rightExpandedNode.contains(nodeMsg) && StringUtils.split(episode, "->").length < this.maxLen) {
      for(r <- rightArray) {
        val newNodes = new ArrayBuffer[TreeNode]()
        val item = r._1
        val time = r._2
        var tmp = updateNode(node, item, time)
        if(tmp != null) {
          newNodes += tmp
        }
        for(n <- rightExpandSubtree) {
          tmp = updateNode(node, item, time)
          if(tmp != null) {
            newNodes += tmp
          }
        }
        rightExpandSubtree ++= newNodes
      }
    }
    calTree(rightExpandSubtree)
  }

  def updateNode(node : TreeNode, item : Int, time : Int) = {
    var newNode : TreeNode= null
    val episode = node.getEpisode
    val start = node.getStart
    val indexOfFlist = this.flist_keys.indexOf(item)
    val expand_eps = episode + "->" + item
    val newNodeMsg = expand_eps + "@" + start + ":" + time
    val episodeLen = StringUtils.split(episode, "->").length
    if(!this.rightExpandedNode.contains(newNodeMsg) && !node.isRightContained(indexOfFlist) && episodeLen < this.maxLen) {
      newNode = new TreeNode(item, time, this.flist_keys.length, indexOfFlist, RIGHT)
      newNode.setStart(start)
      newNode.setEpisode(expand_eps)
      node.setExists(indexOfFlist, RIGHT)
      node.addChild(newNode)
      this.rightExpandedNode += newNodeMsg
    }
    newNode
  }

  def rightExpand(item : Int, time : Int) = {
    val tmpSet = new HashSet[String]()
    val indexOfFlist = this.flist_keys.indexOf(item)
    for(i <- this.treeList.indices.reverse) { //  note reverse because of minimal occurrence
      val tmpList = this.treeList(i)
      val rootWindow = tmpList(0).getWindow
      if(time - rootWindow._1 > this.mtd) {  //  when newly-come item is beyond mtd from root pivot
        calTree(tmpList)
        this.treeList.remove(i)
      }
      else if(time > rootWindow._2) { //  update the whole tree
        for(node <- tmpList) {
          val episode = node.getEpisode
          if(StringUtils.split(episode, "->").length < this.maxLen) {
            val expand_eps = episode + "->" + item
            val nodeMsg = expand_eps + "@" + rootWindow._1 + ":" + time
            if(!tmpSet.contains(expand_eps) && !node.isRightContained(indexOfFlist)) {
              node.setExists(indexOfFlist, RIGHT)
              val newNode = new TreeNode(item, time, this.flist_keys.length, this.flist_keys.indexOf(item), RIGHT)
              newNode.setEpisode(expand_eps)
              newNode.setStart(rootWindow._1)
              tmpList += newNode
              node.addChild(newNode)
              this.rightExpandedNode += nodeMsg
              tmpSet += expand_eps
            }
          }
        }
      }
    }

    if(item == this.pivot) {  //  build a new tree
      val newNode = new TreeNode(item, time, this.flist_keys.length, this.flist_keys.indexOf(this.pivot), "root")
      newNode.setEpisode(item.toString)
      val newTree = ArrayBuffer[TreeNode](newNode)
      this.treeList += newTree
      this.rightExpandedNode += (item.toString+"@"+time+":"+time)
    }
  }

  def calTree(tree : ArrayBuffer[TreeNode]) = {
    for(node <- tree) {
      val episode = node.getEpisode
      val start = node.getStart
      val end = node.getEnd
      this.container += ((episode, start.toString+":"+end))
    }
  }

  def getResult() = {
    val temp = this.container.groupBy(_._1).map(x => (x._1, x._2.length)).toArray
    this.result.clear
    this.result ++= temp
    this.result
  }

}
