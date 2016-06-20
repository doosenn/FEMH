package ict.mldm.alg

import ict.mldm.util.{Transaction, TreeNode}
import org.apache.commons.lang.StringUtils

import scala.collection.mutable.{ArrayBuffer, HashSet}

/**
  * Created by Zorro on 16/6/11.
  */
class MESELO {
  private var minSupport = 0
  private var mtd = 0
  private var delta = 0
  private var flistSize = 0
  private var flist_keys : Array[Int] = null

  private var Q = new HashSet[String]()
  private var treeList = new ArrayBuffer[Array[TreeNode]]()

  def this(minSupport: Int, mtd : Int, delta : Int, flist : Array[Int]) = {
    this()
    this.minSupport = minSupport
    this.mtd = mtd
    this.delta = delta
    this.flist_keys = flist
    this.flistSize = flist.length
  }

  def mineTransaction(t : Transaction) = {
    val seq = t.getSeq
    val seq_encoded = seq.groupBy(_._2).map(x => {
      val items = for(i <- x._2) yield i._1
      (x._1, items.toArray)
    })
    for(ses <- seq_encoded){
      val tmpS = this.Q
      this.Q = new HashSet[String]()
      val trees = buildTree(ses._2, ses._1).toArray
      if(trees != null)
        this.treeList += trees
      if(this.treeList.nonEmpty)
        this.Q = updateTrees(this.treeList, ses._2, ses._1)

    }
  }

  def updateTrees(treeList: ArrayBuffer[Array[TreeNode]], ses : Array[Int], time : Int) = {
    if(ses.nonEmpty) {
      var i = treeList.size - 2
      while(i >= 0) {
        var tree = treeList(i)
        val tmp = new ArrayBuffer[TreeNode]()
        for(nodeOnTree <- tree) {
          val prefix = nodeOnTree.getEpisode
          for(event <- ses){
            if(nodeOnTree.isLastMO){
              val index = nodeOnTree.getIndexInFlist
              if(!nodeOnTree.getExists(index)) {
                nodeOnTree.getExists(index) = true
                val newLeafNode = new TreeNode(event, time, this.flistSize, this.flist_keys.indexOf(event))
                val array : Array[AnyRef] = Array(prefix, event.toString)
                val newEpisode = StringUtils.join(array, "->")
                newLeafNode.setEpisode(newEpisode)
                tmp += newLeafNode
                this.Q.add(newEpisode)
              }
            }
          }
          if(this.Q.contains(prefix))
            nodeOnTree.setLastMO(false)
        }
        tree ++= tmp
        i-=1
      }
    }
    this.Q
  }

  def buildTree(ses : Array[Int], time : Int) = {
    if(ses.length == 0)
      null
    else{
      val ret = new ArrayBuffer[TreeNode]()
      for(item <- ses){
        val node  = new TreeNode(item, time, this.flistSize, this.flist_keys.indexOf(item))
        node.setEpisode(item.toString)
        ret += node
        this.Q += item.toString
      }
      ret
    }
  }

  def mine(root: TreeNode): ArrayBuffer[(String, Int, Int)] = {    //dfs with recursion
  val children = root.getChildren
    val value = root.getValue
    val time = root.getTime
    val path = new ArrayBuffer[(String, Int, Int)]()
    if(children != null){
      for(child <- children) {
        val subPath = mine(child)
        for(c <- subPath){
          val ep = value +"->"+c._1
          path += ((ep, time, c._3))
        }
      }
    }
    else{
      path += ((value.toString, time, time))
    }
    path
  }

}
