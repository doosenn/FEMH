package ict.mldm.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Zorro on 16/6/6.
  */
class TreeNode {
  private var value : Int = 0
  private var time : Int = 0
  private var indexInFlist = 0
  private var children : ArrayBuffer[TreeNode] = null
  private var flistSize : Int = 0
  private var exists : Array[Boolean] = null
  private var lastMO : Boolean = true
  private var episode : String = null

  def this(value: Int, time: Int, flistSize: Int, indexInFlist : Int) = {
    this()
    this.value = value
    this.time = time
    this.flistSize = flistSize
    this.indexInFlist = indexInFlist
    this.exists = new Array[Boolean](flistSize).map(_ => false)
  }

  def isChildContained(indexOfThisChildInFlist: Int) = {
    if(exists(indexOfThisChildInFlist))
      true
    else
      false
  }

  def addLChild(child : TreeNode) = {
    if(isChildContained(child.getIndexInFlist))
      children += child
  }

  def getValue = value

  def getTime = time

  def getIndexInFlist = indexInFlist

  def getChildren = children

  def getExists = exists

  def getFlistSize = flistSize

  def isLastMO = lastMO

  def getEpisode = episode

  def setValue(value: Int) = {
    this.value = value
  }

  def setTime(time: Int) = {
    this.time = time
  }

  def setIndexInFlist(index: Int) = {
    this.indexInFlist = index
  }

  def setChildren(children: ArrayBuffer[TreeNode]) = {
    this.children = children
  }

  def setFlistSize(size: Int) = {
    this.flistSize = size
  }

  def setExists(exists:Array[Boolean]) = {
    this.exists = exists
  }

  def setLastMO(lastMO: Boolean) = {
    this.lastMO = lastMO
  }

  def setEpisode(item : String) = {
    this.episode = item
  }

}
