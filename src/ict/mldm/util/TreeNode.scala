package ict.mldm.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Zorro on 16/6/6.
  */
class TreeNode {
  private var value : Int = 0
  private var start : Int = 0
  private var end : Int = 0
  private var indexInFlist = 0
  private var children : ArrayBuffer[TreeNode] = null
  private var flistSize : Int = 0
  private var exists : Array[Int] = null
  private var isLeftChildOfParent : Any = true
  private var episode : String = null

  private val RIGHT = false
  private val LEFT = true

  def this(value: Int, time : Int, flistSize: Int, indexInFlist : Int, isLeftChildOfParent : Any) = {
    this()
    this.value = value
    this.start = time
    this.end = time
    this.flistSize = flistSize
    this.indexInFlist = indexInFlist
    this.exists = new Array[Int](flistSize).map(_ => 0)
    this.children = new ArrayBuffer[TreeNode]()
    this.isLeftChildOfParent = isLeftChildOfParent
  }

  def isLeftContained(indexOfThisChildInFlist: Int) = {
    if((exists(indexOfThisChildInFlist) & 0x10) == 0x10)
      true
    else
      false
  }

  def isRightContained(indexOfThisChildInFlist : Int) = {
    if((exists(indexOfThisChildInFlist) & 0x01) == 0x01)
      true
    else
      false
  }

  def getValue = this.value

  def getWindow = (this.start, this.end)

  def getStart = this.start

  def getEnd = this.end

  def getIndexInFlist = this.indexInFlist

  def getChildren = this.children

  def getExists = this.exists

  def getFlistSize = this.flistSize

  def getIsLeftChildOfParent() = this.isLeftChildOfParent

  def getEpisode = this.episode

  def setValue(value: Int) = {
    this.value = value
  }

  def setWindow(window: (Int, Int)) = {
    this.start = window._1
    this.end = window._2
  }

  def setStart(start : Int) = {
    this.start = start
  }

  def setEnd(end : Int) = {
    this.end = end
  }

  def setIndexInFlist(index: Int) = {
    this.indexInFlist = index
  }

  def setChildren(children: ArrayBuffer[TreeNode]) = {
    this.children = children
  }

  def addChild(child : TreeNode) = {
    this.children += child
  }

  def setFlistSize(size: Int) = {
    this.flistSize = size
  }

  def setExists(index : Int, dir : Boolean) = {
    if(dir == LEFT) {
      this.exists(index) |= 0x10
    }
    else if(dir == RIGHT){
      this.exists(index) |= 0x01
    }
  }

  def clrExists(index : Int, dir : Boolean) = {
    if(dir == LEFT) {
      this.exists(index) &= 0x0f
    }
    else if(dir == RIGHT){
      this.exists(index) &= 0xf0
    }
  }

  def setIsLeftChildOfParent(value : Any) = {
    this.isLeftChildOfParent = value
  }

  def setEpisode(item : String) = {
    this.episode = item
  }

}
