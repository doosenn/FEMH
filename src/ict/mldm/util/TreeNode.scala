package ict.mldm.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Zorro on 16/6/6.
  */
class TreeNode {
  private var start : Int = 0
  private var end : Int = 0
  private var flistSize : Int = 0
  private var exists : Array[Int] = null
  private var episode : String = null
  private var isMinimal : Boolean = true

  private val RIGHT = false
  private val LEFT = true

  def this(episode : String, start : Int, end : Int, flistSize: Int) = {
    this()
    this.episode = episode
    this.start = start
    this.end = end
    this.flistSize = flistSize
    this.exists = new Array[Int](flistSize).map(_ => 0)
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

  def getWindow = (this.start, this.end)

  def getStart = this.start

  def getEnd = this.end

  def getExists = this.exists

  def getFlistSize = this.flistSize

  def getEpisode = this.episode

  def getEpisodeLen = this.episode.split("->").length

  def getNodeMsg = this.episode+"@"+this.start+":"+this.end

  def isMO() = this.isMinimal

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

  def setEpisode(item : String) = {
    this.episode = item
  }

  def setIsMO(tf : Boolean) = {
    this.isMinimal = tf
  }

}
