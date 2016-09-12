package ict.mldm.util

/**
  * Created by Zorro on 16/6/6.
  */
class TreeNode {
  private var start : Long = 0
  private var end : Long = 0
  private var flistSize : Int = 0
  private var exists : Array[Int] = null
  private var episode : String = null
  private var isMinimal : Boolean = true

  private val RIGHT = false
  private val LEFT = true

  def this(episode : String, start : Long, end : Long, flistSize: Int) = {
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

  def isMO() = this.isMinimal

  def setStart(start : Long) = {
    this.start = start
  }

  def setEnd(end : Long) = {
    this.end = end
  }

  def setExists(index : Int, dir : Boolean) = {
    if(dir == LEFT) {
      this.exists(index) |= 0x10
    }
    else if(dir == RIGHT){
      this.exists(index) |= 0x01
    }
  }

  def setIsMO(tf : Boolean) = {
    this.isMinimal = tf
  }

}
