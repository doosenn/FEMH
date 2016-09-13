package ict.mldm.util

/**
  * Created by Zorro on 16/6/6.
  */
class TreeNode(pepisode : String, pstart : Long, pend : Long, pflistSize: Int){
  private val start : Long = pstart
  private val end : Long = pend
  private val flistSize : Int = pflistSize
  private val exists : Array[Int] = new Array[Int](flistSize).map(_ => 0)
  private val episode : String = pepisode
  private var isMinimal : Boolean = true

  private val RIGHT = false
  private val LEFT = true


  def isLeftContained(indexOfThisChildInFlist : Int) = (exists(indexOfThisChildInFlist) & 0x10) == 0x10

  def isRightContained(indexOfThisChildInFlist : Int) = (exists(indexOfThisChildInFlist) & 0x01) == 0x01

  def getStart = this.start

  def getEnd = this.end

  def getExists = this.exists

  def getFlistSize = this.flistSize

  def getEpisode = this.episode

  def isMO = this.isMinimal

  def setIsMO(tf : Boolean) = this.isMinimal = tf

  def setExists(index : Int, dir : Boolean) =
    dir match {
      case LEFT => this.exists(index) |= 0x10
      case RIGHT => this.exists(index) |= 0x01
    }

}
