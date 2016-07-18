package ict.mldm.main

import ict.mldm.alg.{DMO, EMMO, PRMOD}
import ict.mldm.util.{SuccTransaction, Transaction}
import java.io.{BufferedReader, File, InputStreamReader}
import java.net.URI

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.collection.mutable.{ArrayBuffer, HashMap}


/**
  * Created by Zorro on 2016/4/6.
  */
object FEMH{
  private var sequenceFile : String= null
  private var hierarchyFile : String = null
  private var outputFile : String = null
  private var minSupport : Int = 0
  private var mtd : Int= 0
  private var maxLen : Int = 0
  private var mode : String = "local"
  private var alg : Char = 'p'
  private var jobName : String = "FEMH"
  private var record : Boolean = false

  def main(args: Array[String]): Unit = {
    parameters(args)
    run()
  }

  def run() = {
    println("Parallel FEMH Algorithm running.")
    val conf = new SparkConf().setAppName(this.jobName)
    val sc = new SparkContext(conf)
    val b_minSupport = sc.broadcast(this.minSupport)
    val b_mtd = sc.broadcast(this.mtd)
    val b_maxLen = sc.broadcast(this.maxLen)
    val lines = sc.textFile(sequenceFile)

    //Sequence of items with format (timestamp, items)
    val sequence_rdd = lines.flatMap(line=>{
      val temp = new ArrayBuffer[(Int, String)]
      val splits = StringUtils.split(line, '\t')
      val events = StringUtils.split(splits(1), ",")
      for(e <- events)
        temp += ((splits(0).toInt, e))
      temp
    })
    //sequence_rdd.cache()

    var source : Array[String] = null
    if(this.mode.equalsIgnoreCase("local")) {
      source = readHierarchyFromLocal(hierarchyFile)   //read from local
    }
    else {
      source = readHierarchyFromHdfs(hierarchyFile)  //read from hdfs
    }

    val sourceItems = source.flatMap(StringUtils.split(_, "->"))

    //Event dictionary, a HashMap[String, Int]
    val diff_items = sequence_rdd.map(_._2).distinct().collect()
    val dictionary = getDictionary(diff_items, sourceItems)
    val reverseDic = dictionary.map(x=>(x._2, x._1))
    val b_dictionary = sc.broadcast(dictionary)

    //Hierarchy, a HashMap[Int, Int]
    val hierarchy = genHierarchy(source, dictionary)
    val b_hierarchy = sc.broadcast(hierarchy)

    //flist
    val flist = sequence_rdd.
      map(x => (b_dictionary.value(x._2), x._1)).   // item time
      flatMap(y => {
        val re = getAncestorself(y._1, b_hierarchy.value).map(z => (z, y._2))
        re
      }).
      groupByKey().
      filter(_._2.size >= b_minSupport.value).
      map(x=>(x._1, x._2.toArray.sortWith(_ < _))).
      sortBy(_._2.length, false)
    val c_flist = flist.collect()
    val b_flist = sc.broadcast(c_flist)
    val flist_keys = flist.map(_._1).collect()
    val b_flist_keys = sc.broadcast(flist_keys)

    val startTime = System.currentTimeMillis()
    var episodes : RDD[(String, Array[String])] = null
    if(this.alg == 'd' || this.alg == 'p') {  //  Local mine alg selection
      //transactions
      val transactions = flist.flatMap(x => {
        val ts = new ArrayBuffer[Transaction]()
        val pivot = x._1
        val occs = x._2
        for(occ <- occs){
          var seq = ArrayBuffer[(Int, Int)]((pivot, occ))
          val keys = b_flist_keys.value
          for(f <- b_flist.value;if keys.indexOf(pivot) >= keys.indexOf(f._1)) {
            val temp = f._2.filter(t => {
              val absolute = Math.abs(occ - t)
              if(absolute <= b_mtd.value && absolute != 0)
                true
              else
                false
                })
            seq ++= temp.map(x => (f._1, x))   //(item, time)
              }
          val t = new Transaction(pivot, occ)
          seq = seq.sortBy(_._2)
          t.setSeq(seq)
          ts += t
        }
        ts
      })

      //episode mining
      if(this.alg == 'd') {
        println("Local Mine Alg: DMO")
        episodes = transactions.flatMap(x => {
          val localMiner = new DMO(b_mtd.value, b_maxLen.value)
          val eps = localMiner.mine(x)
          eps
        }).flatMap(x => {
          val temp = new ArrayBuffer[(String, String)]()
          for(occ <- x._2) {
            temp += ((x._1, occ))
          }
          temp
        }).distinct().
          groupByKey().
          map(x => (x._1, x._2.toArray)).
          filter(_._2.length >= b_minSupport.value)
      }
      else if(this.alg == 'p') {
        println("Local Mine Alg: PRMOD")
        episodes = transactions.flatMap(x => {
          val localMiner = new PRMOD(b_mtd.value, b_maxLen.value)
          val eps = localMiner.mine(x)
          eps
        }).map(x => (x._1, x._2._1 + ":" + x._2._2)).
          groupByKey().
          map(x => {
            val occs = checkMO(x._2.toArray)
            (x._1, occs)
          }).
          filter(_._2.length >= b_minSupport.value)
      }
    }
    else if(this.alg == 'e' || this.alg == 'n') {
      val transactions = flist.flatMap(x => {
        val ts = new ArrayBuffer[SuccTransaction]()
        val pivot = x._1
        val occs = x._2.sortWith(_ < _)
        val zones = new ArrayBuffer[(Int, Int)]()
        if(occs.length == 1) {
          zones += ((occs(0), occs(0)))
        }
        else {
          var p = 0
          var start = 0
          while(p < occs.length - 1) {
            if(occs(p+1) - occs(p) > b_mtd.value) {
              zones += ((occs(start), occs(p)))
              start = p + 1
            }
            p += 1
            if(p == occs.length - 1) {
              zones += ((occs(start), occs(p)))
            }
          }
        }
        for(z <- zones) {
          var seq = new ArrayBuffer[(Int, Int)]()
          val keys = b_flist_keys.value
          for(f <- b_flist.value;if keys.indexOf(pivot) >= keys.indexOf(f._1)) {
            seq ++= f._2.filter(x => x >= z._1 - b_mtd.value && x <= z._2 + b_mtd.value).map((_, f._1))
          }
          val _seq = seq.groupBy(_._1).map(x => {
            val items = for(y <- x._2) yield y._2
            (x._1, items)
          }).toArray.sortBy(_._1)
          val t = new SuccTransaction(pivot, new ArrayBuffer ++= _seq)
          ts += t
        }
        ts
      })

      //episode mining
      if(this.alg == 'e') {
        println("Local Mine Alg: EMMO")
        episodes = transactions.flatMap(x => {
          val localMiner = new EMMO(b_mtd.value, b_maxLen.value, b_flist_keys.value)
          val eps = localMiner.mine(x)
          eps
        }).groupByKey().map(x => (x._1, x._2.toArray)).filter(_._2.length >= b_minSupport.value)
      }
      else if(this.alg == 'n') {
        println("Local Mine Alg: NOM")

      }
    }

    val endTime = System.currentTimeMillis()
    val msg = "Mine job finished in "+(endTime - startTime)+" ms."
    println(msg)
    println("Saving result..")
    if(this.record) {
      val _episodes = episodes.map(x => {
        val splits = StringUtils.split(x._1, "->")
        val _splits = for(s <- splits) yield reverseDic(s.toInt)
        var ep = ""
        for(_s <- _splits) {
          ep += _s + "->"
        }
        ep = ep.substring(0, ep.length - 2)
        var str = ep + "\tSUPPORT:\t" + x._2.length + "\n"
        for(occ <- x._2) {
          str += occ + " "
        }
        str
      })
      _episodes.repartition(1).saveAsTextFile(this.outputFile)
    }
    else{
      val _episodes = episodes.map(x => (x._1, x._2.length))
      _episodes.repartition(1).saveAsTextFile(this.outputFile)
    }
    println("Job Finished.")
  }

  def parameters(args: Array[String]) = {
    for (i <- args.indices) {
      if (args(i).equalsIgnoreCase("-i")) {
        this.sequenceFile = args(i+1)
      }
      else if (args(i).equalsIgnoreCase("-h")) {
        this.hierarchyFile = args(i+1)
      }
      else if (args(i).equalsIgnoreCase("-o")) {
        this.outputFile = args(i+1)
      }
      else if (args(i).equalsIgnoreCase("-s")) {
        this.minSupport = args(i+1).toInt
      }
      else if (args(i).equalsIgnoreCase("-w")) {
        this.mtd = args(i+1).toInt
      }
      else if (args(i).equalsIgnoreCase("-l")) {
        val temp = args(i+1).toInt
        if(temp == 0)
          this.maxLen = Int.MaxValue
        else
          this.maxLen = temp
      }
      else if (args(i).equalsIgnoreCase("-m")) {
        this.mode = args(i+1)
      }
      else if (args(i).equalsIgnoreCase("-a")) {
        this.alg = args(i+1).charAt(0).toLower
      }
      else if (args(i).equalsIgnoreCase("-j")) {
        this.jobName = args(i+1)
      }
      else if (args(i).equalsIgnoreCase("-r")) {
        if(args(i+1).equalsIgnoreCase("true")) {
          this.record = true
        }
      }
    }
  }

  def readHierarchyFromLocal(path: String) = {
    if(path == null) {
      println("Non taxonomy!")
      new Array[String](0)
    }
    else {
      val hf = new File(path)
      if (!hf.exists()) {
        println("Taxonomy not exists!")
        new Array[String](0)
      }
      else {
        val source = Source.fromFile(path).getLines().toArray
        source
      }
    }
  }

  def readHierarchyFromHdfs(path: String) = {
    if(path == null) {
      new Array[String](0)
    }
    val source = new ArrayBuffer[String]()
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(path), conf)
    val ins = fs.open(new Path(path))
    val br = new BufferedReader(new InputStreamReader(ins))
    var line:String = ""
    while(line != null){
      line = br.readLine()
      if(line != null)
        source += line
    }
    source.toArray
  }

  def getDictionary(src: Array[String], hie: Array[String]) = {
    val dic = new HashMap[String, Int]()
    var offset = 0
    for (a <- src;if !dic.contains(a)) {
      offset += 1
      dic(a) = offset
    }
    for (a <- hie;if !dic.contains(a)) {
      offset += 1
      dic(a) = offset
    }
    dic
  }

  def genHierarchy(array: Array[String], dic : HashMap[String, Int]) = {
    val hierarchy = new HashMap[Int, Int]()
    for(a <- array) {
      val splits = a.split("->")
      val childId = dic(splits(0))
      val parentId = dic(splits(1))
      hierarchy(childId) = parentId
    }
    hierarchy
  }

  def getAncestorself(child : Int, hierarchy: HashMap[Int, Int]) = {
    val re = new ArrayBuffer[Int]()
    var item = child
    while(hierarchy.contains(item)) {
      re+= hierarchy(item)
      item = hierarchy(item)
    }
    (re+=child).toArray
  }

  def checkMO(eps : Array[String]) = {
    val checkOccs = new ArrayBuffer[String]()
    val occs = eps.distinct.sortBy(_.split(":")(0).toInt)
    if(occs.length == 1) {
      checkOccs += occs(0)
    }
    else if(occs.length > 1) {
      var i = 0
      var j = 1
      while (j < occs.length) {
        val preSplits = StringUtils.split(occs(i), ":")
        val preB = preSplits(0).toLong
        val preE = preSplits(1).toLong
        val sufSplits = StringUtils.split(occs(j), ":")
        val sufB = sufSplits(0).toLong
        val sufE = sufSplits(1).toLong
        if(isContained(preB, preE, sufB, sufE)) {
          j += 1
        }
        else if(isContained(sufB, sufE, preB, preE)) {
          i = j
          j += 1
        }
        else{
          checkOccs += occs(i)
          i = j
          j += 1
        }
        if(j == occs.length) {
          checkOccs += occs(i)
        }
      }
    }
    checkOccs.toArray
  }

  def isContained(b1 : Long, e1 : Long, b2 : Long, e2 : Long) = {
    if(b1 >= b2 && e1 <= e2)
      true
    else
      false
  }

}
