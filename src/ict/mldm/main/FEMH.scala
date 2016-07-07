package ict.mldm.main

import ict.mldm.alg.{DMO, EMMO, PRMOD}
import ict.mldm.debug.MyLog
import ict.mldm.util.{SuccTransaction, Transaction}
import java.io.{BufferedReader, ByteArrayOutputStream, File, FileWriter, InputStreamReader}
import java.net.URI

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

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

  private val logger = Logger.getLogger(this.getClass)
  private val mylog = new MyLog("./data/debug.info")

  def main(args: Array[String]): Unit = {
//    PropertyConfigurator.configure("log4j.properties")
//    parameters(args)
//    run()
    val flist_keys = Array[Int](1, 2, 3)
    val seq = ArrayBuffer[(Int, ArrayBuffer[Int])]((1, ArrayBuffer(2)), (3, ArrayBuffer(1,2)),
      (4, ArrayBuffer(3)), (5, ArrayBuffer(3)), (7, ArrayBuffer(1,3)), (8, ArrayBuffer(1)),
      (9, ArrayBuffer(2)), (11, ArrayBuffer(1)))
    val t = new SuccTransaction()
    t.setPivot(3)
    t.setPivots(ArrayBuffer[Int](4,5,7))
    t.setSeq(seq)
    val e = new EMMO(4, 4, flist_keys)
    e.mine(t)
  }

  def run() = {
    println("Parallel FEMH Algorithm running.\n")
    println("Initializing..")
    val conf = new SparkConf().setAppName("FEMH").setMaster(mode)
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Transaction]))
    conf.set("spark.kryoserializer.buffer.max", "1g")
    val sc = new SparkContext(conf)
    var startTime = System.currentTimeMillis()
    println("Initialized.")
    println("Step 1: Read Sequence Data From File..")
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
    var currentTime = System.currentTimeMillis()
    var timeDiff = currentTime - startTime
    println("\tFinished in " + timeDiff + " ms.")
    startTime = currentTime
    println("Step 2: Dictionary Generating..")
//    val d_sequence_rdd = sequence_rdd.collect()
//    var sequenceMsg : String = "\n"
//    for(d <- d_sequence_rdd)
//      sequenceMsg += d._1 + ":" + d._2 +"\n"
//    mylog.info(sequenceMsg)

    val source = readHierarchyFromLocal(hierarchyFile)   //read from local
    //val source = readHierarchyFromHdfs(hierarchyFile)  //read from hdfs
    val sourceItems = source.flatMap(StringUtils.split(_, "->"))

    //Event dictionary, a HashMap[String, Int]
    val diff_items = sequence_rdd.map(_._2).distinct().collect()
    val dictionary = getDictionary(diff_items, sourceItems)
    currentTime = System.currentTimeMillis()
    timeDiff = currentTime - startTime
    println("\tFinished in " + timeDiff + " ms.")
    startTime = currentTime
    println("Step 3: Taxonomy Generating..")
    val reverseDic = dictionary.map(x=>(x._2, x._1))
    val b_dictionary = sc.broadcast(dictionary)
    //writeDictionary(dictionary)

    //Hierarchy, a HashMap[Int, Int]
    val hierarchy = genHierarchy(source, dictionary)
    currentTime = System.currentTimeMillis()
    timeDiff = currentTime - startTime
    println("\tFinished in " + timeDiff + " ms.")
    startTime = currentTime
    val b_hierarchy = sc.broadcast(hierarchy)
    //writeTaxonomy(hierarchy)

    //flist
    println("Step 4: Flist Generating..")
    val flist = sequence_rdd.
      map(x => (b_dictionary.value(x._2), x._1)).   // item time
      flatMap(y => {
        val re = getAncestorself(y._1, b_hierarchy.value).map(z => (z, y._2))
        re
      }).
      groupByKey().
      filter(_._2.size >= minSupport).
      map(x=>(x._1, x._2.toArray.sortWith(_ < _))).
      sortBy(_._2.length, false)
//    val d_flist = flist.collect()
//    var flistMsg : String = "\n"
//    for(f <- d_flist){
//      flistMsg += f._1 + ":\n"
//      for(occ <- f._2){
//        flistMsg += occ +"\t"
//      }
//      flistMsg += "\n"
//    }
//    mylog.info(flistMsg)
    currentTime = System.currentTimeMillis()
    timeDiff = currentTime - startTime
    println("\tFinished in " + timeDiff + " ms.")
    startTime = currentTime
    val b_flist = sc.broadcast(flist.collect())
    val flist_keys = flist.map(_._1).collect()
    val b_flist_keys = sc.broadcast(flist_keys)

    //transactions
    println("Step 5: Transaction RDD Generating..")
    val transactions = flist.flatMap(x => {
      val ts = new ArrayBuffer[Transaction]()
      val pivot = x._1
      val occs = x._2
      for(occ <- occs){
        var seq = ArrayBuffer[(Int, Int)]((pivot, occ))
        val keys = b_flist_keys.value
        for(f <- b_flist.value;if keys.indexOf(pivot) >= keys.indexOf(f._1)) {
          seq ++= findMTDInArray(occ, f._2).map(x => (f._1, x))   //(item, event)
        }
        if(seq.length > 1) {
          val t = new Transaction(pivot, occ)
          seq = seq.sortBy(_._2)
          t.setSeq(seq)
          ts += t
        }
      }
      ts
    })

//    val transactions2 = flist.flatMap(x => {
//
//    })
    currentTime = System.currentTimeMillis()
    timeDiff = currentTime - startTime
    println("\tTransactions count: "+transactions.count())
    println("\tFinished in " + timeDiff + " ms.")
    startTime = currentTime

    //episode mining
    println("Step 6: Parallel Episode Mining..")
    val episodes = transactions.flatMap(x => {
      val eps = new ArrayBuffer[(String, String)]()

      val dmoMine = new DMO(mtd, 4)
      eps ++= dmoMine.mine(x).flatMap(x => {
        val tmp = new ArrayBuffer[(String, String)]()
        for(occ <- x._2) {
          tmp += ((x._1, occ))
        }
        tmp
      })
      eps
    }).groupByKey().
      map(x => (x._1, x._2.toArray)).
      filter(_._2.length >= minSupport)

    val display_episodes = checkMO(episodes.collect())
    currentTime = System.currentTimeMillis()
    timeDiff = currentTime - startTime
    println("\tFinished in" + timeDiff + " ms")
    println("Saving result..")
    var epsMsg = "\nFEMH results "+display_episodes.length+" :"
    mylog.write(epsMsg)
    for(e <- display_episodes) {
      epsMsg = e._1+":\t\t"
      for(occ <- e._2) {
        epsMsg += occ +"\t"
      }
      mylog.write(epsMsg)
    }

    println("Algorithm Finished.")
  }

  def parameters(args: Array[String]) = {
    for (i <- args.indices) {
      if (args(i).equalsIgnoreCase("-i")) {
        this.sequenceFile = args(i + 1)
      }
      else if (args(i).equalsIgnoreCase("-h")) {
        this.hierarchyFile = args(i + 1)
      }
      else if (args(i).equalsIgnoreCase("-o")) {
        this.outputFile = args(i + 1)
      }
      else if (args(i).equalsIgnoreCase("-s")) {
        this.minSupport = args(i + 1).toInt
      }
      else if (args(i).equalsIgnoreCase("-w")) {
        this.mtd = args(i + 1).toInt
      }
      else if (args(i).equalsIgnoreCase("-l")) {
        val temp = args(i + 1).toInt
        if(temp == 0)
          this.maxLen = Int.MaxValue
        else
          this.maxLen = temp
      }
      else if (args(i).equalsIgnoreCase("-m")) {
        this.mode = args(i + 1)
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
      source += line
    }
    source
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

  def writeDictionary(dic : HashMap[String, Int]) = {
    val dicFile = new File("./data/dictionary")
    if(dicFile.exists())
      dicFile.delete()
    dicFile.createNewFile()
    val dicWriter = new FileWriter(dicFile, true)
    val sorted = dic.toArray.sortBy(_._2)
    for(line <- sorted)
      dicWriter.write(line._2+" : "+line._1+"\n")
    dicWriter.flush()
  }

  def writeTaxonomy(taxonomy: HashMap[Int, Int]) = {
    val taxFile = new File("./data/taxonomy")
    if(taxFile.exists())
      taxFile.delete()
    taxFile.createNewFile()
    val taxWriter = new FileWriter(taxFile, true)
    for(line <- taxonomy)
      taxWriter.write(line._1+" : "+line._2+"\n")
    taxWriter.flush()
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

  def findMTDInArray(pivot: Int, occs: Array[Int]) = {
    occs.filter(x => {
      val absolute = Math.abs(pivot - x)
      if(absolute <= this.mtd && absolute != 0)
        true
      else
        false
    })
  }

  def checkMO(eps : Array[(String, Array[String])]) = {
    val epsCheck = eps.
      map(x => {
        val occs = x._2.distinct.sortBy(_.split(":")(0).toInt)
        val checkOccs = new ArrayBuffer[String]()
        checkOccs += occs(0)
        if(occs.length > 1) {
          var i = 0
          var j = 1
          while(j < occs.length) {
            val preE = StringUtils.split(occs(i), ":")(1).toInt
            val sufB = StringUtils.split(occs(j), ":")(0).toInt
            if(preE < sufB) {
              checkOccs += occs(j)
              i = j
            }
            j += 1
          }
        }
        (x._1, checkOccs.toArray)
      }).
      filter(_._2.length >= minSupport)
    epsCheck
  }
  
  def saveAsObjectFile[Partition](rdd: RDD[Partition], path: String) = {
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)
    rdd.map(splitArray => {
          val kryo = kryoSerializer.newKryo()
          val bao = new ByteArrayOutputStream()
          val output = kryoSerializer.newKryoOutput()
          output.setOutputStream(bao)
          kryo.writeClassAndObject(output, splitArray)
          output.close
          
          val byteWritable = new BytesWritable(bao.toByteArray())
          (NullWritable.get(), byteWritable)
      }).saveAsTextFile(path)
  }

}
