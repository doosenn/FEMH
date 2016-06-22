package ict.mldm.main

import ict.mldm.debug.MyLog
import ict.mldm.util.{Partition, Transaction}
import java.io.{BufferedReader, ByteArrayOutputStream, File, FileWriter, InputStreamReader}
import java.net.URI

import ict.mldm.alg.{DMO, MESELO}
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
  private var maxIter : Int = 0
  private var mode : String = "local"

  private val logger = Logger.getLogger(this.getClass)
  private val mylog = new MyLog("./data/debug.info")

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("log4j.properties")
    parameters(args)
    run()
  }

  def run() = {
    val conf = new SparkConf().setAppName("FEMH").setMaster(mode)
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Partition]))
    conf.set("spark.kryoserializer.buffer.max", "1g")
    val sc = new SparkContext(conf)
    val startTime = System.currentTimeMillis()
    val lines = sc.textFile(sequenceFile)

    //Sequence of items with format (item, timestamp)
    val sequence_rdd = lines.map(line=>{
      val splits = StringUtils.split(line, '\t')
      (splits(0), splits(1).toInt)
    })

//    val d_sequence_rdd = sequence_rdd.collect()
//    var sequenceMsg : String = "\n"
//    for(d <- d_sequence_rdd)
//      sequenceMsg += d._1 + ":" + d._2 +"\n"
//    mylog.info(sequenceMsg)

    val source = readHierarchyFromLocal(hierarchyFile)   //read from local
    //val source = readHierarchyFromHdfs(hierarchyFile)  //read from hdfs
    val sourceItems = source.flatMap(StringUtils.split(_, "->"))

    //Event dictionary, a HashMap[String, Int]
    val diff_items = sequence_rdd.map(_._1).distinct().collect()
    val dictionary = getDictionary(diff_items, sourceItems)
    val reverseDic = dictionary.map(x=>(x._2, x._1))
    val b_dictionary = sc.broadcast(dictionary)
    writeDictionary(dictionary)

    //Hierarchy, a HashMap[Int, Int]
    val hierarchy = genHierarchy(source, dictionary)
    val b_hierarchy = sc.broadcast(hierarchy)
    writeTaxonomy(hierarchy)

    //flist
    val flist = sequence_rdd.
      map(x => (b_dictionary.value(x._1), x._2)).
      flatMap(x => {
        val re = getAncestorself(x._1, b_hierarchy.value).map(y => (y, x._2))
        re
      }).
      groupByKey().
      filter(_._2.size >= minSupport).
      map(x=>(x._1, x._2.toArray.sortWith(_ < _))).
      sortBy(x=>x._2.length, false)
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
    val b_flist = sc.broadcast(flist.collect())
    val flist_keys = flist.map(_._1).collect()
    val b_flist_keys = sc.broadcast(flist_keys)

    //partitions
    val partitions = flist.map(x => {
      val pivot = x._1
      val occs = x._2
      val p = new Partition(pivot)
      for(occ <- occs){
        var seq = ArrayBuffer[(Int, Int)]((pivot, occ))
        val keys = b_flist_keys.value
        for(f <- b_flist.value;if keys.indexOf(pivot) >= keys.indexOf(f._1)){
          seq ++= findMTDInArray(occ, f._2).map(x => (f._1, x))   //(event, time)
        }
        val t = new Transaction(pivot, occ)
        seq = seq.sortBy(_._2)
        t.setSeq(seq)
        p.addTran(t)
      }
      p
    })
//    val d_partitions = partitions.collect()
//    for(p <- d_partitions) {
//      var parMsg = "\nPartition " + p.getPivot+":\n"
//      for(t <- p.getTrans) {
//        parMsg += t.getTime+"::\t"
//        for(ses <- t.getSeq){
//          parMsg += ses._1+":"+ses._2+"\t"
//        }
//        parMsg += "\n"
//      }
//      mylog.info(parMsg)
//    }

    val episodes = partitions.flatMap(x => {
      val eps = new ArrayBuffer[(String, Array[String])]()
      val ts = x.getTrans
      for(t <- ts) {
//        val meseloMine = new MESELO()
//        eps ++= meseloMine.mine(t)

         val dmoMine = new DMO(mtd, minSupport, 4)
         eps ++= dmoMine.mine(t)
      }
      //checkMO(eps)
      val ret = checkMO(eps)
      ret
    })

    val d_episodes = episodes.collect()
    var epsMsg = "\nFEMH results "+d_episodes.length+" :\n"
    for(e <- d_episodes){
      epsMsg += e._1+"\t"
      for(occ <- e._2) {
        epsMsg += occ +"\t"
      }
      epsMsg += "\n"
    }
    mylog.info(epsMsg)

    val endTime = System.currentTimeMillis()
    val timeDiff = endTime - startTime
    println("Parallel FEMH finished in "+ timeDiff +".")
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
      else if (args(i).equalsIgnoreCase("-d")) {
        this.maxIter = args(i + 1).toInt
      }
      else if (args(i).equalsIgnoreCase("-m")) {
        this.mode = args(i + 1)
      }
    }
  }

  def readHierarchyFromLocal(path: String) = {
    val source = Source.fromFile(path).getLines().toArray
    source
  }

  def readHierarchyFromHdfs(path: String) = {
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
    for(a <- array){
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
    while(hierarchy.contains(item)){
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

  def checkMO(eps : ArrayBuffer[(String, Array[String])]) = {
    val epsCheck = eps.
      flatMap(x => {
        val temp = new ArrayBuffer[(String, String)]()
        for(occ <- x._2){
          temp += ((x._1, occ))
        }
        temp
      }).
      groupBy(_._1).
      map(x => {
        val occs = (for(t <- x._2) yield t._2).distinct
        val checkOccs = new ArrayBuffer[String]()
        checkOccs += occs(0)
        if(occs.length > 1){
          var i = 0
          var j = 1
          while(j < occs.length){
            val preE = StringUtils.split(occs(i), ":")(1).toInt
            val sufB = StringUtils.split(occs(j), ":")(0).toInt
            if(preE < sufB){
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
    rdd.map(splitArray=>{
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
