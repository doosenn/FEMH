package ict.mldm.main

/**
  * Created by Zorro on 2016/4/6.
  */

import ict.mldm.alg.{DKE, EMMO}
import ict.mldm.util.Transaction
import java.io.{BufferedReader, InputStreamReader}
import java.net.URI

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.Breaks

object FEMH {
  def main(args : Array[String]) :Unit = {
    val paras = new HashMap[String, String]()
    for(i <- args.indices) {
      args(i) match {
        case "-i" => paras("sequenceFile") = args(i+1)
        case "-h" => paras("hierarchyFile") = args(i+1)
        case "-o" => paras("outputPath") = args(i+1)
        case "-s" => paras("minSupport") = args(i+1)
        case "-w" => paras("mtd") = args(i+1)
        case "-p" => paras("paraNum") = args(i+1)
        case "-a" => paras("alg") = args(i+1)
        case "-j" => paras("jobName") = args(i+1)
        case "-b" => paras("beginTime") = args(i+1)
        case "-e" => paras("endTime") = args(i+1)
        case _ => println("Non-sense parameter: "+_)
      }
    }

    if(paras.get("sequenceFile") == None ||
    paras.get("hierarchyFile") == None ||
    paras.get("outputPath") == None ||
    paras.get("minSupport") == None ||
    paras.get("alg") == None ||
    paras.get("jobName") == None) {
      println("Wrong parameters! Exit!")
      System.exit(0)
    }
    if(paras.get("minSupport") == None) {
      paras("minSupport") = "1"
    }
    if(paras.get("mtd") == None) {
      paras("mtd") = "2"
    }
    if(paras.get("paraNum") == None) {
      paras("paraNum") = "6"
    }
    if(paras.get("alg") == "n") {
      if(paras.get("beginTime") == None ||
      paras.get("endTime") == None) {
        println("Either start or end time exists!")
        System.exit(0)
      }
    }

    val runner = new FEMH()
    runner.run(paras)
  }
}

class FEMH {

  def run(paras : HashMap[String, String]) = {
    println("Parallel FEMH Algorithm Running...")
    val conf = new SparkConf().setAppName(paras.get("jobName").get)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.registerKryoClasses(Array(classOf[Transaction], classOf[Array[String]],
      classOf[Array[Tuple3[_,_,_]]], classOf[Array[Int]], //classOf[scala.reflect.ClassTag$$anon$1],
      classOf[java.lang.Class[_]], classOf[Array[Tuple2[_,_]]], classOf[Array[Transaction]],
      classOf[ArrayBuffer[_]], classOf[Array[Object]]))

    val sc = new SparkContext(conf)
    val lines = sc.textFile(paras.get("sequenceFile").get)

    val sequence_rdd = lines.flatMap{case line => {
      val splits = StringUtils.split(line, '\t')
      val items = StringUtils.split(splits(1), ",")
      items.map{case item => (splits(0).toLong, item)}
    }}  //time item
    sequence_rdd.cache()

    val hsource = readHierarchyFromHdfs(paras.get("hierarchyFile").get)
    val sourceItems = hsource.flatMap(StringUtils.split(_, "->"))

    // Get dictionary and hierarchy
    val seqItems = sequence_rdd.map(_._2).distinct().collect()
    val dictionary = (sourceItems union seqItems).distinct.zipWithIndex.toMap
    val reverseDic = dictionary.map{case (item, id) => (id, item)}
    val b_revDic = sc.broadcast(reverseDic)
    val b_dictionary = sc.broadcast(dictionary)
    val hierarchy = hsource.map{case line => {
      val splits = StringUtils.split(line, "->")
      (splits(0), splits(1))
    }}.map(t => (dictionary(t._1), dictionary(t._2))).toMap

    val flist = sequence_rdd.
      map(x => (b_dictionary.value(x._2), x._1)).
      flatMap(y => {
        val re = new ArrayBuffer[Int]()
        var item = y._1
        while(hierarchy.contains(item)) {
          re+= hierarchy(item)
          item = hierarchy(item)
        }
        (re+=y._1).map(z => (z, y._2))
      }).
      groupByKey().
      filter(_._2.size >= paras.get("minSupport").get.toLong).
      map(x => (x._1, x._2.toArray))
    flist.cache()
    sequence_rdd.unpersist()
    val flist_keymap = flist.map(x => (x._1, x._2.length)).collect().sortBy(_._2).toMap
    val b_flist_keymap = sc.broadcast(flist_keymap)
    val c_flist = flist.
      flatMap(x => for(time <- x._2) yield (time, x._1)).
      groupByKey().
      map(x => {
        val map = b_flist_keymap.value
        (x._1, x._2.toArray.sortWith(map(_) < map(_)))
      }).collect()
    val b_flist = sc.broadcast(c_flist)

    println("Partitioning begin...")
    val transactions = flist.flatMap(x => {
      paras.get("alg") match {
        //case "n" => {}
        case _ => {
          val ts = ArrayBuffer[Transaction]()
          val pivot = x._1
          val occs = x._2.sortWith(_ < _)
          val zones = occs.length match {
            case 1 => ArrayBuffer((occs(0), occs(0)))
            case _ => splitArrayWithMTD(occs, paras.get("mtd").get.toInt)
          }

          val _flist = b_flist.value
          val map = b_flist_keymap.value
          val mtd = paras.get("mtd").get.toInt
          var idx = 0
          val pivotNums = map(pivot)
          for(z <- zones) {
            val seq = new ArrayBuffer[(Long, ArrayBuffer[Int])]()
            while(idx < _flist.length && z._1-mtd > _flist(idx)._1) idx += 1
            val start = idx
            while(idx < _flist.length && z._2+mtd >= _flist(idx)._1) idx += 1
            val end = idx - 1
            for(i <- start to end) {
              val time = _flist(i)._1
              val items = new ArrayBuffer[Int]()
              val loop = new Breaks
              loop.breakable {
                for(item <- _flist(i)._2) {
                  if(map(item) > pivotNums) loop.break()
                  else items += item
                }
              }
              seq += ((time, items))
            }
            val _seq = seq.toArray
            val t = new Transaction(pivot, new ArrayBuffer ++= _seq)
            ts += t
            idx = Math.max(0, end - mtd)
          }
          ts
        }
      }
    })

    val episodes = transactions.
      flatMap(t => {
        val mtd = paras.get("mtd").get.toInt
        val keys = b_flist_keymap.value.keys.toArray
        paras.get("alg").get match {
          case "e" => {
            val localMiner = new EMMO(t.getSeq, t.getPivot, mtd, keys)
            localMiner.mine()
          }
          case "d" => {
            val localMiner = new DKE(t.getSeq, t.getPivot, mtd)
            localMiner.mine()
          }
        }
      }).
      reduceByKey(_ + _).
      filter(_._2 >= paras.get("minSupport").get.toInt)

    episodes.repartition(1).saveAsTextFile(paras.get("outputPath").get)

    println("Job Finished.")

  }

  def readHierarchyFromHdfs(path: String) : Array[String] = {
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

  def splitArrayWithMTD(occs : Array[Long], mtd : Int) = {
    var tmp = new ArrayBuffer[(Long, Long)]()
    var p = 0
    var start = 0
    while(p < occs.length - 1) {
      if(occs(p+1) - occs(p) >= mtd) {
        tmp += ((occs(start), occs(p+1)))
        start = p + 1
      }
      p += 1
      if(p == occs.length - 1) {
        tmp += ((occs(start), occs(p+1)))
      }
    }
    tmp
  }

}
