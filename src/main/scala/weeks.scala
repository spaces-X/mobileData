import java.text.SimpleDateFormat

import java.time.LocalDate


import scala.collection.mutable.ListBuffer
import java.util.Date
import java.util.Calendar


import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import test.{calcDis, cellData, movePoint, retrieve_neighbors, parse, tDbscanAndJudgeAttri,judgePointAttri}


import scala.collection.mutable
import scala.io.Source
import scala.util.control.Breaks

object weeks {

  var calendar = Calendar.getInstance()
  case class stableStopPoint(plng:Double, plat:Double,var attr:String="null",var daycount:String="NO" ,times:Iterable[(Date,Date)]) {
    def this(plng:Double, plat:Double,times:Iterable[(Date,Date)])
      = this(plng,plat,"null","NO",times)
    override def toString: String = {
      var line = new StringBuilder()
      attr = this.attributeJudge(times)
      moreThan2Days(times)
      line.append(plng + ","+ plat + ","+ attr + "," + daycount + ",")
      var len = times.size
      line.append(len)
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      if(len>0) {
        for (time<-times){
          line.append(",")
          line.append(format.format(time._1) + "," + format.format(time._2))
        }
      }
      line.toString()
    }
    def attributeJudge(times: Iterable[(Date,Date)]): String = {
      var unknown = 0
      var home = 0
      var work = 0
      var result = "null"
      for (d<-times) {
        var result = judgePointAttri(d._1,d._2)
        if (result.equals("work")) {
//          work+=1
          work += (d._2.getTime() - d._1.getTime()) / 1000
        }
        else if (result.equals("home")) {
//          home+=1
          home += (d._2.getTime() - d._1.getTime()) / 1000
        }
        else if (result.equals("unknown")) {
//          unknown+=1
          unknown += (d._2.getTime() - d._1.getTime()) / 1000
        }
      }
      if ((home==0 && work==0) || (work+home)*10 < unknown) {
        result="unknown"
      }
      else if (home>= work){
        result = "home"
      }
      else if (work> home) {
        result = "work"
      }
      result
    }
    def moreThan2Days(times:Iterable[(Date,Date)]): Boolean = {
      var daySet = Set[Int]()
      for (s <- times) {
        daySet+= s._1.getDate()
      }
      daycount = if (daySet.size >= 3) "YES" else "NO"
      return daySet.size>=3
    }
  }

  case class temporaryStopPoint(plng:Double, plat:Double, dstart:Date, dend:Date) {
    override def toString: String = {
      var format = new SimpleDateFormat("yyyyMMddHHmmss")
      plng+","+plat+","+format.format(dstart)+","+format.format(dend)
    }
  }

  /**
    * 用于第二次聚类，得到当前StopPoint的邻居们StopPoint，不考虑时间，只考虑不同StopPoint之间的距离
    * @param index_center
    * @param df
    * @param spatial_threshold
    * @return
    */
  def retrieve_neighbors_sp(index_center:Int, df:Array[(Int,Date,Date,Double,Double,Array[Int])],spatial_threshold:Double) ={
    val res=new scala.collection.mutable.ArrayBuffer[(Int,Date,Date,Double,Double,Array[Int])]
    val empty=new scala.collection.mutable.ArrayBuffer[(Int,Date,Date,Double,Double,Array[Int])]
    var i = 0
    while(i<df.length){
      if (i==index_center) {
        i+=1
      }
      else {
        if (calcDis(df(i)._4, df(i)._5, df(index_center)._4, df(index_center)._5)<=spatial_threshold) {
          res+=df(i)
        }
        i+=1
      }
    }

    if (res.length<1)
      empty
    else
      res.sortBy( x => x._2)
  }


  /**
    * 根据第一次聚类得到的停留点进行二次聚类,仅仅考虑参数距离和邻居个数
    * @param line
    * @param spatial_threshold
    * @param min_neighbors
    * @return
    */
  def DbscanSecond(line:(String,Iterable[stopPoint]),spatial_threshold:Double,min_neighbors:Int) = {
    var index= -1
    var clusterIndex=0
    var stack=new mutable.Stack[Int]()
    /*
      -1表示为未标记
      0表示离群点
      1....n表示簇集的id
     */
    val df=line._2.map { x =>
      val kind = Array(-1)
      index+=1
      (index,x.dStart,x.dEnd,x.lng,x.lat,kind)
    }.toArray

    for(data<-df)
    {
      if(data._6(0) == -1) {
        var neighbor = retrieve_neighbors_sp(data._1, df, spatial_threshold)
        if(neighbor.length<min_neighbors)
          data._6(0)=0
        //        else if(neighbor(neighbor.length-1)._2.getTime-neighbor
        //        (0)._2.getTime<temporal_threshold)
        //          data._5(0)=0
        else{
          //          neighbor.remove(data._1)
          clusterIndex+=1
          data._6(0)=clusterIndex

          for(dataNeighbor<-neighbor)
          {
            dataNeighbor._6(0)=clusterIndex
            stack.push(dataNeighbor._1)
          }
          while (stack.isEmpty==false)
          {
            val cur=stack.pop()
            val newNeighbor=retrieve_neighbors_sp(cur, df, spatial_threshold)
            if(newNeighbor.length>=min_neighbors)
            {
              for(s<-newNeighbor)
              {
                if(s._6(0)== -1||s._6(0)==0)
                {
                  s._6(0)=clusterIndex
                  stack.push(s._1)
                }
              }
            }
          }
        }
      }
    }
    /*
      输出格式：
      ID:
      长期稳定停留点：（中心lng,中心lat,[(停留开始时间，停留结束时间),...]）
      临时停留点： (lng,lat,（停留开始时间，停留结束时间）)
     */
    val stop=df.groupBy(x=>x._6(0)).filter(x=>x._1!=0).map{x=>
      var clng=0.0
      var clat=0.0
      // 按 dstart 时间排序
      val l=x._2.sortBy(t=>t._2)
      var times = new ListBuffer[(Date,Date)]()
      for(y<-l)
      {
        clng+=y._4
        clat+=y._5
        var datePair = (y._2,y._3)
        times += datePair
      }
      // 构造稳定停留点
      new stableStopPoint(clng/l.length, clat/l.length, times)
    }

    val move=df.filter(x=>x._6(0)==0).map{
      x=>
        temporaryStopPoint(x._4, x._5, x._2,x._3)
    }
    //用户id,稳定停留点集合，非稳定停留点集合
    (line._1,stop,move)
  }


  def sortByDateTime(line:(String,Iterable[String])):(String,Iterable[String])={
    var ele=line._2.toArray
    val format=new SimpleDateFormat("yyyyMMddHHmmss")
    var date = new Date()
    (line._1,ele.sortBy(x=>
     new Date((x.split(",")(1).replaceAll("CST","")))))
  }


  /**
    * 从HDFS中读取第一次聚类的结果
    * @param line
    * @return
    */
  def parseClusterRes(line: String) ={
    var items = line.split(",")
    val id = items(0)
    val lng = items(1).toDouble
    val lat = items(2).toDouble
    val dateStart = new Date(items(3).replace("CST",""))
    val dateEnd = new Date(items(4).replace("CST",""))
    val attr = items(5)
    var newKey = id
    ( newKey, new stopPoint(lng, lat, dateStart, dateEnd, attr))
  }

  /**
    * 从ActiveData 中读取数据,并转换成(id,cellData)
    * @param line
    * @return
    */
  def parseActiveData(line: String) :(String,cellData) = {
    var items = line.split(",")
    val id = items(0)
    val date = new Date(items(1).replace("CST",""))
    val lng = items(2).toDouble
    val lat = items(3).toDouble
    (id, cellData(id,date,lng,lat))
  }

  /**
    * 将第一次聚类的结果进行过滤，至少n天都有停留点
    * @param data
    * @param n
    * @return
    */
  def continueStopPoint(data: (String,Iterable[stopPoint]), n: Int): Boolean = {
    var stopPoints = data._2
//    var cl = Calendar.getInstance()
    var DaySet = mutable.Set[Int]()
    for( s <- stopPoints){
      calendar.setTime(s.dStart)
      var day = calendar.get(Calendar.DAY_OF_MONTH)
      DaySet.add(day)
    }
    DaySet.size>=n
  }

  /**
    * 将预处理后并且活跃的数据作为输入, 提取大于n天活跃的用户
    * @param data
    * @param n
    * @return
    */
  def continueCell(data:(String, Iterable[cellData]), n:Int) :Boolean = {
    var cellDatas = data._2
    var DaySet = mutable.Set[Int]()

    DaySet.clear()
    for (data <- cellDatas) {
      calendar.setTime(data.date)
      var day = calendar.get(Calendar.DAY_OF_MONTH)
      DaySet.add(day)
    }
    DaySet.size >= n

  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("weeks").setMaster("spark://bigdata02:7077").set("spark.executor.memory", "128g").set("spark.executor.cores", "32")
    val sc = new SparkContext(conf)
    /**
      * 两周的数据第一次聚类
      */
    var data = sc.textFile("hdfs://bigdata01:9000/home/wx/twoweeks/clusterResults/stopAll/*/*")
    var results = data.map(x=>parseClusterRes(x)).groupByKey(5)
      .map(x => DbscanSecond(x,500,2))
    var allLSP = results.filter(x=>x._2.size>0).map(x=>(x._1,x._2)).flatMapValues(x=>x)
      .map(x=>x._1+","+x._2.toString)
    allLSP.saveAsTextFile("hdfs://bigdata01:9000/home/wx/twoweeks/clusterSecond/allLSP")
    var onlyLSP = results.filter( x=> (x._2.size>0 && x._3.size==0)).map(x=>(x._1,x._2)).flatMapValues(x=>x)
      .map(x=>x._1+","+x._2.toString)
    onlyLSP.saveAsTextFile("hdfs://bigdata01:9000/home/wx/twoweeks/clusterSecond/onlyLSP")
    var allTSP = results.filter(x=> x._3.size>0).map(x=>(x._1,x._3)).flatMapValues(x=>x)
      .map(x=>x._1+","+x._2.toString)
    allTSP.saveAsTextFile("hdfs://bigdata01:9000/home/wx/twoweeks/clusterSecond/allTSP")
    var onlyTSP = results.filter(x=> (x._2.size==0 && x._3.size>0)).map(x=>(x._1,x._3)).flatMapValues(x=>x)
      .map(x=>x._1+","+x._2.toString)
    onlyTSP.saveAsTextFile("hdfs://bigdata01:9000/home/wx/twoweeks/clusterSecond/onlyTSP")

    }


}
