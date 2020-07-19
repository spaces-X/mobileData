import java.text.SimpleDateFormat

import scala.collection.mutable.ListBuffer
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source
import scala.util.control.Breaks

class stopPoint(plng:Double,plat:Double,pdStart:Date,pdEnd:Date,pattri:String) extends Serializable {
  var lng=plng
  var lat=plat
  var dStart=pdStart
  var dEnd=pdEnd
  var attri=pattri

  override def toString: String = {

    lng+","+lat+","+dStart+","+dEnd+","+attri
  }
}


object test {
  def judgeUserV1(line:(String,Iterable[String])):Boolean={

    var userId=line._1
    val messge=line._2
    val format=new SimpleDateFormat("yyyyMMddHHmmss")
    var count0to7=0
    var count7to10=0
    var count10to13=0
    var count13to16=0
    var count16to19=0
    var count19to24=0

    for(s<-messge)
    {
      try{
        val eleHour=format.parse(s.split(",")(2)).getHours
        if(eleHour>=0&&eleHour<7)
          count0to7+=1
        else if(eleHour>=7&&eleHour<10)
          count7to10+=1
        else if(eleHour>=10&&eleHour<13)
          count10to13+=1
        else if(eleHour>=13&&eleHour<16)
          count13to16+=1
        else if(eleHour>=16&&eleHour<19)
          count16to19+=1
        else
          count19to24+=1
      }
      catch
        {
          case ex:Exception=>{
            println(s)
          }

        }
    }
    if(count0to7>=1&&count7to10>=1&&count10to13>=1&&count13to16
      >=1&&count16to19>=1&&count19to24>=1)
      true
    else
      false
  }

  def judgeUserV2(line:(String,Iterable[String])):Boolean={

    var userId=line._1
    val messge=line._2
    val format=new SimpleDateFormat("yyyyMMddHHmmss")

    var count0to7=0
    var count7to10=0
    var count11to14=0
    var count15to18=0
    var count19to24=0

    for(s<-messge)
    {
      try{
        val eleHour=format.parse(s.split(",")(2)).getHours
        if(eleHour>=0&&eleHour<7)
          count0to7+=1
        else if(eleHour>=7&&eleHour<=10)
          count7to10+=1
        else if(eleHour>=11&&eleHour<=14)
          count11to14+=1
        else if(eleHour>=15&&eleHour<=18)
          count15to18+=1
        else
          count19to24+=1
      }
      catch
        {
          case ex:Exception=>{
            println(s)
          }

        }
    }
    if(count0to7>=1&&count7to10>=1&&count11to14>=1&&count15to18
      >=1&&count19to24>=1)
      true
    else
      false
  }

  def judgeUserUptoThirty(line:(String,Iterable[String])):Boolean= {
    line._2.size > 30
  }

  def conVert(line:(String,Iterable[String])):(String,Iterable[String])={

    val res = new scala.collection.mutable.ArrayBuffer[String]()

    for(s<-line._2)
    {
      res+=s.split(",")(2)

    }
    (line._1,res)
  }

  /*  def sort(line:(String,Iterable[String])):(String,Iterable[String])={
      val userId=line._1
      val messge=line._2
      val format=new SimpleDateFormat("yyyyMMddHHmmss")
      val date=format.parse()

    }*/

  def judgeData(line:String):Boolean={
    val format=new SimpleDateFormat("yyyyMMddHHmmss")
    var judgeLength=0
    var judgeTime=0
    var judgeLoc=0
    var judgeScope=0
    try{
      val eleTime=format.parse(line.split(",")(2))
      val lng=line.split(",")(4).toDouble
      val lat=line.split(",")(5).toDouble
      if(lat<39.4333||lat>41.05||lng<115.41667||lng>117.5)
      {
        judgeScope=1
      }

    }
    catch {
      case exLength:java.lang.ArrayIndexOutOfBoundsException=>{
        judgeLength=1
      }
      case ex:java.text.ParseException=>{
        judgeTime=1
      }
      case exLoc:java.lang.NumberFormatException=>{
        judgeLoc=1
      }
    }
    if(judgeTime==1||judgeLoc==1||judgeLength==1||judgeScope==1)
      false
    else
      true
  }

  def sortByTime(line:(String,Iterable[String])):(String,Iterable[String])={
    var ele=line._2.toArray
    val format=new SimpleDateFormat("yyyyMMddHHmmss")
    (line._1,ele.sortBy(x=>(format.parse(x.split(",")(2)))))
  }

  def deleteShakeV3(line:(String,Iterable[String])):(String,Iterable[(Date,Double,Double)])={
    val format=new SimpleDateFormat("yyyyMMddHHmmss")
    val ele=line._2.map{x=>
      val ele=x.split(",")
      (format.parse(ele(2)),ele(4).toDouble,ele(5).toDouble)
    }.toBuffer

    if(ele.size<3)
      (line._1,ele)
    else
    {

      var dBefore=0.0
      var dAfter=0.0
      var tBefore:Long=0
      var tAfter:Long=0
      val recordIndex=new ListBuffer[(Date,Double,Double)]

      recordIndex+=ele(0)
      for(i<- 1 to ele.length-2 )
      {
        dBefore=calcDis(ele(i-1)._2,ele(i-1)._3,ele(i)._2,ele(i)._3)
        dAfter=calcDis(ele(i)._2,ele(i)._3,ele(i+1)._2,ele(i+1)._3)
        tBefore=(ele(i)._1.getTime-ele(i-1)._1.getTime)/1000
        tAfter=(ele(i+1)._1.getTime-ele(i)._1.getTime)/1000

        if(!(dBefore>3000&&dAfter>3000&&tBefore<40&&tAfter<40))
          recordIndex+=ele(i)
      }
      recordIndex+=ele(ele.length-1)
      (line._1,recordIndex)
    }
  }


  def calcDis(lng1:Double,lat1:Double,lng2:Double,lat2:Double):Double={
    val r=6371393 //地球半径
    val con=Math.PI/180.0 //用于计算的常量
    val radLat1: Double = lat1*con
    val radLat2: Double = lat2*con
    val a: Double = radLat1 - radLat2
    val b: Double = lng1*con - lng2*con
    var s: Double = 2.0 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2.0), 2.0) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2.0), 2.0)))
    s = s * r
    s = Math.round(s * 10000.0) / 10000.0
    s
  }

  /***
    * 不考虑时间进行neighbors检测
    */

  def retrieve_neighbors(index_center:Int, df:Array[(Int,Date,Date,Double,Double,Array[Int])],spatial_threshold:Double) ={
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


  def retrieve_neighborsT(index_center:Int, df:Array[(Int,Date,Date,Double,Double,Array[Int])],
                          spatial_threshold:Double, temporal_threshold:Long)={
    val res=new scala.collection.mutable.ArrayBuffer[(Int,Date,Date,Double,Double,Array[Int])]
    val empty=new scala.collection.mutable.ArrayBuffer[(Int,Date,Date,Double,Double,Array[Int])]
    var forward=index_center
    var backward=index_center
    val loop1=new Breaks
    val loop2=new Breaks
    loop1.breakable{
      while(backward>0)
      {
        backward-=1
        if(calcDis(df(backward)._4,df(backward)._5,df(index_center)._4,df(index_center)._5)<=spatial_threshold)
        {
          res+=df(backward)
        }
        else
          loop1.break()
      }
    }
    loop2.breakable{
      while(forward<df.length-1)
      {
        forward+=1
        if(calcDis(df(forward)._4,df(forward)._5,df(index_center)._4,df(index_center)._5)<=spatial_threshold)
        {
          res+=df(forward)
        }
        else
          loop2.break()
      }
    }
    res+=df(index_center)
    res.sortBy(x=>x._2)
    if(res(res.length-1)._3.getTime-res(0)._2.getTime<temporal_threshold)
      empty
//    if(res.length<2) // 不关心邻居的个数 但是关心邻居的时长（上面）
//      empty
    else
      res.filter(x=>x._1!=index_center)
  }

  case class cellData(id:String,sdate: Date,edate: Date ,lng:Double,lat:Double) {
    override def toString: String = {
      id + "," + sdate.toString+ "," + edate.toString + "," + lng + "," + lat
    }
  }
  case class movePoint(lng:Double,lat:Double,smove:Date, emove:Date){
    override def toString: String = {
      lng+","+lat+","+ smove.toString() + "," + emove.toString()
    }
  }

  def tDbscanAndJudgeAttri(line:(String,Iterable[cellData]),spatial_threshold:Double,
                           temporal_threshold:Long,min_neighbors:Int) ={
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
      (index,x.sdate,x.edate,x.lng,x.lat,kind)
    }.toArray

    for(data<-df)
    {
      if(data._6(0) == -1) {
        var neighbor = retrieve_neighborsT(data._1, df, spatial_threshold, temporal_threshold)
//        if(neighbor.length<min_neighbors)
//          data._6(0)=0
        if(neighbor.length < 1) {
          if (data._3.getTime() - data._2.getTime() >= temporal_threshold){
            clusterIndex += 1
            data._6(0) = clusterIndex
          }
          else {
            data._6(0) = 0;
          }

        }
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
            val newNeighbor=retrieve_neighborsT(cur,df,
              spatial_threshold,temporal_threshold
            )
            // ！！！！！！！
            // 对于找到的新的neighbor 要不要加到簇里 还有待研究
            // 未来可能将 新加入的点 单独 算一个时间跨度 如果大于阈值的话就会加入  否则不加入
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
    /*
      输出格式：
      停留点：（中心lng,中心lat,(停留开始时间，停留结束时间)，STOP）
      移动点： (lng,lat,（移动发生时间，移动发生时间）,MOVE)
     */
    val stop=df.groupBy(x=>x._6(0)).filter(x=>x._1!=0).map{x=>
      var clng=0.0
      var clat=0.0
      val l=x._2.sortBy(t=>t._2)
      for(y<-l)
      {
        clng+=y._4
        clat+=y._5
      }
      new stopPoint(clng/l.length,clat/l.length,l(0)._2,l(l.length-1)._3,
        judgePointAttri(l(0)._2,l(l.length-1)._3))
    }

    val move=df.filter(x=>x._6(0)==0).map{
      x=>
        movePoint(x._4, x._5, x._2, x._3)
    }
    //用户id,停留点集合,移动点集合
    (line._1,stop,move)
  }
/**
  * 第二次聚类，时间条件放宽松，距离条件要求严格一些
  * 多天的结果进行聚类
  * 这个时间条件根本没用，留着他是为了和原函数保持参数一致
  *
  * */

  def tDbscanSecond(line:(String,Iterable[cellData]),spatial_threshold:Double,
                           temporal_threshold:Long,min_neighbors:Int) ={
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
      (index,x.sdate, x.edate,x.lng,x.lat,kind)
    }.toArray

    for(data<-df)
    {
      if(data._6(0) == -1) {
        var neighbor = retrieve_neighbors(data._1, df, spatial_threshold)
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
            val newNeighbor=retrieve_neighbors(cur,df, spatial_threshold)
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
      停留点：（中心lng,中心lat,(停留开始时间，停留结束时间)，STOP）
      移动点： (lng,lat,（移动发生时间，移动发生时间）,MOVE)
     */
    val stop=df.groupBy(x=>x._6(0)).filter(x=>x._1!=0).map{x=>
      var clng=0.0
      var clat=0.0
      val l=x._2.sortBy(t=>t._2)
      for(y<-l)
      {
        clng+=y._4
        clat+=y._5
      }
      // 是工作还是在家 通过拼接的key 进行得到
      new stopPoint(clng/l.length,clat/l.length,l(0)._2,l(l.length-1)._2,
        line._1.split("_")(1))
    }

    val move=df.filter(x=>x._6(0)==0).map{
      x=>
        movePoint(x._4, x._5, x._2, x._3)
    }
    //用户id,停留点集合,移动点集合
    (line._1,stop,move)
  }





  def convertDay(line:(String,Iterable[Int])): Tuple2[String,Int] =
  {
    var res=line._2.toSet
    (line._1,res.size)
  }

  def judgePointAttri(start:Date,end:Date)={
    val threeH= 3*60*60*1000
    val twoH= 2*60*60*1000
    val tmp=start
    var attri="unknown"
    /*
      定义居家时间段与工作时间段
     */
    val workTime=(new Date(tmp.getYear,tmp.getMonth,tmp.getDate,7,0,0),
      new Date(tmp.getYear,tmp.getMonth,tmp.getDate,19,0,0))
    val homeTime1=(new Date(tmp.getYear,tmp.getMonth,tmp.getDate,0,0,0),
      new Date(tmp.getYear,tmp.getMonth,tmp.getDate,7,0,0))
    val homeTime2=(new Date(tmp.getYear,tmp.getMonth,tmp.getDate,19,0,0),
      new Date(tmp.getYear,tmp.getMonth,tmp.getDate,24,0,0))
    /*
      计算工作时间段交集
     */
    var Intersection=0.0
    var ts=0L
    var te=0L
    if(start.before(workTime._2)&&end.after(workTime._1))
    {
      if(start.before(workTime._1))
        ts=workTime._1.getTime
      else
        ts=start.getTime
      if(end.before(workTime._2))
        te=end.getTime
      else
        te=workTime._2.getTime
      Intersection=(te-ts).toDouble
      val t1=(end.getTime-start.getTime).toDouble
      if(Intersection/t1>0.5&&Intersection>threeH.toLong)
      {
        attri="work"
      }
      else if((Intersection/t1)<=0.5&&(t1-
        Intersection)>twoH.toLong)
      {
        attri="home"
      }
    }
    else
    {
      Intersection=end.getTime-start.getTime
      if(Intersection>twoH.toLong)
        attri="home"
    }
    attri
  }
  def parse(line:String)={
    val pieces=line.split(",")
    val id=pieces(0)
    val sdate=new Date(pieces(1).replace("CST",""))
    val edate=new Date(pieces(2).replace("CST",""))
    val lng=pieces(3).toDouble
    val lat=pieces(4).toDouble
    cellData(id,sdate,edate,lng,lat)
  }

  def parseClusterRes(line: String) ={
    var items = line.split(",")
    val id = items(0)
    val lng = items(1).toDouble
    val lat = items(2).toDouble
    val dateStart = new Date(items(3).replace("CST",""))
    val dateEnd = new Date(items(4).replace("CST",""))
    val attr = items(5)
    var newKey = id+"_"+attr
    ( newKey, cellData(id, dateStart, dateEnd, lng, lat) )
//    ( newKey, new stopPoint(lng, lat, dateStart, dateEnd, attr))
  }

  def analyResults(line: Iterable[String]) ={
    var home = 0
    var work = 0
    var unknow = 0
    for ( attr<-line ) {
      if (attr.equals("home")) {
        home += 1
      }
      else if (attr.equals("work")) {
        work += 1
      }
      else if (attr.equals("unkown")) {
        unknow += 1
      }
    }
    (home,work,unknow)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("spark://bigdata02:7077").set("spark.executor.memory", "128g").set("spark.executor.cores", "32")
    val sc = new SparkContext(conf)

   for (i <- 10 to 14) {
     var rdd = sc.textFile("/home/xw/201411"+ i + "-raw/*")
     var rdd1 = rdd.filter(x=>judgeData(x)).filter(x=>(x.split(",")(2)).substring(6,8).equals(i.toString))
     var rdd2 = rdd1.map(x=>(x.split(",")(0),x)).groupByKey().map(x=>sortByTime(x)).
       filter(x=>judgeUserV2(x)).map(x=>deleteShakeV3(x)).repartition(5)

     rdd2.filter(x => x._2.size> 30).
       map{x=>
         val res=new ListBuffer[String]
         for(data<-x._2)
         {
           // id,time,lng,lat
           res+=x._1+","+data._1+","+data._2+","+data._3
         }
         res
       }.flatMap(x=>x).saveAsTextFile("hdfs://bigdata01:9000/home/wx/test/activeData/"+i)

     rdd2.map{x=>
         val res=new ListBuffer[String]
         for(data<-x._2)
         {
           // id,time,lng,lat
           res+=x._1+","+data._1+","+data._2+","+data._3
         }
         res
       }.flatMap(x=>x).saveAsTextFile("hdfs://bigdata01:9000/home/wx/test/preProc/"+i)
   }

  }



}
