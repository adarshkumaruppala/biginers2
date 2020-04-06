import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object rddUsingReduceByKey {
  def main(args:Array[String]):Unit={
    val sparkConf=new SparkConf()
                     .setAppName(" working on RDD")
                     .setMaster("local")
    val sc=new SparkContext(sparkConf)
    val lines=sc.textFile("src/test/resources/datasets/fakefriends.txt")
    val no=lines.flatMap(line=>line.split(",")).map(word=>(word,1)).reduceByKey((x,y)=>x+y)
    .sortByKey()
    
   print(no.collect().foreach(println))
    
  }
/*  def parseLine(line:String)={
    val field=line.split(",")
    print(field)
    val age=field(1).toUpperCase()
    val noOfFrnds=field(3)
    (age,noOfFrnds)
    
       
  }*/
}