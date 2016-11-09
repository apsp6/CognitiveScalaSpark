package org.cgs.mines.MineSweeper

//maven takes care of pulling the necessary spark libraries.
//add to /etc/hosts 127.0.0.1 Karthiks-MacBook-Pro.local

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.io.Source

object Field {
  
  def main(args: Array[String]) = {
  
    //setting up SparkContext 
    val conf = new SparkConf()
      .setAppName("MineField")
      .setMaster("local")
    val sc = new SparkContext(conf)
    
   
    //Loading the Field Layout. Increase the number of partitions(the second parameter to textFile)
    //if you have large files.
    //One can also call repartition on the loaded mineField if you see load on a single worker.
    //val mineField_repart = mineField.repartition(4)
    val mineField = sc.textFile("TestFields/Layout2.txt",1)
  
    
    //Read the Script File
     val firingScript = ("TestScripts/Script2.txt")
     val lines = Source.fromFile(firingScript).getLines.toList
     val instr = lines map { x => x.split(' ') }
    
    // for (i <- instr) (if(i.length>1) {println(i(0),i(1),i.length)} else {println(i(0),i.length)})
    
     //Forcing the loaded mineFiled to memory.
    mineField.cache()
    
  
    val originalMineField_X_Y = mineField.zipWithIndex map { case (k,v) => (k.zipWithIndex map { case(i,j)=>(i,j,v)}) }
    
    //Printing  
    originalMineField_X_Y.map{case (k) => (k map { case(l,i,j) => (l)})}.foreach{e=>println(e.mkString(""))}
     
     // Getting the field size by looking the co-ordinates of last position.
     //The Field needs to be resized because of moving sweeper hence a var.
     
       var fieldSize = originalMineField_X_Y.flatMap(x=>x).reduce((i,j)=>('A',math.max(i._2,j._2),math.max(i._3,j._3)))
       println(fieldSize.toString())
     //fieldSize.foreach(e=>println(e._2,e._3))
     
     
     //get the bounds of printable minefeild. 
     //  1. filter for only mines
     //  2. get the first and last x and y cordinates where mines are found 
     //  3. check this against the size of actual minefeild before printing
     var MineBoundaries = originalMineField_X_Y.flatMap {x=>x }.filter { x=>(x._1 != '.')}.
               map{ case (i,j,k) => (j,j,k,k) }.
               reduce((x,y) => (math.min(x._1, y._1), math.max(x._2, y._2),math.min(x._3, y._3), math.max(x._4, y._4)))
     println(MineBoundaries.toString())
     
     var printBoundaries = (0,0,0,0)
     
     
     var updatedMineField_X_Y = originalMineField_X_Y
    
    var loopCounter = 0;
    //intializing Sweeper Depth
    var sweeperDepth = 'a'
    
    def adjustPrintBoundaries () = {
        var low_x=0
        var low_y =0
        var high_x=0
        var high_y =0
        
        if ((MineBoundaries._1-0)>=2) {
          low_x=MineBoundaries._1
        } else {
          low_x = 0
        }
          
         if ((MineBoundaries._3-0)>=2) {
          low_y=MineBoundaries._3.toInt
        } else {
          low_y = 0
        }
  
        if ((fieldSize._2- MineBoundaries._2)>=2) {
          high_x=MineBoundaries._2
        } else {
          high_x = fieldSize._2
        }
          
        if ((fieldSize._3 - MineBoundaries._4)>=2) {
          high_y=MineBoundaries._4.toInt
        } else {
          high_y = fieldSize._3.toInt
        }
        
        printBoundaries = (low_x,high_x,low_y,high_y)
      }
      
      def checkIfPrintable (x: Int, y: Long) : Boolean = {
        MineBoundaries  
        fieldSize
        //fix the boundaries
        if (x>=printBoundaries._1 && x<=printBoundaries._2 
              && y>=printBoundaries._3 && y<=printBoundaries._4) {
          return true
        } else {
         return false
        }
      }
    
adjustPrintBoundaries()
println(printBoundaries)


      // get the Script instructions and execute
      for (i <- instr){
        
        println("Step : " + loopCounter + "_0")
        
      //Print MineField  
      updatedMineField_X_Y.map {case (k) => (k map { case(l,i,j) => if(checkIfPrintable(i,j))(l)else '*'})}.
                                    foreach(e=>println(e.mkString("").replace("*", "")))

        
        var newMineField_X_Y = updatedMineField_X_Y map {case (k) => (k map { case(l,i,j) => (newDepth(l),i,j)})} 
        
        println("Step : " + loopCounter + "_1")
               
      newMineField_X_Y.map {case (k) => (k map { case(l,i,j) => if(checkIfPrintable(i,j))(l)else ('*')})}.
                       foreach(e=>println(e.mkString("").replace("*", "")))

        
        updatedMineField_X_Y = newMineField_X_Y
        
        sweeperDepth = newDepthSweeper(sweeperDepth)
        loopCounter = loopCounter+1
       
      }
      
      def newDepth (a:Char) : Char = {
         if (a =='A') { 
				    return 'z'
			  } else if (a =='.') {
			    return a
			  } else {
				  (a-1).toChar
			 }
      }
      
      def newDepthSweeper (a:Char) : Char = {
         if (a =='z') { 
				    return 'A'
			  } else {
				  (a+1).toChar
			 }
      }
      

	
   
    //mineField.saveAsTextFile("test.txt")
  }
}