package las;

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Text }
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{ OutputWriter, OutputWriterFactory, PartitionedFile }
import org.apache.spark.sql.execution.datasources.text.TextOutputWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import com.github.mreutegg.laszip4j.{ LASHeader, LASReader, LASPoint, LASInputStreamReader }
import scala.collection.JavaConverters._

case class LASException(private val message: String = "",
  private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

class LASRelation(val sqlContext: SQLContext, val path: String) extends BaseRelation with PrunedFilteredScan {
  
  // Identifies the columns and datatypes of the RDD
  
  override def schema: StructType = StructType(Seq(
    StructField("path", StringType, false),
    StructField("x", DoubleType, false),
    StructField("y", DoubleType, false),
    StructField("z", DoubleType, false),
    StructField("gps_time", DoubleType, false),
    StructField("intensity", IntegerType, false),
    StructField("return_n", ByteType, false),
    StructField("n_returns", ByteType, false),
    StructField("scan_angle_rank", ByteType, false),
    StructField("scan_direction_flag", ByteType, false)))
     
  // Performs the action of reading the files into rows.
    
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    
    // This is left as an example of how to process Filters - it currently doesn't do anything...
    
    /*val (application, from, to) = filters
      .foldLeft((Option.empty[Double], Option.empty[Double], Option.empty[Double])) {
        case (a, EqualTo("x", value: Double)) => a.copy(_1 = Some(value))
        case (a, GreaterThan("t", value: Double)) => a.copy(_2 = Some(value))
        case (a, GreaterThanOrEqual("t", value: Double)) => a.copy(_2 = Some(value))
        case (a, LessThan("t", value: Double)) => a.copy(_3 = Some(value))
        case (a, LessThanOrEqual("t", value: Double)) => a.copy(_3 = Some(value))
        case (a, _) => a
      }*/

    //For each file found on the path, map it out...
    sqlContext.sparkContext.binaryFiles(path, 0).flatMap { line =>

      // Get the path name
      val filepath = line._1 // could also use stream.getPath()
      
      // Get the PortableDataInputStream object
      var stream = line._2
      
      // Construct the homebrew reader for LAS (crudely adapted from laszip4j)
      val reader = new LASInputStreamReader(filepath, stream)
      val header = reader.getHeader()

      // Read relevant header variables
      
      val x_off = header.getXOffset()
      val y_off = header.getYOffset()
      val z_off = header.getZOffset()

      val x_sc = header.getXScaleFactor()
      val y_sc = header.getYScaleFactor()
      val z_sc = header.getZScaleFactor()

      // Retrieve the Point iterator
      
      val points = reader.getPoints()
      
      points.asScala.map { p =>
        
        // Sample fields that can be extracted.  Not all are shown or used.
        val id = p.getPointSourceID()
        val t = p.getGPSTime()
        val x = p.getX()
        val y = p.getY()
        val z = p.getZ()
        val i:Int = p.getIntensity()
        val rn = p.getReturnNumber()
        val nr = p.getNumberOfReturns()
        val sar = p.getScanAngleRank()
        val sdf = p.getScanDirectionFlag()
        
        //Re-scale the point data and return as a Row object
        
        Row(filepath, x*x_sc+x_off, y*y_sc+y_off, z*z_sc+z_off, t, i, rn, nr, sar, sdf)
      }
    }
  }
}

