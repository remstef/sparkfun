/*
 *   Copyright 2015
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package sparkfun.wc;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


/**
 * @author Steffen Remus
 *
 */
public class AppJ8 {
	
	public static void main(String[] args) {
		
		if (args.length < 2) {
			System.err.println("Usage: MainClass <in-file> <out-file>");
			System.exit(1);
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCountJ8").setMaster("local[2]").set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec");
		//sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf); // "local[1]","test",
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
		 
	    JavaPairRDD<String, Integer> counts = lines.flatMap(line -> Arrays.asList(line.split(" ")))
			.mapToPair(w -> new Tuple2<String,Integer>(w, 1))
			.reduceByKey((x, y) -> x + y);

//	    counts.foreach(t -> System.out.println(t._1() + ": " + t._2()));
	    
	    counts.map( t -> new Tuple2<>(t._1, t._2)).saveAsTextFile(args[1]);
	    
		ctx.close();

	}

}
