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
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author Steffen Remus
 *
 */
public class AppJ7 {
	private static final Pattern SPACE = Pattern.compile(" ");
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: MainClass <in-file> <out-file>");
			System.exit(1);
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCountJ7");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf); // "local[2]","test",
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});
		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
//		List<Tuple2<String, Integer>> output = counts.collect();
//		for (Tuple2<?,?> tuple : output) {
//			System.out.println(tuple._1() + ": " + tuple._2());
//		}
		
		counts.saveAsTextFile(args[1]);
		
		ctx.close();
		
	}
}
