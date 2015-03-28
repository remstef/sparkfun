package sparkfun.wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author stevo
 */
object AppSc {

	def main(args: Array[String]) {

		if (args.size < 2) {
			System.err.println("Usage: MainClass <in-file> <out-file>");
			System.exit(1);
		}

		val conf = new SparkConf().setAppName("Spark Pi");
		val sc = new SparkContext(conf);

		val lines = sc.textFile(args(0));

		val counts = lines.flatMap(line => line.split(" "))
				.map(word => (word, 1))
				.reduceByKey(_ + _);

		counts.saveAsTextFile(args(1));

		sc.stop();

	}

}