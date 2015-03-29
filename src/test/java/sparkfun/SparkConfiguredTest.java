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
package sparkfun;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.junit.Test;

import scala.Tuple2;

/**
 * @author Steffen Remus
 *
 */
public class SparkConfiguredTest {

	@Test
	public void test() {
		SparkConfigured job = new SparkConfigured() {
			@Override
			public void run(SparkConf conf, String[] args) {
				System.out.println(Arrays.toString(args));
				for(Tuple2<String, String> t : conf.getAll())
					System.out.println(t);
			}
		};
		job.run("-key1=val1 --key2=val2 -key3 val3 -key4 val4.1 val4.2 -key5=val5.1 val5.2 -key6".split(" "));
		job.run("-key1=val1 --key2=val2 -key3 val3 -key4 val4.1 val4.2".split(" "));
		job.run("---key1=val1".split(" "));

	}

}
