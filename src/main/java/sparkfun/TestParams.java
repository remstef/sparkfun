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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


/**
 * @author Steffen Remus
 *
 */
public class TestParams extends SparkConfigured{
	
	public void run(SparkConf conf, String[] args) {
		
		conf.setAppName("TestParams");
	
		JavaSparkContext ctx = new JavaSparkContext(conf);
		
		System.out.println(ctx.appName());
		System.out.println(ctx.getConf().getOption("testopt"));
		
		for(Tuple2<?,?> t : ctx.getConf().getAll())
			System.out.println(t);
		System.out.println(ctx.getConf().getOption("spark.master"));
		
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			    
		ctx.close();

	}
	
	public static void main(String[] args) {
		new TestParams().run(args);
	}

}
