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

/**
 * @author Steffen Remus
 *
 */
public abstract class SparkConfigured {
	
	public abstract void run(SparkConf conf, String[] args);

	/*
	 * -key1=val1 --key2=val2 -key3 val3 -key4 val4 val5 -key6=val7 val8 -key9 
	 * -key1=val1 --key2=val2 -key3 val3 -key4 val5 val6
	 */
	public void run(String[] args){
		SparkConf conf = new SparkConf();
		String key = "", value = "";
		for(int i = 0; i < args.length; i++){
			if(args[i].startsWith("-")){
				key = args[i].replaceFirst("--?", ""); // match -key, --key
				value = "";
			}
			else
				value += " " + args[i];
			if(key.contains("=")){
				value = key.substring(key.indexOf("=")+1);
				key = key.substring(0, key.indexOf("="));
				conf.set(key.trim(), value.trim());
				continue;
			}
			if(!key.isEmpty() && !value.isEmpty())
				conf.set(key.trim(), value.trim());
		}
		run(conf, args);
	}
	
}
