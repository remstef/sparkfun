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
package sparkfun.breezetest

import breeze.linalg.{sum, DenseVector}

/**
 * @author Steffen Remus
 *
 */
object test {
  
  def main(args: Array[String]): Unit = {
    val s = DenseVector(0,1,2,3);
    val t = s + DenseVector(1,1,1,1);
    val z = t.foldLeft("")((x,y) => x + "\t" + y.toString());

    println(sum(t(0,2)))
    println(sum(t))
    println("a b c".split(" ", 5).toList)
  }

}