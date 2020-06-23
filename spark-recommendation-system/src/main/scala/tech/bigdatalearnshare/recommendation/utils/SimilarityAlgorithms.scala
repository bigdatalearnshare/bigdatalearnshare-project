package tech.bigdatalearnshare.recommendation.utils

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.jblas.DoubleMatrix
import org.slf4j.LoggerFactory

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-19
  */
object SimilarityAlgorithms {

  private val LOGGER = LoggerFactory.getLogger(SimilarityAlgorithms.getClass)

  /**
    * 皮尔逊相关
    *
    * @param arr1 double array
    * @param arr2 double array
    * @return
    */
  def pearsonCorrelationSimilarity(arr1: Array[Double], arr2: Array[Double]): Double = {
    //数组长度必须相等
    require(arr1.length == arr2.length, s"SimilarityAlgorithms:Array length do not match: Len(x)=${arr1.length} and Len(y)" +
      s"=${arr2.length}.")

    val sum_vec1 = arr1.sum
    val sum_vec2 = arr2.sum

    val square_sum_vec1 = arr1.map(x => x * x).sum
    val square_sum_vec2 = arr2.map(x => x * x).sum

    val zipVec = arr1.zip(arr2)

    val product = zipVec.map(x => x._1 * x._2).sum
    val numerator = product - (sum_vec1 * sum_vec2 / arr1.length)

    val dominator = math.pow((square_sum_vec1 - math.pow(sum_vec1, 2) / arr1.length) * (square_sum_vec2 - math.pow(sum_vec2, 2) / arr2.length), 0.5)

    if (dominator == 0) Double.NaN else numerator / (dominator * 1.0)
  }

  /* 调整后的余弦相似度，传入的参数(向量或数组)都是两个用户对不同物品的的评分数据 计算的是item和item之间的*/

  /**
    * 调整后的余弦相似度
    *
    * @param x DoubleMatrix
    * @param y DoubleMatrix
    * @return
    */
  def adjustedCosineSimJblas(x: DoubleMatrix, y: DoubleMatrix): Double = {
    require(x.length == y.length, s"SimilarityAlgorithms:DoubleMatrix length do not match: Len(x)=${x.length} and Len(y)" +
      s"=${y.length}.")

    val avg = (x.sum() + y.sum()) / (x.length + y.length)
    val v1 = x.sub(avg)
    val v2 = y.sub(avg)
    v1.dot(v2) / (v1.norm2() * v2.norm2())
  }

  /**
    * 调整后的余弦相似度
    *
    * @param x Array[Double]
    * @param y Array[Double]
    * @return
    */
  def adjustedCosineSimJblas(x: Array[Double], y: Array[Double]): Double = {
    require(x.length == y.length, s"SimilarityAlgorithms:Array length do not match: Len(x)=${x.length} and Len(y)" +
      s"=${y.length}.")

    val v1 = new DoubleMatrix(x)
    val v2 = new DoubleMatrix(y)

    adjustedCosineSimJblas(v1, v2)

    //x和y元素和的平均值
    /*val avg = (x.sum + y.sum) / (x.length + y.length)
    val newX = x.map(_ - avg)
    val newY = y.map(_ - avg)
    val v1 = new DoubleMatrix(newX)
    val v2 = new DoubleMatrix(newY)
    v1.dot(v2) / (v1.norm2() * v2.norm2())*/
  }

  /**
    * jblas实现余弦相似度
    *
    * @param v1 DoubleMatrix
    * @param v2 DoubleMatrix
    * @return
    */
  def cosineSimJblas(v1: DoubleMatrix, v2: DoubleMatrix): Double = {
    require(v1.length == v2.length, s"SimilarityAlgorithms:DoubleMatrix length do not match: Len(v1)=${v1.length} and Len(v2)" +
      s"=${v2.length}.")
    v1.dot(v2) / (v1.norm2() * v2.norm2())
  }

  /**
    * jblas实现余弦相似度
    *
    * @param x Array[Double]
    * @param y Array[Double]
    * @return
    */
  def cosineSimJblas(x: Array[Double], y: Array[Double]): Double = {
    require(x.length == y.length, s"SimilarityAlgorithms:Array length do not match: Len(v1)=${x.length} and Len(v2)" +
      s"=${y.length}.")

    val v1 = new DoubleMatrix(x)
    val v2 = new DoubleMatrix(y)
    //dot：求两个向量的点积如v1(a1,b1) v2(a2,b2) a1*a2+b1*b2 norm2是求向量的L2范数:向量元素平方和开根
    v1.dot(v2) / (v1.norm2() * v2.norm2())
  }

  /**
    * 调整后的余弦相似度
    *
    * @param v1 向量
    * @param v2 向量
    * @return
    */
  def adjustedCosineSimilarity(v1: Vector, v2: Vector): Double = {
    //向量长度必须相等
    require(v1.size == v2.size, s"SimilarityAlgorithms:Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)" +
      s"=${v2.size}.")
    val x = v1.toArray
    val y = v2.toArray

    adjustedCosineSimilarity(x, y)
  }

  /**
    * 调整后的余弦相似度
    * "原始余弦相似度"从方向上区分差异，对绝对的数值不敏感，因此没法衡量每个维度上数值的差异，会在衡量用户评分时出现偏差
    *
    * @param x Array[Double]
    * @param y Array[Double]
    * @return
    */
  def adjustedCosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
    //数组长度必须相等
    require(x.length == y.length, s"SimilarityAlgorithms:Array length do not match: Len(x)=${x.length} and Len(y)" +
      s"=${y.length}.")

    //x和y元素和的平均值
    val avg = (x.sum + y.sum) / (x.length + y.length)

    //对公式分子部分进行计算
    val member = x.map(_ - avg).zip(y.map(_ - avg)).map(d => d._1 * d._2).sum

    //求解分母变量
    val temp1 = math.sqrt(x.map(num => math.pow(num - avg, 2)).sum)
    val temp2 = math.sqrt(y.map(num => math.pow(num - avg, 2)).sum)

    //求出分母
    val denominator = temp1 * temp2
    //求出公式值
    if (denominator == 0) Double.NaN else member / (denominator * 1.0)
  }

  /**
    * 余弦相似度
    *
    * @param v1 向量
    * @param v2 向量
    * @return
    */
  def cosineSimilarity(v1: Vector, v2: Vector): Double = {
    //向量长度必须相等
    require(v1.size == v2.size, s"SimilarityAlgorithms:Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)" +
      s"=${v2.size}.")

    //"特征"向量
    val x = v1.toArray
    val y = v2.toArray

    cosineSimilarity(x, y)
  }

  /**
    * 余弦相似度
    *
    * @param x double array
    * @param y double array
    * @return
    */
  def cosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
    //数组长度必须相等
    require(x.length == y.length, s"SimilarityAlgorithms:Array length do not match: Len(x)=${x.length} and Len(y)" +
      s"=${y.length}.")

    //对公式分子部分进行计算
    val member = x.zip(y).map(d => d._1 * d._2).sum
    //求解分母变量
    /*val temp1 = math.sqrt(x.map(num => {math.pow(num, 2)}).reduce(_ + _))*/
    val temp1 = math.sqrt(x.map(math.pow(_, 2)).sum)
    val temp2 = math.sqrt(y.map(math.pow(_, 2)).sum)
    //求出分母
    val denominator = temp1 * temp2
    //求出公式值
    if (denominator == 0) Double.NaN else member / (denominator * 1.0)
  }

  /**
    * 欧几里得距离
    * 调用Spark KMeans 最终计算欧氏距离的方法
    *
    * @param v1 Vector
    * @param v2 Vector
    * @return
    */
  def euclidean(v1: Vector, v2: Vector): Double = {
    //两个向量之间的平方距离         欧式
    val sqdist = Vectors.sqdist(v1, v2)
    math.sqrt(sqdist)
  }

  /**
    * 欧几里得距离
    *
    * @param v1 向量
    * @param v2 向量
    * @return
    */
  def euclidean2(v1: Vector, v2: Vector): Double = {
    //向量长度必须相等
    require(v1.size == v2.size, s"SimilarityAlgorithms:Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)" +
      s"=${v2.size}.")

    val x = v1.toArray
    val y = v2.toArray

    euclidean(x, y)
  }

  /**
    * 欧几里得距离
    *
    * @param x double array
    * @param y double array
    * @return
    */
  def euclidean(x: Array[Double], y: Array[Double]): Double = {
    //数组长度必须相等
    require(x.length == y.length, s"SimilarityAlgorithms:Array length do not match: Len(x)=${x.length} and Len(y)" +
      s"=${y.length}.")

    math.sqrt(x.zip(y).map(p => p._1 - p._2).map(d => d * d).sum)
  }
}
