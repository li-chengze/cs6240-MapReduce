import org.apache.log4j._
import org.apache.spark._

import scala.collection.mutable.ArrayBuffer

object Main {
  val XDIM : String = "512"
  val YDIM : String = "512"

  // change the size of neighbor matrix
  val wholeNeighborX : Int = 21;
  val wholeNeighborY : Int = 21;
  val wholeNeighborZ : Int = 7;

  val neighborX : Int = wholeNeighborX / 2;
  val neighborY : Int = wholeNeighborY / 2;
  val neighborZ : Int = wholeNeighborZ / 2;

  val STEP = 15

  /**
    * this tuple represent the coordination of a pixel, (z, x, y)
    */
  type axis = (Int, Int, Int);

  /**
    * this tuple represent the constructure of a node, (axis, neightbors)
    */
  type node = (axis, List[Byte]);
  type label = (axis, Byte)

  def rotate90(pixels: Array[Array[Array[Byte]]]) : Array[Array[Array[Byte]]] = {
    for {
      z <- pixels.indices
      x <- pixels(z).indices
      y <- pixels(z)(x).indices
    } {
      val temp = pixels(z)(x)(y)
      pixels(z)(x)(y) = pixels(z)(pixels(z)(x).length - 1 - y)(x)
      pixels(z)(pixels(z)(x).length - 1 - y)(x) = temp
    }
    pixels
  }

  def mirror(layered: Array[Array[Array[Byte]]], zDim: Int) : Array[Array[Array[Byte]]] = {

    for {
      iz <- 0 until zDim
      ix <- 0 until XDIM.toInt / 2
      iy <- 0 until YDIM.toInt
    } {
      val temp = layered(iz)(ix)(iy)
      layered(iz)(ix)(iy) = layered(iz)(XDIM.toInt - 1 - ix)(iy)
      layered(iz)(XDIM.toInt - 1 - ix)(iy) = temp
    }
    layered
  }

  def getDistLabel(layered: Array[Array[Array[Byte]]], zDim: Int) : TraversableOnce[label] = {
    var result = new ArrayBuffer[label]()
    for {
      iz <- 0 until zDim by STEP / 5
      ix <- 0 until XDIM.toInt by STEP
      iy <- 0 until YDIM.toInt by STEP
    } {
      result += (((iz, ix, iy), layered(iz)(ix)(iy)));
    }

    return result;
  }

  def extractNeighbors(layered: Array[Array[Array[Byte]]], zDim : Int) : TraversableOnce[node] = {
    var result = new ArrayBuffer[node]();

    for {
      iz <- 0 until zDim by STEP / 5
      ix <- 0 until XDIM.toInt by STEP
      iy <- 0 until YDIM.toInt by STEP
    } {
      var neighbor : ArrayBuffer[Byte] = new ArrayBuffer[Byte]();

      val minZ = math.max(0, iz - neighborZ);
      val maxZ = math.min(zDim, iz + neighborZ + 1);

      val minX = math.max(0, ix - neighborX);
      val maxX = math.min(XDIM.toInt, ix + neighborX + 1);

      val minY = math.max(0, iy - neighborY);
      val maxY = math.min(YDIM.toInt, iy + neighborY + 1);

      for {
        z <- minZ until maxZ
        x <- minX until maxX
        y <- minY until maxY
      } {
        neighbor += layered(z)(x)(y)
      }
      val curAxis = (iz, ix, iy)

      result += ((curAxis, neighbor.toList))
    }
    result
  }

  def layerMatrix(pixels: Array[Byte], zDim: Int) : Array[Array[Array[Byte]]] = {
    val layered = Array.ofDim[Byte](zDim, XDIM.toInt, YDIM.toInt);
    var i = 0

    for {
      iz <- 0 until zDim
      ix <- 0 until XDIM.toInt
      iy <- 0 until YDIM.toInt
    } {
      layered(iz)(ix)(iy) = pixels(i)  // convert a 1-dimention array to 3-dimentions array
      i += 1;
    };

    layered
  }

  def processImage(image: String, sc: SparkContext, output: String, itemNo: Int) = {

    val imageFiles = Array("input/" + image + "_image.tiff", XDIM, YDIM);
    val distFiles = Array("input/" + image + "_dist.tiff", XDIM, YDIM);

    val distArray= LoadMultiStack.process(distFiles);
    val brightArray = LoadMultiStack.process(imageFiles);

    val zDim = distArray(0).length / (XDIM.toInt * YDIM.toInt)

    /**
      * original
      */

    val layeredDist = sc.parallelize(distArray).map(x => layerMatrix(x, zDim)).persist
    val layeredBrightness = sc.parallelize(brightArray).map(x => layerMatrix(x, zDim)).persist

    val distRdd =
      layeredDist
        .flatMap(x => getDistLabel(x, zDim))  // item structure: (axis, Byte)
        .filter(x => x._2 != 2.toByte && x._2 != 3.toByte)
        .map(x => {
          if (x._2 == 0 || x._2 == 1) {
            (x._1, 1)
          } else {
            (x._1, 0)
          }
        });

    val brightnessRdd =
      layeredBrightness
        .flatMap(x => extractNeighbors(x, zDim)) // item structure: (axis, List[Int])

    /**
      * mirrored original
      */

    val mirrorDistRdd =
      layeredDist
        .map(x => mirror(x, zDim))
        .flatMap(x => getDistLabel(x, zDim))  // item structure: (axis, Byte)
        .filter(x => x._2 != 2.toByte && x._2 != 3.toByte)
        .map(x => {
          if (x._2 == 0 || x._2 == 1) {
            (x._1, 1)
          } else {
            (x._1, 0)
          }
        });

    val mirrorBrightnessRdd =
      layeredBrightness
        .map(x => mirror(x, zDim))
        .flatMap(x => extractNeighbors(x, zDim))

    /**
      * rotated 90
      */

    val rotate90Dist = layeredDist.map(rotate90).persist
    val rotate90Brightness = layeredBrightness.map(rotate90).persist
    layeredDist.unpersist();
    layeredBrightness.unpersist();

    val rotate90DistRdd =
      rotate90Dist
        .flatMap(x => getDistLabel(x, zDim))
        .filter(x => x._2 != 2.toByte && x._2 != 3.toByte)
        .map(x => {
          if (x._2 == 0 || x._2 == 1) {
            (x._1, 1)
          } else {
            (x._1, 0)
          }
        });

    val rotate90BrightnessRdd =
      rotate90Brightness
        .flatMap(x => extractNeighbors(x, zDim)) // item structure: (axis, List[Int])


    /**
      * mirror rotated 90
      */

    val mirrorRotate90Dist = rotate90Dist.map(x => mirror(x, zDim))
    val mirrorRorate90Brightness = rotate90Brightness.map(x => mirror(x, zDim))

    val mirrorRotate90DistRdd =
      mirrorRotate90Dist
        .flatMap(x => getDistLabel(x, zDim))
        .filter(x => x._2 != 2.toByte && x._2 != 3.toByte)
        .map(x => {
          if (x._2 == 0 || x._2 == 1) {
            (x._1, 1)
          } else {
            (x._1, 0)
          }
        });

    val mirrorRotate90BrightnessRdd =
      mirrorRorate90Brightness
        .flatMap(x => extractNeighbors(x, zDim)) // item structure: (axis, List[Int])

    /**
      * rotate 180
      */

    val rotate180Dist = rotate90Dist.map(rotate90).persist()
    val rotate180Brightness = rotate90Brightness.map(rotate90).persist()

    rotate90Dist.unpersist()
    rotate90Brightness.unpersist()

    val rotate180DistRdd =
      rotate180Dist
        .flatMap(x => getDistLabel(x, zDim))
        .filter(x => x._2 != 2.toByte && x._2 != 3.toByte)
        .map(x => {
          if (x._2 == 0 || x._2 == 1) {
            (x._1, 1)
          } else {
            (x._1, 0)
          }
        });

    val rotate180BrightnessRdd =
      rotate180Brightness
        .flatMap(x => extractNeighbors(x, zDim))

    /**
      * mirror rotate 180
      */

    val mirrorRotate180Dist = rotate180Dist.map(x => mirror(x, zDim))
    val mirrorRotate180Brightness = rotate180Brightness.map(x => mirror(x, zDim))

    val mirrorRotate180DistRdd =
      mirrorRotate180Dist
        .flatMap(x => getDistLabel(x, zDim))
        .filter(x => x._2 != 2.toByte && x._2 != 3.toByte)
        .map(x => {
          if (x._2 == 0 || x._2 == 1) {
            (x._1, 1)
          } else {
            (x._1, 0)
          }
        });

    val mirrorRotate180BrightnessRdd =
      mirrorRotate180Brightness
        .flatMap(x => extractNeighbors(x, zDim))


    /**
      * rotate 270
      */

    val rotate270Dist = rotate180Dist.map(rotate90).persist()
    val rotate270Brightness = rotate180Brightness.map(rotate90).persist()

    rotate180Dist.unpersist()
    rotate180Brightness.unpersist()

    val rotate270DistRdd =
      rotate270Dist
        .flatMap(x => getDistLabel(x, zDim))
        .filter(x => x._2 != 2.toByte && x._2 != 3.toByte)
        .map(x => {
          if (x._2 == 0 || x._2 == 1) {
            (x._1, 1)
          } else {
            (x._1, 0)
          }
        });

    val rotate270BrightnessRdd =
      rotate270Brightness
        .flatMap(x => extractNeighbors(x, zDim))

    /**
      * mirror rotate 270
      */

    val mirrorRotate270Dist = rotate270Dist.map(x => mirror(x, zDim))
    val mirrorRotate270Brightness = rotate270Brightness.map(x => mirror(x, zDim))

    val mirrorRotate270DistRdd =
      mirrorRotate270Dist
        .flatMap(x => getDistLabel(x, zDim))
        .filter(x => x._2 != 2.toByte && x._2 != 3.toByte)
        .map(x => {
          if (x._2 == 0 || x._2 == 1) {
            (x._1, 1)
          } else {
            (x._1, 0)
          }
        });

    val mirrorRotate270BrightnessRdd =
      mirrorRotate270Brightness
        .flatMap(x => extractNeighbors(x, zDim))

    /**
      * join all the rdds
      */

    val originalJoinedRdd = distRdd.join(brightnessRdd)
    val originalMirroredJoinedRdd = mirrorDistRdd.join(mirrorBrightnessRdd)
    val rotate90JoinedRdd = rotate90DistRdd.join(rotate90BrightnessRdd)
    val mirrorRotate90JoinedRdd = mirrorRotate90DistRdd.join(mirrorRotate90BrightnessRdd)
    val rotate180JoinedRdd = rotate180DistRdd.join(rotate180BrightnessRdd)
    val mirroredRotate180JoinedRdd = mirrorRotate180DistRdd.join(mirrorRotate180BrightnessRdd)
    val rotate270JoinedRdd = rotate270DistRdd.join(rotate270BrightnessRdd)
    val mirrorRotate270JoinedRdd = mirrorRotate270DistRdd.join(mirrorRotate270BrightnessRdd)

    val finalRdd =
      originalJoinedRdd
        .union(originalMirroredJoinedRdd)
        .union(rotate90JoinedRdd)
        .union(mirrorRotate90JoinedRdd)
        .union(rotate180JoinedRdd)
        .union(mirroredRotate180JoinedRdd)
        .union(rotate270JoinedRdd)
        .union(mirrorRotate270JoinedRdd)

    finalRdd.sample(false, math.min(itemNo / finalRdd.count, 1)).saveAsTextFile(output + "/image" + image)
  }
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Data Mining").set("spark.network.timeout", "600s");
    val sc = new SparkContext(conf);
    val output = args(0);
    val itemNo = args(1).toInt;
    for(i <- 1 to 5) {
      processImage("" + i, sc, output, itemNo);
    }

  }

}
