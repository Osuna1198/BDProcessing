import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, sum}

object Ejercicio5 {

  def ejercicio5(): Unit = {
    val spark = SparkSession.builder()
      .appName("VentasApp")
      .master("local[*]")
      .getOrCreate()

    val filePath = "C://Users//Alejandro Osuna//Desktop//BOOTCAMP//BIG DATA PROCESSING//ventas.csv"

    val ventasDF: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    println("Contenido del DataFrame original:")
    ventasDF.show()

    val ingresosDF: DataFrame = ventasDF
      .withColumn("ingreso_total", col("cantidad") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(sum("ingreso_total").alias("ingreso_total"))

    println("Ingreso total por producto:")
    ingresosDF.show()
  }
}
