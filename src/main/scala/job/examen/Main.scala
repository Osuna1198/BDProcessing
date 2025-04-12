package job.examen.practica

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import job.examen.Ejercicio
import job.examen.{Ejercicio1, Ejercicio2, Ejercicio3}
object Main {
  def main(args: Array[String]): Unit = {
    // Iniciar la sesión de Spark
    val spark = SparkSession.builder()
      .appName("Ejercicio Spark")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // EJERCICIO 1
    println("Ejercicio 1:")
    Ejercicio1.ejercicio1()(spark)

    // EJERCICIO 2
    println("Ejercicio 2:")
    val numeros = Seq(1, 2, 3, 4, 5).toDF("numero")
    Ejercicio2.ejercicio2(numeros)(spark)

    // EJERCICIO 3
    println("Ejercicio 3:")
    val estudiantes = Seq(
      (1, "Juan"),
      (2, "Ana"),
      (3, "Pedro"),
      (4, "Lucía"),
      (5, "Carlos")
    ).toDF("id", "nombre")

    val calificaciones = Seq(
      (1, "Matemáticas", 7),
      (2, "Matemáticas", 9),
      (3, "Matemáticas", 8),
      (4, "Matemáticas", 6),
      (5, "Matemáticas", 10)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    Ejercicio3.ejercicio3(estudiantes, calificaciones)(spark)

    // EJERCICIO 4
    println("Ejercicio 4:")
    val palabras = List("hola", "mundo", "hola", "spark", "mundo", "hola")
    val conteo = Ejercicio.Ejercicio(palabras)(spark)
    conteo.collect().foreach(println)

    // Detener Spark
    spark.stop()
  }
}