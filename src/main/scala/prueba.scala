package job.examen

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object Ejercicio1 {

  // Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
  def ejercicio1()(implicit spark: SparkSession): DataFrame = {

    // Importar implicits para usar toDF
    import spark.implicits._

    // Crear una secuencia de tuplas con datos de estudiantes
    val estudiantes = Seq(
      ("Juan", 20, 7),
      ("Ana", 22, 9),
      ("Pedro", 21, 8),
      ("Lucía", 19, 6),
      ("Carlos", 23, 10)
    )

    // Crear el DataFrame con columnas nombre, edad, calificación
    val df = estudiantes.toDF("nombre", "edad", "calificacion")

    // Mostrar el esquema del DataFrame
    df.printSchema()

    // Filtrar los estudiantes con calificación mayor a 8
    val estudiantesFiltrados = df.filter($"calificacion" > 8)
    estudiantesFiltrados.show()

    // Seleccionar los nombres de los estudiantes y ordenarlos por calificación descendente
    val estudiantesOrdenados = df.select("nombre", "calificacion")
      .orderBy($"calificacion".desc)
    estudiantesOrdenados.show()

    // Devolver el DataFrame ordenado
    estudiantesOrdenados
  }

}

object Ejercicio2 {

  // Ejercicio 2: UDF (User Defined Function) para determinar si un número es par o impar
  val esParOImpar: Int => String = (numero: Int) => {
    if (numero % 2 == 0) "Par" else "Impar"
  }

  // Registrar la UDF
  val esParOImparUDF = udf(esParOImpar)

  // Función para crear el DataFrame y aplicar la UDF
  def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // Importar implicits para usar toDF
    import spark.implicits._

    // Aplicar la UDF a la columna "numero" del DataFrame y crear una nueva columna "par_o_impar"
    val resultado = numeros.withColumn("par_o_impar", esParOImparUDF(col("numero")))

    // Mostrar el resultado
    resultado.show()

    // Devolver el DataFrame con la nueva columna
    resultado
  }

}

object Ejercicio3 {

  // Ejercicio 3: Joins y agregaciones
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame)(implicit spark: SparkSession): DataFrame = {

    // Realizar el join entre los DataFrames de estudiantes y calificaciones
    val estudiantesCalificaciones = estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
      .select(estudiantes("id"), estudiantes("nombre"), calificaciones("calificacion"))

    // Calcular el promedio de calificaciones por estudiante
    val promedioCalificaciones = estudiantesCalificaciones
      .groupBy("id", "nombre")
      .agg(avg("calificacion").alias("promedio_calificacion"))

    // Mostrar el resultado
    promedioCalificaciones.show()

    // Devolver el DataFrame con los promedios
    promedioCalificaciones
  }
}


object Ejercicio {

  /** Ejercicio 4: Uso de RDDs
   * Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
   */
 def Ejercicio(palabras:List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
   val rdd = spark.sparkContext.parallelize(palabras)
   val conteo = rdd.map(palabra => (palabra, 1)).reduceByKey(_ + _)

   // Mostrar el resultado
   conteo.collect().foreach { case (palabra, cantidad) =>
     println(s"$palabra: $cantidad")
   }

   conteo
 }
