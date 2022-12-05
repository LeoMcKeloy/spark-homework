package lev.pyryanov

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

import scala.language.postfixOps

object App {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder().appName("Simple Application")
            .master("local[1]")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        import spark.implicits._

        var df_web = spark.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/sparkdb")
            .option("dbtable", "public.web")
            .option("user", "spark")
            .option("password", "spark")
            .load()

        println(df_web.show())

        val df_lk = spark.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/sparkdb")
            .option("dbtable", "public.lk")
            .option("user", "spark")
            .option("password", "spark")
            .load()

        println(df_lk.show())

        println(
            df_web.groupBy(col("id"))
                .count()
                .sort(col("count").desc)
                .show(1))

        val withSign = df_web.filter("sign = true").count()
        println(withSign * 1.0 / df_web.count() * 100)

        println(
            df_web.filter("type = 'click'")
                .groupBy(col("page_id"))
                .count()
                .show(5)
        )

        df_web = df_web.withColumn("event_time", from_unixtime(col("timestamp")))

        println(
            df_web.withColumn("time_range", floor(hour(col("event_time")) / lit(4)))
                .groupBy("time_range")
                .count()
                .orderBy(col("count").desc)
                .show(1)
        )

        df_web = df_web.withColumn("time_range", floor(hour(col("event_time")) / lit(4)))

        var df_all = df_lk.join(df_web, df_lk("user_id") === df_web("id"), "full")

        println(df_all.show())

        val get_surname = udf((s: String) => s.split(" ")(0))

        println(
            df_all.select(get_surname(col("fio")) as "surname")
                .filter("tag = 'Sport'")
                .distinct()
                .show()
        )

        val calc_gender = (fio: String) => {
            if (fio == null) {
                "unknown"
            } else {
                val str = fio.split(" ")
                if ((str(0).substring(str(0).length - 2).equals("ов") ||
                    str(0).substring(str(0).length - 2).equals("ов")) &&
                    str(2).substring(str(2).length - 2).equals("ич")) "m" else "w"}
        }

        val gender_udf = udf(calc_gender)

        df_all = df_all.withColumn("gender", gender_udf(col("fio")))

        println(
            df_all.filter("gender = 'm'")
                .groupBy(col("page_id"))
                .count()
                .orderBy(col("count").desc)
                .show(5)
        )

        println(
            df_all.filter("gender = 'w'")
                .groupBy(col("page_id"))
                .count()
                .orderBy(col("count").desc)
                .show(5)
        )

        df_all = df_all.withColumn("age", floor(months_between(current_date,'birthday)/12))

        df_all = df_all.withColumn(
            "favorite_tag",
            max("tag")
                .over(Window.partitionBy("user_id")))

        df_all = df_all.withColumn(
            "favorite_time_range",
            max("time_range")
                .over(Window.partitionBy("user_id")))

        df_all = df_all.withColumn(
            "days_diff", datediff(col("event_time"),col("create_date")))

        df_all = df_all.withColumn(
            "visit_number",
            count("id")
                .over(Window.partitionBy("id")))

        println(df_all.show())

        val data_mart = df_all.select(
            col("user_id"), col("age"), col("gender"), col("favorite_tag"),
            col("favorite_time_range"), col("lk_id"), col("days_diff"),
            col("visit_number"))

        println(data_mart.show())

        data_mart.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/sparkdb")
            .option("dbtable", "public.data_mart")
            .option("user", "spark")
            .option("password", "spark")
            .save()
    }
}
