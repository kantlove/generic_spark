module_name="generic_spark"

spark_submit="/home/$me/spark_2.1.0/bin/spark-submit"
spark_app_name="--name ${module_name}"
spark_specs="--total-executor-cores 1"
spark_target="--class ${module_name}.Main generic_spark/target/scala-2.11/${module_name}.jar"

${spark_submit} ${spark_app_name} ${spark_specs} ${spark_config} ${spark_target}
