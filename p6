val format = kafkaConfig.getOrElse("format", "string")

if (format == "avro") {
  // Call your Avro utility directly here
  AvroProcessingUtility.processKafkaData(spark, /* other parameters */)
} else {
  // Existing logic for other formats
  val kafkaDF = readKafkaData(spark, kafkaSparkProperties, topics.mkString("."), startingOffsets).persist()
  
  val kafkaBatchDF = kafkaDF
    .withColumn("topic", col("topic").cast("string"))
    .withColumn("key", col("key").cast("string"))
    .withColumn("value", col("value").cast("binary")).persist(StorageLevel.MEMORY_AND_DISK)
    
  // ... rest of your standard processing
}
