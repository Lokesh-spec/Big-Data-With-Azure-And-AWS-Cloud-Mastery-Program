# Apache Spark Basics Notes

## Word Count Example
- Demonstrates how to perform a basic word count operation using RDDs in Spark.

## Partition Demo
- **Set partition to 200**  
  - Adjusting the number of partitions in an RDD to optimize parallelism.

## RDD Operations
RDD (Resilient Distributed Dataset) is a fundamental data structure in Spark. Below are some basic operations:

### Map
- Applies a function to each element in the RDD.

#### Parse Data
- Splitting and processing raw data into a structured format.

## Wide vs Narrow Transformation
- **Narrow Transformations:** Operations like `map`, `filter`, where data movement happens within partitions.
- **Wide Transformations:** Operations like `groupByKey`, `reduceByKey` that cause data shuffling across partitions.

