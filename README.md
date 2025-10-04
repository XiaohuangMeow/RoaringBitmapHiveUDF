RoaringBitmap Hive UDF
--
- [Introduction](#introduction)
- [Features](#features)
- [Versions](#versions)
- [Installation \& Deployment](#installation--deployment)
  - [1. Obtain the UDF JAR](#1-obtain-the-udf-jar)
    - [Option 1: Download the latest JAR file  from the project's Releases page.](#option-1-download-the-latest-jar-file--from-the-projects-releases-page)
    - [Option 2: Build from Source​​](#option-2-build-from-source)
  - [2. Register UDFs/UDAFs in Hive](#2-register-udfsudafs-in-hive)
- [Usage Examples](#usage-examples)
  - [1. Sample Data](#1-sample-data)
  - [2. Build RoaringBitmap](#2-build-roaringbitmap)
  - [3. Calculate Cardinality](#3-calculate-cardinality)
  - [4. Set Operations](#4-set-operations)
- [Important Notes](#important-notes)
  - [Explicit Type Conversion When Using rb\_build](#explicit-type-conversion-when-using-rb_build)
  - [BIGINT Mandatory for Integer Parameters](#bigint-mandatory-for-integer-parameters)
  - [Memory Comsumtion](#memory-comsumtion)
  - [Aovid Excessively Large Size](#aovid-excessively-large-size)
- [API Documentation](#api-documentation)
  - [UDAF](#udaf)
    - [rb\_and\_agg](#rb_and_agg)
    - [rb\_and\_cardinality\_agg](#rb_and_cardinality_agg)
    - [rb\_build\_agg](#rb_build_agg)
    - [rb\_cardinality\_agg](#rb_cardinality_agg)
    - [rb\_or\_agg](#rb_or_agg)
    - [rb\_or\_cardinality\_agg](#rb_or_cardinality_agg)
    - [rb\_xor\_agg](#rb_xor_agg)
    - [rb\_xor\_cardinality\_agg](#rb_xor_cardinality_agg)
  - [UDF](#udf)
    - [rb\_and](#rb_and)
    - [rb\_and\_cardinality](#rb_and_cardinality)
    - [rb\_andnot](#rb_andnot)
    - [rb\_andnot\_cardinality](#rb_andnot_cardinality)
    - [rb\_build](#rb_build)
    - [rb\_cardinality](#rb_cardinality)
    - [rb\_clear](#rb_clear)
    - [rb\_contains](#rb_contains)
    - [rb\_equal](#rb_equal)
    - [rb\_fill](#rb_fill)
    - [rb\_intersect](#rb_intersect)
    - [rb\_is\_empty](#rb_is_empty)
    - [rb\_maximum](#rb_maximum)
    - [rb\_minimum](#rb_minimum)
    - [rb\_not\_equal](#rb_not_equal)
    - [rb\_or](#rb_or)
    - [rb\_or\_cardinality](#rb_or_cardinality)
    - [rb\_range](#rb_range)
    - [rb\_range\_cardinality](#rb_range_cardinality)
    - [rb\_to\_array](#rb_to_array)
    - [rb\_xor](#rb_xor)
    - [rb\_xor\_cardinality](#rb_xor_cardinality)
  - [Contributing](#contributing)

# Introduction
This project delivers a high-performance suite of Hive/Spark UDFs/UDAFs powered by ​​RoaringBitmap​​, a state-of-the-art compressed bitmap data structure.    
> 该项目提供了一套高性能的Hive/Spark UDF（用户定义函数）和UDAF（用户定义聚合函数）套件，其核心驱动力为​​RoaringBitmap​​——一种先进的压缩位图数据结构。

It is specifically engineered to handle massive datasets, enabling ​​lightning-fast set operations​​ (intersection, union, difference) and ​​precise cardinality estimation​​ with minimal memory footprint.   
> 它专为处理海量数据集而设计，能够以极低的内存开销实现​​非常快速的集合运算​​（如交集、并集、差集）和​​精确的基数统计​​。

By leveraging RoaringBitmap's intelligent compression (utilizing Array, Bitmap, and Run containers based on data sparsity), it dramatically reduces computational overhead and storage costs compared to traditional methods like COUNT(DISTINCT) or native Hive arrays, making large-scale distinct counting and user cohort analysis both efficient and practical 
> 通过利用RoaringBitmap的智能压缩技术（根据数据稀疏程度，动态使用Array、Bitmap和Run等容器），与传统方法（如COUNT(DISTINCT)）或Hive原生数组相比，它能​​显著降低计算开销和存储成本​​，使得大规模精确去重和用户分群分析既高效又实用。

# Features
​​**High Performance**​​: Leverages RoaringBitmap's compression for superior speed over traditional SET or ARRAY aggregation.
> ​​高性能​​: 利用 RoaringBitmap 的压缩特性，速度远优于传统的 SET或 ARRAY聚合操作。

​**​Rich Set Operations**​​: Supports union (rb_or), intersection (rb_and), difference (rb_andnot) and more between multiple RoaringBitmaps.
> ​​丰富的集合运算​​: 支持多个 RoaringBitmap 之间的并集 (rb_or)、交集(rb_and)、差集 (rb_andnot) 等操作。

​**​Cardinality Estimation**​​: Quickly calculate the number of unique elements (cardinality) in single or aggregated bitmaps.
> 基数统计​​: 快速计算单个或多个聚合后的 RoaringBitmap 的基数（唯一元素个数）。

**Serialization/Deserialization​**​: Convert RoaringBitmaps to and from Hive BIGINTarrays for storage and transmission.
> ​序列化/反序列化​​: 支持将 RoaringBitmap 与 Hive 的 BIGINT数组相互转换，便于存储和传输。    

**Flexible Construction**​​: Build RoaringBitmaps directly from BIGINTarrays or aggregated column values.
> ​​灵活的构建方式​​: 支持从 BIGINT数组或列转行的结果直接构建 RoaringBitmap。

# Versions
| Dependency      | Version     | Scope       |
| -----------     | ----------- | ----------- |
| ​​JDK​​             | 1.8         | compile |
| ​​Apache Spark​​    | 3.3.0       | provided    |
| ​​Apache Hive​​     | 3.1.0       | provided    |
| ​​RoaringBitmap​​   | 1.3.0       | compile     |
| ​​Jackson​​         | 2.13.0      | provided    |
| ​​Janino​​          | 3.0.16      | provided    |
| ​​JUnit​​           | RELEASE     | test        |
| ​​JUnit Jupiter​​   | 5.11.4      | test        |

Refer to pom.xml for the details.



# Installation & Deployment
## 1. Obtain the UDF JAR 
### Option 1: Download the latest JAR file  from the project's Releases page. 
> 方法1: 从项目的 Releases页面下载最新的 JAR 文件。  

https://github.com/XiaohuangMeow/roaringbitmap_hive_udf/releases


### Option 2: Build from Source​​
> 方法2: 从源码构建文件。

```bash
git clone https://github.com/XiaohuangMeow/roaringbitmap_hive_udf.git
cd roaringbitmap_hive_udf
mvn clean package
```
The built JAR will be in the target/directory.
> 构建完成后，JAR 文件将位于 target/目录下。

## 2. Register UDFs/UDAFs in Hive
First put the Jar on the cluster or local environment.
> 先把Jar包放在本地或集群环境。
```sql
ADD JAR /path/to/RoaringBitmap-1.0-SNAPSHOT.jar;
```
Then register the UDF/UDAF functions.
> 先然后注册所有的UDF/UDAF函数。
```sql
-- Register all UDF functions
-- UDF
CREATE TEMPORARY FUNCTION rb_and as 'roaringbitmap.udf.RoaringBitmapAnd';
CREATE TEMPORARY FUNCTION rb_and_cardinality as 'roaringbitmap.udf.RoaringBitmapAndCardinality';
CREATE TEMPORARY FUNCTION rb_andnot as 'roaringbitmap.udf.RoaringBitmapAndNot';
CREATE TEMPORARY FUNCTION rb_andnot_cardinality as 'roaringbitmap.udf.RoaringBitmapAndNotCardinality';
CREATE TEMPORARY FUNCTION rb_build as 'roaringbitmap.udf.RoaringBitmapBuild';
CREATE TEMPORARY FUNCTION rb_cardinality as 'roaringbitmap.udf.RoaringBitmapCardinality';
CREATE TEMPORARY FUNCTION rb_clear as 'roaringbitmap.udf.RoaringBitmapClear';
CREATE TEMPORARY FUNCTION rb_contains as 'roaringbitmap.udf.RoaringBitmapContains';
CREATE TEMPORARY FUNCTION rb_equal as 'roaringbitmap.udf.RoaringBitmapEqual';
CREATE TEMPORARY FUNCTION rb_fill as 'roaringbitmap.udf.RoaringBitmapFill';
CREATE TEMPORARY FUNCTION rb_intersect as 'roaringbitmap.udf.RoaringBitmapIntersect';
CREATE TEMPORARY FUNCTION rb_is_empty as 'roaringbitmap.udf.RoaringBitmapIsEmpty';
CREATE TEMPORARY FUNCTION rb_maximum as 'roaringbitmap.udf.RoaringBitmapMaximum';
CREATE TEMPORARY FUNCTION rb_minimum as 'roaringbitmap.udf.RoaringBitmapMinimum';
CREATE TEMPORARY FUNCTION rb_not_equal as 'roaringbitmap.udf.RoaringBitmapNotEqual';
CREATE TEMPORARY FUNCTION rb_or as 'roaringbitmap.udf.RoaringBitmapOr';
CREATE TEMPORARY FUNCTION rb_or_cardinality as 'roaringbitmap.udf.RoaringBitmapOrCardinality';
CREATE TEMPORARY FUNCTION rb_range as 'roaringbitmap.udf.RoaringBitmapRange';
CREATE TEMPORARY FUNCTION rb_range_cardinality as 'roaringbitmap.udf.RoaringBitmapRangeCardinality';
CREATE TEMPORARY FUNCTION rb_to_array as 'roaringbitmap.udf.RoaringBitmapToArray';
CREATE TEMPORARY FUNCTION rb_xor as 'roaringbitmap.udf.RoaringBitmapXor';
CREATE TEMPORARY FUNCTION rb_xor_cardinality as 'roaringbitmap.udf.RoaringBitmapXorCardinality';
-- UDAF
CREATE TEMPORARY FUNCTION rb_and_agg as 'roaringbitmap.udaf.RoaringBitmapAndAgg';
CREATE TEMPORARY FUNCTION rb_and_cardinality_agg as 'roaringbitmap.udaf.RoaringBitmapAndCardinalityAgg';
CREATE TEMPORARY FUNCTION rb_build_agg as 'roaringbitmap.udaf.RoaringBitmapBuildAgg';
CREATE TEMPORARY FUNCTION rb_cardinality_agg as 'roaringbitmap.udaf.RoaringBitmapCardinalityAgg';
CREATE TEMPORARY FUNCTION rb_or_agg as 'roaringbitmap.udaf.RoaringBitmapOrAgg';
CREATE TEMPORARY FUNCTION rb_or_cardinality_agg as 'roaringbitmap.udaf.RoaringBitmapOrCardinalityAgg';
CREATE TEMPORARY FUNCTION rb_xor_agg as 'roaringbitmap.udaf.RoaringBitmapXorAgg';
CREATE TEMPORARY FUNCTION rb_xor_cardinality_agg as 'roaringbitmap.udaf.RoaringBitmapXorCardinalityAgg';
```

# Usage Examples
## 1. Sample Data
Assume a user_page_viewstable storing page IDs visited by users.
> 假设有一张 user_page_views表，存储用户访问的页面ID。
| user_id   | page_id | 
| :---:     | :----:  |
| 1         | 1001    |
| 2         | 1002    |
| 3         | 1001    |
| 4         | 1003    |

## 2. Build RoaringBitmap
```sql
SELECT
  user_id,
  rb_build_agg(page_id) AS page_bitmap
FROM user_page_views
GROUP BY user_id;
```

## 3. Calculate Cardinality
```sql
SELECT
  user_id,
  rb_cardinality_agg(page_id) AS unique_page_count
FROM user_page_views
GROUP BY user_id;
```

## 4. Set Operations
​​Use rb_or_agg(union) to find all pages visited by any user.​​
> ​​使用 rb_or_agg(并集) 计算所有用户访问过的页面集合。​​
```sql
SELECT rb_to_array(rb_or_agg(rb_build(page_id))) AS all_pages_array
FROM user_page_views;
```

# Important Notes
## Explicit Type Conversion When Using rb_build
Explicit type casting is highly recommended when using the rb_build UDF function in some Hive/Spark version.   
> 由于版本问题，强烈推荐只用rb_build进行显式类型转化，否则某些情况会报错。
```sql
-- Might cause errors in some versions.
SELECT rb_cardinality(rb_build(ARRAY(1L, 2L, 3L))) AS rb;

-- Recommend
SELECT rb_cardinality(rb_build(CAST(ARRAY(1L, 2L, 3L) AS ARRAY<BIGINT>))) AS rb;
```

## BIGINT Mandatory for Integer Parameters
When an integer type is passed as a parameter, it **MUST** be of **BIGINT** type. The UDF/UDAF will **NOT** perform implicit type conversion internally.
> 当整型数据作为参数传入时，​​必须​​为 ​​BIGINT​​ 类型。UDF/UDAF 内部​​不会​​执行隐式类型转换。

## Memory Comsumtion
Although RoaringBitmap is optimized for storage, it can still consume a significant amount of memory when processing large data volumes. In such cases, it is necessary to adjust the executor's memory.
> 尽管roaringbitmap针对存储进行了优化，但是在数据量大的情况下，依然需要消费大量内存，这种情况下，需要调整executor的内存
```sql
-- For example
set spark.executor.memory=60G;
```
## Aovid Excessively Large Size
During serialization/deserialization in UDF/UDAF workflows, and especially during union set operations, the RoaringBitmap's size must remain within Hive's handling capacity for binary data to avoid errors.
> 在UDF/UDAF工作流的序列化/反序列化过程中，特别是在并集运算期间，RoaringBitmap的大小必须始终控制在Hive Binary类型范围内，以避免发生错误。

# API Documentation
## UDAF
### rb_and_agg
Aggregate function that computes the intersection of multiple RoaringBitmaps.
```sql
binary rb_and_agg(binary bitmap)
```
bitmap(BINARY): RoaringBitmap column.
```sql
SELECT category,rb_and_agg(bitmap_column) AS aggregated_intersection
FROM bitmap_table
GROUP BY category;
```
### rb_and_cardinality_agg
Aggregate function that computes the cardinality of the intersection of multiple RoaringBitmaps.
```sql
bigint rb_and_cardinality_agg(binary bitmap)
```
bitmap(BINARY): RoaringBitmap column.
```sql
SELECT category,rb_and_cardinality_agg(bitmap_column) AS intersection_count
FROM bitmap_table
GROUP BY category;
```
### rb_build_agg
Aggregate function that builds a RoaringBitmap from multiple BIGINT values.
```sql
binary rb_build_agg(bigint value)
```
value(BIGINT): Integer value column.
```sql
SELECT category,rb_build_agg(user_id) AS user_bitmap
FROM user_activity
GROUP BY category;
```
### rb_cardinality_agg
Aggregate function that computes cardinality from multiple RoaringBitmaps.
```sql
bigint rb_cardinality_agg(binary bitmap)
```
bitmap(BINARY): RoaringBitmap column.
```sql
SELECT category,rb_cardinality_agg(bitmap_column) AS total_count
FROM bitmap_table
GROUP BY category;
```
### rb_or_agg
Aggregate function that computes the union of multiple RoaringBitmaps.
```sql
binary rb_or_agg(binary bitmap)
```
bitmap(BINARY): RoaringBitmap column.
```sql
SELECT category,rb_or_agg(bitmap_column) AS aggregated_union
FROM bitmap_table
GROUP BY category;
```
### rb_or_cardinality_agg
Aggregate function that computes the cardinality of the union of multiple RoaringBitmaps.
```sql
bigint rb_or_cardinality_agg(binary bitmap)
```
bitmap(BINARY): RoaringBitmap column.
```sql
SELECT category,rb_or_cardinality_agg(bitmap_column) AS union_count
FROM bitmap_table
GROUP BY category;
```
### rb_xor_agg 
Aggregate function that computes the symmetric difference of multiple RoaringBitmaps.
```sql
binary rb_xor_agg(binary bitmap)
```
bitmap(BINARY): RoaringBitmap column.
```sql
SELECT category,rb_xor_agg(bitmap_column) AS aggregated_xor
FROM bitmap_table
GROUP BY category;
```
### rb_xor_cardinality_agg
Aggregate function that computes the cardinality of the symmetric difference of multiple RoaringBitmaps.
```sql
bigint rb_xor_cardinality_agg(binary bitmap)
```
bitmap(BINARY): RoaringBitmap column.
```sql
SELECT category,rb_xor_cardinality_agg(bitmap_column) AS xor_count
FROM bitmap_table
GROUP BY category;
```


## UDF
### rb_and
Computes the intersection (bitwise AND) of two RoaringBitmaps.  
```sql
binary rb_and(binary bitmap1, binary bitmap2)
```
bitmap1(BINARY): First RoaringBitmap.    
bitmap2(BINARY): Second RoaringBitmap.   
```sql
SELECT rb_and(rb_build(ARRAY(1L, 2L, 3L)), rb_build(ARRAY(2L, 3L, 4L))) AS result;
-- RoaringBitmap containing values [2, 3]
```
### rb_and_cardinality
Computes the cardinality of the intersection of two RoaringBitmaps.
```sql
bigint rb_and_cardinality(binary bitmap1, binary bitmap2)
```
bitmap1(BINARY): First RoaringBitmap.   
bitmap2(BINARY): Second RoaringBitmap.
```sql
SELECT rb_and_cardinality(rb_build(ARRAY(1L, 2L, 3L)), rb_build(ARRAY(2L, 3L, 4L))) AS 
count;
-- Returns: 2
```
### rb_andnot
Computes the difference (bitwise ANDNOT) between two RoaringBitmaps.
```sql
binary rb_andnot(binary bitmap1, binary bitmap2)
```
bitmap1(BINARY): First RoaringBitmap.  
bitmap2(BINARY): Second RoaringBitmap.
```sql
SELECT rb_andnot(rb_build(ARRAY(1L, 2L, 3L)), rb_build(ARRAY(2L, 3L, 4L))) AS result;
-- RoaringBitmap containing values [1]
```
### rb_andnot_cardinality
Computes the cardinality of the difference between two RoaringBitmaps.
```sql
bigint rb_andnot_cardinality(binary bitmap1, binary bitmap2)
```
bitmap1(BINARY): First RoaringBitmap.   
bitmap2(BINARY): Second RoaringBitmap.
```sql
SELECT rb_andnot_cardinality(rb_build(ARRAY(1L, 2L, 3L)), rb_build(ARRAY(2L, 3L, 4L))) AS count;
-- Returns: 1
```
### rb_build
Creates a RoaringBitmap from an array of BIGINT values.
```sql
binary rb_build(array<bigint> values)
```
values(ARRAY<BIGINT>): Array of integer values.
```sql
SELECT rb_build(ARRAY(1L, 2L, 3L, 5L, 8L)) AS bitmap;
-- RoaringBitmap containing values [1, 2, 3, 5, 8]
```
### rb_cardinality
Returns the number of elements in a RoaringBitmap.
```sql
bigint rb_cardinality(binary bitmap)
```
bitmap(BINARY): Input RoaringBitmap.
```sql
SELECT rb_cardinality(rb_build(ARRAY(1L, 2L, 3L, 5L))) AS count;
-- Returns: 4
```
### rb_clear
Clears a range of values in a RoaringBitmap.
```sql
binary rb_clear(binary bitmap, bigint start, bigint end)
```
bitmap(BINARY): Input RoaringBitmap.   
start(BIGINT): Start of range (inclusive).   
end(BIGINT): End of range (exclusive).   
```sql
SELECT rb_clear(rb_build(ARRAY(1L, 2L, 3L)) ,1L ,3L) AS empty_bitmap;
-- RoaringBitmap containing values [3]
```
### rb_contains
Checks if a specific value exists in the RoaringBitmap.
```sql
boolean rb_contains(binary bitmap, bigint value)
```
bitmap(BINARY): Input RoaringBitmap.    
value(BIGINT): Value to check.
```sql
SELECT rb_contains(rb_build(ARRAY(1L, 2L, 3L)), 2L) AS contains;
-- Returns: true
```
### rb_equal
Checks if two RoaringBitmaps are equal.
```sql
boolean rb_equal(binary bitmap1, binary bitmap2)
```
bitmap1(BINARY): First RoaringBitmap.    
bitmap2(BINARY): Second RoaringBitmap.
```sql
SELECT rb_equal(rb_build(ARRAY(1L, 2L)), rb_build(ARRAY(1L, 2L))) AS is_equal;
-- Returns: true
```
### rb_fill
Fills a range of values in a RoaringBitmap.
```sql
binary rb_fill(binary bitmap, bigint start, bigint end)
```
bitmap(BINARY): Input RoaringBitmap.   
start(BIGINT): Start of range (inclusive).   
end(BIGINT): End of range (exclusive).
```sql
SELECT rb_fill(rb_build(ARRAY(1L)), 5L, 8L) AS filled_bitmap;
-- RoaringBitmap containing values [1, 5, 6, 7]
```
### rb_intersect
Checks if two RoaringBitmaps have any overlapping elements.
```sql
boolean rb_intersect(binary bitmap1, binary bitmap2)
```
bitmap1(BINARY): First RoaringBitmap.   
bitmap2(BINARY): Second RoaringBitmap. 
```sql
SELECT rb_intersect(rb_build(ARRAY(1L, 2L)), rb_build(ARRAY(2L, 3L))) AS intersects;
-- Returns: true
```
### rb_is_empty
Checks if a RoaringBitmap is empty.
```sql
boolean rb_is_empty(binary bitmap)
```
bitmap(BINARY): Input RoaringBitmap.
```sql
SELECT rb_is_empty(rb_build(ARRAY())) AS is_empty;
-- Returns: true
```
### rb_maximum
Returns the maximum value in a RoaringBitmap.
```sql
bigint rb_maximum(binary bitmap)
```
bitmap(BINARY): Input RoaringBitmap.
```sql
SELECT rb_maximum(rb_build(ARRAY(1L, 5L, 3L))) AS max_value;
-- Returns: 5
```
### rb_minimum
Returns the minimum value in a RoaringBitmap.
```sql
bigint rb_minimum(binary bitmap)
```
bitmap(BINARY): Input RoaringBitmap.
```sql
SELECT rb_minimum(rb_build(ARRAY(5L, 1L, 3L))) AS min_value;
-- Returns: 1
```
### rb_not_equal
Checks if two RoaringBitmaps are not equal.
```sql
boolean rb_not_equal(binary bitmap1, binary bitmap2)
```
bitmap1(BINARY): First RoaringBitmap.   
bitmap2(BINARY): Second RoaringBitmap.
```sql
SELECT rb_not_equal(rb_build(ARRAY(1L, 2L)), rb_build(ARRAY(1L, 3L))) AS not_equal;
-- Returns: true
```
### rb_or
Computes the union (bitwise OR) of two RoaringBitmaps.
```sql
binary rb_or(binary bitmap1, binary bitmap2)
```
bitmap1(BINARY): First RoaringBitmap.
bitmap2(BINARY): Second RoaringBitmap.
```sql
SELECT rb_or(rb_build(ARRAY(1L, 2L)), rb_build(ARRAY(2L, 3L))) AS union_bitmap;
-- RoaringBitmap containing values [1, 2, 3]
```
### rb_or_cardinality
Computes the cardinality of the union of two RoaringBitmaps.
```sql
bigint rb_or_cardinality(binary bitmap1, binary bitmap2)
```
bitmap1(BINARY): First RoaringBitmap.
bitmap2(BINARY): Second RoaringBitmap.
```sql
SELECT rb_or_cardinality(rb_build(ARRAY(1L, 2L)), rb_build(ARRAY(2L, 3L))) AS count;
-- Returns: 3
```
### rb_range
Creates a RoaringBitmap containing a range of values.
```sql
binary rb_range(bigint start, bigint end)
```
start(BIGINT): Start value (inclusive).
end(BIGINT): End value (exclusive).
```sql
SELECT rb_range(5L, 10L) AS range_bitmap;
-- RoaringBitmap containing values [5, 6, 7, 8, 9]
```
### rb_range_cardinality
Computes the cardinality of a value range.
```sql
bigint rb_range_cardinality(bigint start, bigint end)
```
start(BIGINT): Start value (inclusive).   
end(BIGINT): End value (exclusive).
```sql
SELECT rb_range_cardinality(5L, 10L) AS range_count;
-- Returns: 5
```
### rb_to_array
Converts a RoaringBitmap to an array of BIGINT values.  
```sql
array<bigint> rb_to_array(binary bitmap).
```
bitmap(BINARY): Input RoaringBitmap.
```sql
SELECT rb_to_array(rb_build(ARRAY(1L, 2L, 3L))) AS values_array;
-- Returns: [1, 2, 3]
```
### rb_xor
Computes the symmetric difference (bitwise XOR) of two RoaringBitmaps.
```sql
binary rb_xor(binary bitmap1, binary bitmap2)
```
bitmap1(BINARY): First RoaringBitmap.
bitmap2(BINARY): Second RoaringBitmap.
```sql
SELECT rb_xor(rb_build(ARRAY(1L, 2L)), rb_build(ARRAY(2L, 3L))) AS xor_bitmap;
-- RoaringBitmap containing values [1, 3]
```
### rb_xor_cardinality
Computes the cardinality of the symmetric difference of two RoaringBitmaps.
```sql
bigint rb_xor_cardinality(binary bitmap1, binary bitmap2)
```
bitmap1(BINARY): First RoaringBitmap.    
bitmap2(BINARY): Second RoaringBitmap.
```sql
SELECT rb_xor_cardinality(rb_build(ARRAY(1L, 2L)), rb_build(ARRAY(2L, 3L))) AS count;
-- Returns: 2
```

## Contributing
Issues and Pull Requests are welcome! 
> 欢迎提交 Issue 和 Pull Request！
