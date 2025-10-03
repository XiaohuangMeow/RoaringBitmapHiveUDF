# RoaringBitmap Hive UDF

## Introduction
This project delivers a high-performance suite of Hive/Spark UDFs/UDAFs powered by ​​RoaringBitmap​​, a state-of-the-art compressed bitmap data structure. It is specifically engineered to handle massive datasets, enabling ​​lightning-fast set operations​​ (intersection, union, difference) and ​​precise cardinality estimation​​ with minimal memory footprint. By leveraging RoaringBitmap's intelligent compression (utilizing Array, Bitmap, and Run containers based on data sparsity), it dramatically reduces computational overhead and storage costs compared to traditional methods like COUNT(DISTINCT) or native Hive arrays, making large-scale distinct counting and user cohort analysis both efficient and practical 

> 该项目提供了一套高性能的Hive/Spark UDF（用户定义函数）和UDAF（用户定义聚合函数）套件，其核心驱动力为​​RoaringBitmap​​——一种先进的压缩位图数据结构。该套件专为处理海量数据集而设计，能够以极低的内存开销实现​​非常快速的集合运算​​（如交集、并集、差集）和​​精确的基数统计​​。通过利用RoaringBitmap的智能压缩技术（根据数据稀疏程度，动态使用Array、Bitmap和Run等容器），与传统方法（如COUNT(DISTINCT)）或Hive原生数组相比，它能​​显著降低计算开销和存储成本​​，使得大规模精确去重和用户分群分析既高效又实用。

## Features
​​High Performance​​: Leverages RoaringBitmap's compression for superior speed over traditional SET or ARRAY aggregation.
> ​​高性能​​: 利用 RoaringBitmap 的压缩特性，速度远优于传统的 SET或 ARRAY聚合操作。

​​Rich Set Operations​​: Supports union (rb_or), intersection (rb_and), difference (rb_andnot) and more between multiple RoaringBitmaps.
> ​​丰富的集合运算​​: 支持多个 RoaringBitmap 之间的并集 (rb_or)、交集(rb_and)、差集 (rb_andnot) 等操作。

​​Cardinality Estimation​​: Quickly calculate the number of unique elements (cardinality) in single or aggregated bitmaps.
> 基数统计​​: 快速计算单个或多个聚合后的 RoaringBitmap 的基数（唯一元素个数）。

Serialization/Deserialization​​: Convert RoaringBitmaps to and from Hive BIGINTarrays for storage and transmission.
> ​序列化/反序列化​​: 支持将 RoaringBitmap 与 Hive 的 BIGINT数组相互转换，便于存储和传输。

​​Flexible Construction​​: Build RoaringBitmaps directly from BIGINTarrays or aggregated column values.
> ​​灵活的构建方式​​: 支持从 BIGINT数组或列转行的结果直接构建 RoaringBitmap。

## Installation & Deployment
### 1. Obtain the UDF JAR 
#### Option 1: Download the latest JAR file (e.g. ) from the project's Releases page. 
> 方法1: 从项目的 Releases页面下载最新的 JAR 文件。
> JAVA版本修正《todo》

#### Option 2: Build from Source​​
> 方法2: 从源码构建文件。

```bash
git clone https://github.com/XiaohuangMeow/roaringbitmap_hive_udf.git
cd roaringbitmap_hive_udf
mvn clean package
```
The built JAR will be in the target/directory.
> 构建完成后，JAR 文件将位于 target/目录下。

### 2. Register UDFs/UDAFs in Hive
```sql
ADD JAR /path/on/server/to/roaringbitmap-hive-udf-1.0.0.jar;

-- Register all UDF functions / 注册所有 UDF 函数
CREATE TEMPORARY FUNCTION rb_build AS 'com.yourcompany.hive.udf.RoaringBitmapBuildUDF';
CREATE TEMPORARY FUNCTION rb_or AS 'com.yourcompany.hive.udf.RoaringBitmapOrUDF';
CREATE TEMPORARY FUNCTION rb_and AS 'com.yourcompany.hive.udf.RoaringBitmapAndUDF';
CREATE TEMPORARY FUNCTION rb_cardinality AS 'com.yourcompany.hive.udf.RoaringBitmapCardinalityUDF';
CREATE TEMPORARY FUNCTION rb_serialize AS 'com.yourcompany.hive.udf.RoaringBitmapSerializeUDF';
```

## Usage 
### 1. Sample Data
Assume a user_page_viewstable storing page IDs visited by users.
> 假设有一张 user_page_views表，存储用户访问的页面ID。

| user_id   | page_id | 
| :---:     | :----:  |
| 1         | 1001    |
| 2         | 1002    |
| 3         | 1001    |
| 4         | 1003    |

### 2. Build RoaringBitmap 
```sql
SELECT
  user_id,
  rb_build(page_id) AS page_bitmap
FROM user_page_views
GROUP BY user_id;
```

### 3. Calculate Cardinality
```sql
SELECT
  user_id,
  rb_or_cardinality_agg(rb_build(page_id)) AS unique_page_count
FROM user_page_views
GROUP BY user_id;
```

### 4. Set Operations / 集合运算
​​Use rb_or(union) to find all pages visited by any user.​​
> ​​使用 rb_or(并集) 计算所有用户访问过的页面集合。​​
```sql
SELECT rb_to_array(rb_or_agg(rb_build(page_id))) AS all_pages_array
FROM user_page_views;
```

### 5. Impprtant Notes
todo

## API Reference
todo

## Technique
todo

## Contributing
Issues and Pull Requests are welcome! 
> 欢迎提交 Issue 和 Pull Request！
