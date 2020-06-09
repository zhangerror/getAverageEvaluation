## getAverageEvaluation

- 简单Map/reduce程序设计及测试

## 程序功能：

- 数据集ramen-ratings.txt，包含全世界2580种方便面的品牌、国家、包装类型、评分等内容，
使用MapReduce计数并求平均值
输出：所有国家方便面的平均分（即哪国的方便面最好吃）

- 代码逻辑：

MAP
输入：一行数据
处理：使用空格将字符串split成数组，提取国家和评分，分别作为键和值输出
输出：<国家, 评分>

Reduce：
输入：<国家，[该国家所有的评分数组]> 
处理：计算平均分
输出：<国家，平均分> 

## environment
- win10
- IntelliJ IDEA Community Edition 2020.1.1 x64
- virtualbox
- centos7
- Hadoop3.1.3
- JDK1.8
- apche-maven-3.6.3
