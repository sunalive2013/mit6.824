# mit6.824
MIT的分布式入门课程
网址：https://pdos.csail.mit.edu/6.824/labs/lab-1.html 
lab1：阅读mapreduce论文，完成相关作业
partI：
完成doMap(）调用map函数，将输入文件进行分割，以kv的格式写入中间文件，中间文件也是kv格式
完成doReduce()读入中间文件，按照key进行排序后，调用reduce函数进行输出

partII：
完成map函数，使用进行wordcount统计，将结果以["word","1"]的格式计入输出结果
完成reduce函数，对每个["word","1"进行统计累加，计算出每个单词出现的次数，输出这个结果
