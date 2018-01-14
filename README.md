## Influxdb 数据迁移工具使用说明

### 主要用途

实现一段时间里influxdb数据库之间数据的导入导出，优化数据库的存储及读写性能

### 使用说明

执行命令go run main.go -选项 -参数值
各个选项及参数如下：
-h        帮助

-s        指定需要导出数据的数据库的主机地址，默认为127.0.0.1

-sport    指定需要导出数据的数据库的运行端口，默认为8086

-sdb      指定需要导出数据的数据库名，默认为mydb

-d        指定需要导入数据的数据库的主机地址，默认为127.0.0.1

-dport    指定需要导入数据的数据库的运行端口，默认为8086

-ddb      指定需要导入数据的数据库名，默认为yourdb

-stime    指定起始时间，可以用于实现某段时间内数据的导入导出，默认为"1970-01-01 00:00:00"

-etime    指定结束时间，可以用于实现某段时间内数据的导入导出，默认为"2100-01-01 00:00:00"

（注意：支持的导入时间宽度为一年以内，起始时间 -stime一定要写,否则默认导入1970起的数据，会产生错误）
