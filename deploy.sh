#!/bin/bash

jar=$1
jar=${jar:=panda_spark.jar}
echo $jar
mvn clean compile package

dir=/Users/likaiqing/space/panda/panda_spark/target
mv $dir/panda_spark-1.0-SNAPSHOT.jar $dir/$jar

scp -p $dir/$jar root@10.131.6.45:/root/jar/

rsync -auvz --password-file=/Users/likaiqing/.panda_rsyncd.secrets  $dir/$jar likaiqing@recruit1v.bigdata.bjtb.pdtv.it::likaiqing/
