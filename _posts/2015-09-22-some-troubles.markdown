---
layout: post
title:  "记录hadoop环境的一些坑"
date:   2015-09-10 00:23:25
categories: code
tags: java
image: /assets/article_images/2014-11-30-mediator_features/night-track.JPG
---


1. `Exception in thread "main" java.lang.UnsupportedOperationException: Not implemented by the DistributedFileSystem FileSystem implementation` 

jar包冲突，maven依赖中尽量不要使用hadoop core的jar包

2. `org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException`

文件流冲突

一般创建文件时都会打开一个供写入的文件流。而我们希望是追加，所以如果使用了错误的API ，就有可能引起上述问题。以FileSystem类为例，如果使用create()方法之后再调用append()方法，就会抛出上述异常。所以最好使用createNewFile方法，只创建文件，不打开流

3. `Failed to APPEND_FILE`

append开关没有打开，在`hdfs-site.xml`中需要配置
{% highlight java %}
<property>
    <name>dfs.support.append</name>
    <value>true</value>
</property>
{% endhighlight %}
或者client调用时在conf中set相应kv对
{% highlight java %}
Configuration conf = new Configuration();
conf.setBoolean("dfs.support.append", true);
{% endhighlight %}

