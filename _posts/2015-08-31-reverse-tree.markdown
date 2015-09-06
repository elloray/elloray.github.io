---
layout: post
title:  "用map/reduce实现倒排索引"
date:   2015-09-01 02:23:25
categories: code
tags: mapreduce
image: /assets/article_images/2014-11-30-mediator_features/night-track.JPG
---
最近开发接到了一个需求，需求描述是这样的：

- 用户数量大约1亿级别
- 每个用户的指标维度在50个左右
- 需要进行根据指标快速分群，查询符合特征的人群。

最后的结论是先生成倒排树，再根据特征生成`bitmap`做索引，实现快速查询。

我这边的开发工作主要是根据原始数据，生成倒排树。项目提供的数据主要是两块：id-mobile映射的文件和mobile为主键的用户指标文件。

所以在设计任务流程的时候，将这个任务分成了两个阶段:

1. join阶段，对两个文件做join，生成id为主键的用户指标文件(一定要用id作为主键的原因是很多文件中mobile是重复的，而我们需要一个唯一标识)
2. 生成倒排索引的阶段，根据join阶段的输出文件做倒排树。

join阶段的实现很简单，不再赘述。主要在于生成倒排树的阶段：

join阶段产出的文件如下所示
{% highlight java %}
id        age        gender        city
1         15         male          beijing
2         15         female        beijing
3         20         male          beijing
4         15         male          shanghai
5         20         male          hangzhou
6         15         female        shanghai
{% endhighlight %}
对这个文件做倒排树，我们需要根据每个指标，根据不同的指标值存放符合条件的id，这里有两种存放方式：

1. 每个指标作为一个文件夹，每个值作为一个文件
2. 每个指标作为一个文件，每个值的id分段存储

比较两种方式，方法1存的小文件过多，需要占用的文件句柄过多，而且再次操作map会浪费很多资源，所以选择方法2。

以age指标为例，最终的文件输出样式如下:
{% highlight java %}
###15
1，2，4，6
###20
3，5
{% endhighlight %}
简而言之，我们就是要将用户指标文件分解成如上表所示的指标倒排树。

map阶段，读取指标文件，输出key-value对时这里遇到一个问题：

我们需要以指标名作为key，这样每个reduce可以指向一个文件，提高性能。但是指标文件中一个指标的值的分布是随机分布的，并不是相同值分布在一起，所以我们需要按指标名做分区，而需要将指标值做排序。我这里处理的方法是新建一个数据结构，重写了shuffle阶段的sort和partition。

首先构造自己的数据结构：
{% highlight java %}
public class KVPair implements WritableComparable<KVPair>
{% endhighlight %}
这个数据结构包含两个成员
{% highlight java %}
private String first;
private String second;
{% endhighlight %}
first用来分区，second用来排序，在项目中将指标名放在first，指标值放在second

然后重写shuffle阶段的class

partition类:
{% highlight java %}
public class FirstPartitioner extends Partitioner<KVPair,IntWritable>{
    @Override
    public int getPartition(KVPair key, IntWritable value,
                            int numPartitions) {
        return key.getFirst().hashCode();
    }
}
{% endhighlight %}
sort类:
{% highlight java %}
public class FirstGroupingComparator implements RawComparator<KVPair> {
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return WritableComparator.compareBytes(b1, s1, Integer.SIZE / 8, b2,
				s2, Integer.SIZE / 8);
	}

	public int compare(KVPair o1, KVPair o2) {
		String l = o1.getFirst();
		String r = o2.getFirst();
		return l.compareTo(r);
	}
}
{% endhighlight %}

在map阶段输出如下kv对
{% highlight java %}
KVPair kvPair = new KVPair();
kvPair.setFirst(指标名);
kvPair.setSecond(指标值);
context.write(kvPair, new Text(id));
{% endhighlight %}
reduce阶段根据输入，需要做简单的数据处理即可，我这里为读取方便，每行做了最多1024个id的限制
{% highlight java %}
private final static String SEPARATOR = "/";

private final static int NUM_PER_LINE = 1024;

private MultipleOutputs<NullWritable, Text> output;

protected void setup(Context context) {
	output = new MultipleOutputs<NullWritable, Text>(context);
	logger.info("Now Reduce");
}
// 实现reduce函数
protected void reduce(KVPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// quota值变化标志位
		String lastvalue = "";
		// 计数器
		int counter = 0;
		Iterator<Text> it = values.iterator();
		String value = "";
		// 输出路径
		String baseOutputPath = key.getFirst();
		while (it.hasNext()) {
			// 相同值的seqid
			if (!key.getSecond().equals(lastvalue)) {
				if (counter != 0) {
					output.write(
							NullWritable.get(),
							new Text(value),
							key.getFirst().substring(
									key.getFirst().lastIndexOf(SEPARATOR) + 1,
									key.getFirst().length()));
					// counter清零
					counter = 0;
					// value清空
					value = "";
				}
				output.write(NullWritable.get(),
						new Text("###" + key.getSecond()), baseOutputPath);
				// lastvalue标志位变更
				lastvalue = key.getSecond();
			} else {
				// counter小于NUM_PER_LINE，将seqid放入value中
				if (counter < NUM_PER_LINE) {
					value += it.next() + ",";
					counter++;
				} else {
					// counter等于NUM_PER_LINE，输出一行id
					output.write(NullWritable.get(),
							new Text(value.substring(0, value.length() - 1)),
							baseOutputPath);
					// counter清零，value清空
					counter = 0;
					value = "";
				}
			}
		}
		// 输出最后的值
		output.write(NullWritable.get(),
				new Text(value.substring(0, value.length() - 1)),
				baseOutputPath);
	}
{% endhighlight %}

至此，完成了倒排树的主要实现部分。

在该需求下，基本倒排树的生成时间在45min左右，而且当数据扩容时平滑的增加任务个数即可，时间不会增加，这部分平滑扩容，可以和多任务调度一起放在任务控制部分，增强模块的可维护性，这部分的代码不再赘述。