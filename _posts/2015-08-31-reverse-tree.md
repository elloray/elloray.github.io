{% highlight java %}
public class FirstPartitioner extends Partitioner<KVPair,IntWritable>{
    @Override
    public int getPartition(KVPair key, IntWritable value,
                            int numPartitions) {
        return key.getFirst().hashCode();
    }
}
{% endhighlight %}