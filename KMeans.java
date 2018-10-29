import java.io.*;
import java.util.*;
import java.lang.Math;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {

  private static double dist(double[] x1, double[] x2)
  {
    double result=0;
    for(int i=0;i<x1.length;i++)
    {
      result+=(x1[i]- x2[i])*(x1[i]- x2[i]);
    }
    return Math.sqrt(result);
  }

  private static boolean equal(double[] x1, double[] x2)
  {
    for(int i=0;i<x1.length;i++)
      if(x1[i]!=x2[i])
        return false;
    return true;
  }

  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private List<double[]> centroids = new ArrayList();

    public void setup(Context context) throws IOException {

      System.out.println("Setting up the mapper................................");
      Configuration conf = new Configuration();
      Path path=new Path("interm/centroids.txt");
      FileSystem fs = path.getFileSystem(conf);
      FSDataInputStream inputStream = fs.open(path);
      BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
      String line;
      while((line = br.readLine())!=null)
      {
        System.out.println(line);
        String[] data = line.split("\t");
        System.out.println(data.length);
        double[] pt = new double[data.length-2];
        for(int i=0;i<pt.length;i++)
        {
          pt[i] = Double.parseDouble(data[i+2]);
        }
        centroids.add(pt);
      }
      br.close();
    }
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      System.out.println("in the map function.......................................");
      String[] data=value.toString().split("\t");
      //System.out.println(data.length);
      //System.out.println(value);
      double[] pt= new double[data.length-2];
      for(int i=0;i<pt.length;i++)
      {
        pt[i]=Double.parseDouble(data[i+2]);
      }
      double mind=Double.MAX_VALUE;
      int mini=-1;
      for(int i=0;i<centroids.size();i++)
      {
        double dis=dist(pt,centroids.get(i));
        if(dis < mind)
        {
          mind = dis;
          mini=i;
        }
      }
      System.out.println(value.toString());
      String out=value.toString();
      out=out+"\t"+"1.0";
      context.write(new IntWritable(mini),new Text(out));
    }
  }

  public static class IntSumReducer
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    private IntWritable result = new IntWritable();
    private List<double[]> centroids = new ArrayList();
    public static enum Counter{
      CONVERGED;
    }


    public void setup(Context context) throws IOException {
      System.out.println("set up reducer...............................................");
      Configuration conf = new Configuration();
      Path path=new Path("interm/centroids.txt");
      FileSystem fs = path.getFileSystem(conf);
      FSDataInputStream inputStream = fs.open(path);
      BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
      String line;
      while((line = br.readLine())!=null)
      {
        System.out.println(line);
        String[] data = line.split("\t");
        System.out.println(data.length);
        double[] pt = new double[data.length-2];
        for(int i=0;i<pt.length;i++)
        {
          pt[i] = Double.parseDouble(data[i+2]);
        }
        centroids.add(pt);
      }
    }

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      System.out.println("in the reduce function..........................................");
      System.out.println("the value of key is"+key.get());
      double[] value=new double[centroids.get(0).length];
      double count=0;
      for (Text val : values) {
        String[] s=val.toString().split("\t");
        //System.out.println(val);
        //System.out.println(s.length);
        for(int i=0;i<value.length;i++)
          value[i]+=Double.parseDouble(s[i+2]);
        count+=Double.parseDouble(s[s.length-1]);
      }
      for(int i=0;i<value.length;i++)
        value[i]=value[i]/count;
      System.out.println("the count is "+String.valueOf(count));
      if(!equal(value,centroids.get(key.get())))
      {
        context.getCounter(Counter.CONVERGED).increment(1l);
      }
      String out="0\t";
      for(int i=0;i<value.length-1;i++)
        out+=(String.valueOf(value[i]))+"\t";
      out+=String.valueOf(value[value.length-1]);
      context.write(key, new Text(out));
    }
  }


  public static class KMeansCombiner
       extends Reducer<IntWritable,Text,IntWritable,Text> {


    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      System.out.println("in the conbine function~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
      System.out.println("the value of key is"+key.get());
      double[] value=null;
      double count=0;
      for (Text val : values) {
        String[] s=val.toString().split("\t");
        //System.out.println(val);
        System.out.println(s.length);
        if(value==null)
          value=new double[s.length-3];
        for(int i=0;i<value.length;i++)
          value[i]+=Double.parseDouble(s[i+2]);
        count+=Double.parseDouble(s[s.length-1]);
      }
      System.out.println("the count is "+String.valueOf(count));
      String out="0\t0\t";
      for(int i=0;i<value.length;i++)
        out+=(String.valueOf(value[i]))+"\t";
      out+=String.valueOf(count);
      context.write(key, new Text(out));
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "KMeans");

    //new added code
    //FileSystem fs = FileSystem.get(new Configuration)
    //BufferReader bf=new BufferReader(new InputStreamReader(fs.open())
    //System.out.println("main function!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    //System.out.println(args[0]);

    //read the data
    System.out.println("running the first time...............................................................");
    //change here to change the input files !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    Path path=new Path(args[0]+"/cho.txt");
    FileSystem fs = path.getFileSystem(conf);
    FSDataInputStream inputStream = fs.open(path);
    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
    String line=br.readLine();
    List<String> pts=new ArrayList();
    while(line != null){
      //System.out.println(line);
      pts.add(line);
      line=br.readLine();
    }

    //random select points as initial centroid
    Random rng=new Random();
    Set<Integer> idx = new HashSet();
    //change here to change the number of clusters!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    while(idx.size()<5)
    {
      idx.add(rng.nextInt(pts.size()));
    }

    //write initialpoint to file
    Path outpath=new Path("interm/centroids.txt");
    fs = path.getFileSystem(conf);
    if( fs.exists(outpath))
      fs.delete(outpath,true);
    FSDataOutputStream outputStream = fs.create(outpath);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));
    for(int i: idx)
    {
      bw.write(pts.get(i));
      bw.newLine();
    }
    bw.close();



    job.setJarByClass(KMeans.class);
    job.setMapperClass(TokenizerMapper.class);
    //Although I can use conbiner and reducer as well, How ever this cause the reducer to run twice which is not right.
    job.setCombinerClass(KMeansCombiner.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //System.exit(job.waitForCompletion(true) ? 0 : 1);

    job.waitForCompletion(true);
    long counter = job.getCounters().findCounter(IntSumReducer.Counter.CONVERGED).getValue();
    System.out.printf("Finish getting the counter");
    while(counter>0)
    {
      System.out.printf("running the job again.....................................................");
      conf=new Configuration();
      job=Job.getInstance(conf);
      Path resultfile=new Path("output/part-r-00000");
      if( fs.exists(outpath))
        fs.delete(outpath,true);
      fs.rename(resultfile,outpath);
      Path resultdir=new Path("output");
      if(fs.exists(resultdir))
        fs.delete(resultdir);
      job.setJarByClass(KMeans.class);
      job.setMapperClass(TokenizerMapper.class);
    //Although I can use conbiner and reducer as well, How ever this cause the reducer to run twice which is not right.
      job.setCombinerClass(KMeansCombiner.class);
      job.setReducerClass(IntSumReducer.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.waitForCompletion(true);
      counter = job.getCounters().findCounter(IntSumReducer.Counter.CONVERGED).getValue();
    }

    //reassign the points according to the cluster.
    System.out.println("begin to write clustering result!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    //read the centroid
    List<double[]> centroids=new ArrayList();
    conf = new Configuration();
    path=new Path("interm/centroids.txt");
    fs = path.getFileSystem(conf);
    inputStream = fs.open(path);
    br = new BufferedReader(new InputStreamReader(inputStream));
    while((line = br.readLine())!=null)
    {
      System.out.println(line);
      String[] data = line.split("\t");
      System.out.println(data.length);
      double[] pt = new double[data.length-2];
      for(int i=0;i<pt.length;i++)
          pt[i] = Double.parseDouble(data[i+2]);
      centroids.add(pt);
    }
    br.close();

    //classify the point
    List<Integer> tags=new ArrayList();
    path=new Path(args[0]+"/cho.txt");
    fs = path.getFileSystem(conf);
    inputStream = fs.open(path);
    br = new BufferedReader(new InputStreamReader(inputStream));
    while((line=br.readLine())!= null){
      System.out.println(line);
      String[] data = line.split("\t");
      double[] pt = new double[data.length-2];
      for(int i=0;i<pt.length;i++)
          pt[i] = Double.parseDouble(data[i+2]);
      int minidx=-1;
      double mindist= Double.MAX_VALUE;
      for(int i=0;i<centroids.size();i++)
        if (dist(pt,centroids.get(i))<mindist)
        {
          mindist=dist(pt,centroids.get(i));
          minidx=i;
        }
      tags.add(minidx);
    }

    //write the result 
    outpath=new Path(args[1]+"/tag.txt");
    fs = path.getFileSystem(conf);
    if( fs.exists(outpath))
      fs.delete(outpath,true);
    outputStream = fs.create(outpath);
    bw = new BufferedWriter(new OutputStreamWriter(outputStream));
    for(int i: tags)
    {
      bw.write(Integer.toString(i));
      bw.newLine();
    }
    bw.close();

  }
}