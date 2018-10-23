import java.io.*;
import java.util.*;
import java.lang.Math;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {

  private double dist(double[] x1, double[] x2)
  {
    double result=0;
    for(int i=0;i<x1.length;i++)
    {
      result+=(x1[i]- x2[i])*(x1[i]- x2[i]);
    }
    return Math.sqrt(result);
  }

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private List<double[]> centroids = new ArrayList();

    //public void setup(Context context) throws IOException {
      /*BufferReader reader = new BufferReader(new FileReader(centerfile));
      String line;
      while(line = reader.readLine()!=null)
      {
        String[] data = line.split(" ");
        float[] pt = new float(data.length);
        for(int i=0;i<data.length;i++)
        {
          pt[i] = Float.ParseFloat(data[i]);
        }
        centroids.append(pt);
      }*/

    //}
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      /*String[] data=value.toString().split(" ");
      float[] pt= new float[value.length];
      for(i=0;i<pt.length;i++)
      {
        pt[i]=float.ParseFloat(data[i])
      }
      for*/
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      /*int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);*/
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "KMeans");

    //new added code
    //FileSystem fs = FileSystem.get(new Configuration)
    //BufferReader bf=new BufferReader(new InputStreamReader(fs.open())
    System.out.println("main function!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    System.out.println(args[0]);

    //read the data
    Path path=new Path(args[0]+"/cho.txt");
    FileSystem fs = path.getFileSystem(conf);
    FSDataInputStream inputStream = fs.open(path);
    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
    String line=br.readLine();
    List<String> pts=new ArrayList();
    while(line != null){
      System.out.println(line);
      pts.add(line);
      line=br.readLine();
    }

    //random select points as initial centroid
    Random rng=new Random();
    Set<Integer> idx = new HashSet();
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
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}