package least5;
import java.io.*; 
import java.util.*; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 

  
public class Least_5_Mapper extends Mapper<Object, 
                            Text, Text, LongWritable> { 
  
    //private TreeMap<Long, String> tmap; 
    private TreeMap<String, Long> test;
  
    @Override
    public void setup(Context context) throws IOException, 
                                     InterruptedException 
    { 
       // tmap = new TreeMap<Long, String>(); 
    	test = new TreeMap<String, Long>();
    } 
  
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    { 
  
        // input data format => movie_name     
        // no_of_views  (tab seperated) 
        // we split the input data 
    	 
    	String word = value.toString();
    	long numtimes;
    	if(test.containsKey(word)) {
    		numtimes = (long)test.get(word) + 1;
    	}else {
    		numtimes = 1;
    	}
        
    	
    	test.put(word, numtimes);
    	//String[] tokens = value.toString().split("\t"); 
        //String word = tokens[0]; 
        //long numtimes = Long.parseLong(tokens[1]); 
        
        
        
         
    } 
  
    @Override
    public void cleanup(Context context) throws IOException, 
                                       InterruptedException 
    { 
        for (Map.Entry<String, Long> entry : test.entrySet())  
        { 
        	
            
        	long count = entry.getValue();
        	String name = entry.getKey();
        	
        	context.write(new Text(name), new LongWritable(count)); 
        }
        
   
         
    } 
} 