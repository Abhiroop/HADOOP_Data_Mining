
import java.io.IOException;

import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;

public class Dectree{
	
																	/*CLASSMAP1*/
																	
	public static class Classmap1 extends MapReduceBase implements Mapper<LongWritable,Text, Text,Text>
		{
			public void map(LongWritable Key, Text value,OutputCollector<Text, Text> oc,Reporter rep)throws IOException
			{
				StringTokenizer itr=new StringTokenizer(value.toString());
				String classval="";
				while(itr.hasMoreTokens())
				{
					classval=itr.nextToken();
				}
				oc.collect(new Text(classval),new Text("1"));							/*			YES (CLASS)		1(COUNT)			*/
				
			}
		} 
		
																	/*CLASSRED1*/
																	
		public static class Classred1 extends MapReduceBase implements Reducer<Text, Text,Text,Text>
		{
			public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text,Text>oc,Reporter rep)throws IOException
			{
				int c=0;
				while(values.hasNext())
				{
					c++;
					values.next();
				}
				oc.collect(new Text("1"),new Text(c+""));								/*			1 	4 (NO. OF RECORDS OF CLASS X)			*/
			}
		}
		
					
																	/*CLASSMAP2*/
		public static class Classmap2 extends MapReduceBase implements Mapper<LongWritable,Text, Text,Text>
		{
			public void map(LongWritable Key, Text value,OutputCollector<Text, Text> oc,Reporter rep)throws IOException
			{
				StringTokenizer itr=new StringTokenizer(value.toString());
				
				String k=itr.nextToken();
				String val=itr.nextToken();
				oc.collect(new Text(k),new Text(val));
				
			}
		} 
		
		
																	/*CLASSRED2*/
		public static class Classred2 extends MapReduceBase implements Reducer<Text, Text,Text,Text>
		{
			public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text,Text>oc,Reporter rep)throws IOException
			{
				float entropy=0,pval,total=0;
				ArrayList<Float> arr=new ArrayList<Float>();
				while(values.hasNext())
				{
					pval=Float.parseFloat(values.next().toString());
					arr.add(pval);
					total+=pval;
				}
				int c=0;
				float term;
				while(c<arr.size())
				{
					term=arr.get(c)/total;
					entropy-=term*Math.log(term);
					c++;
				}
				oc.collect(new Text(entropy+""),new Text(" "));							/*			(ENTROPY OF TOTAL DATASET) 	(" ")			*/
			}
		}
		
		
																	/*PROGMAP*/
																	
	public static class progmap extends MapReduceBase implements Mapper<LongWritable,Text, Text,Text>
		{
			private String attri;
			public void configure(JobConf job) {
			attri = job.get("attrno");
			}
			public void map(LongWritable Key, Text value,OutputCollector<Text, Text> oc,Reporter rep)throws IOException
			{
				int arg=Integer.parseInt(attri);
				StringTokenizer itr=new StringTokenizer(value.toString());
				String token="";
				while(arg>=1)
				{
					arg--;
					token=itr.nextToken();
				}
				String classval="";
				while(itr.hasMoreTokens())
				{
					classval=itr.nextToken();
				}
				oc.collect(new Text(token),new Text(classval));									/*			attribute value 	class label			*/
				
			}
		} 
		
		
																	/*PROGRED*/
																	
		public static class progred extends MapReduceBase implements Reducer<Text, Text,Text,Text>
		{
			private String attri;
			public void configure(JobConf job) {
			attri = job.get("attrno");
			}
			public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text,Text>oc,Reporter rep)throws IOException
			{
				HashMap m = new HashMap();
				String st="";
				int no_rec=0;
				while(values.hasNext())
				{
					no_rec++;
					st=values.next().toString();
					if(!m.containsKey(st))
						m.put(st,1);
					else
					m.put(st,(Integer)m.get(st)+1);
					
				}
				float delta=0,p;
				Iterator it=m.keySet().iterator();
				while(it.hasNext())
				{
					int temp = (Integer)m.get(it.next());
					p=(float)temp/(float)no_rec;
					if(p!=0)
					{
						delta-=p*Math.log(p);
					}
				}
				
				oc.collect(new Text(attri),new Text(delta+":"+no_rec));	
				
				/*	(Attribute 'index' for that value) 	(corr. entropy:N(no. of records)  )		*/
			}
		}
		
		
																	/*PROGMAP2*/
																	
		public static class progmap2 extends MapReduceBase implements Mapper<LongWritable,Text, Text,Text>
		{
			
			public void map(LongWritable Key, Text value,OutputCollector<Text, Text> oc,Reporter rep)throws IOException
			{
				
				StringTokenizer itr=new StringTokenizer(value.toString());
				String k=itr.nextToken();
				String val=itr.nextToken();
				oc.collect(new Text(k),new Text(val));							/*			(Attribute index) 	(Entropy value:N)			*/
				
			}
		} 
		
		
																	/*PROGRED2*/
																	
		public static class progred2 extends MapReduceBase implements Reducer<Text, Text,Text,Text>
		{
			private float info=0;
			private String entotal;
			public void configure(JobConf job) {
			entotal = job.get("attrno");
			}
			public void reduce(Text key, Iterator<Text> values,OutputCollector<Text,Text>oc,Reporter rep)throws IOException
			{
				float total_rec=0;
				while(values.hasNext())
				{
					String st=values.next().toString();
					StringTokenizer itr=new StringTokenizer(st,":");
					float p1=Float.parseFloat(itr.nextToken());
					float p2=Float.parseFloat(itr.nextToken());	
					info+=p1*p2;
					total_rec+=p2;		
				}
				
				info=info/total_rec;
				info=Float.parseFloat(entotal)-info;
				oc.collect(new Text(key),new Text(info+""));					/*		attribute index		corresponding net Info Gain 		*/
			}
		}
		
		
																	/*PROGMAP3*/
																	
		public static class progmap3 extends MapReduceBase implements Mapper<LongWritable,Text, Text,Text>
		{
			
			public void map(LongWritable Key, Text value,OutputCollector<Text, Text> oc,Reporter rep)throws IOException
			{
				StringTokenizer itr=new StringTokenizer(value.toString());
				String k=itr.nextToken();
				Float val=Float.parseFloat(itr.nextToken());	/* 	Info gain	*/
				oc.collect(new Text("value"),new Text(val+":"+k));						/*	"value"	(common key for all)	Info gain:attribute index */
			}
		} 
		
		
																	/*PROGRED3*/
																	
		public static class progred3 extends MapReduceBase implements Reducer<Text, Text,Text,Text>
		{
			
			private float max=0;
			public void reduce(Text key, Iterator<Text> values,OutputCollector<Text,Text>oc,Reporter rep)throws IOException
			{
				
				/*				each value contains an amalgamation of attribute index and its info gain		*/
				String maxkey="dummy";
				
				// iterating through all the values
				while(values.hasNext())
				{
					
					String st=values.next().toString();
					StringTokenizer temp=new StringTokenizer(st,":");						//separating the parts of value string
						
					float intval=Float.parseFloat(temp.nextToken());						/*	taking the info gain out of the string value		*/
					if(max<intval)															/* finding the maximum info gain						*/
					{
						max=intval;
						
						maxkey=temp.nextToken();											/*	taking the attribute index out of string value	*/
					}		
				}
				oc.collect(new Text(maxkey),new Text(max+""));									/*	attribute index		Info Gain						*/
			}
		}
		
		
																	/*PROGMAP4*/
																	
		public static class progmap4 extends MapReduceBase implements Mapper<LongWritable,Text, Text,Text>
		{
			private String attri;
			public void configure(JobConf job) {
			attri = job.get("attrno");
			}
			public void map(LongWritable Key, Text value,OutputCollector<Text, Text> oc,Reporter rep)throws IOException
			{
				int arg=Integer.parseInt(attri);
				StringTokenizer itr=new StringTokenizer(value.toString());
				String token="";
				String extract="";
				int count=1;
				
				//extracting that particular attribute value
				while(itr.hasMoreTokens())
				{
					if(count==arg)
					{	
						token=itr.nextToken();
						break;
					}
					else
					{
						token=itr.nextToken();
						extract+=token+" ";
						count++;
					}
				}
				while(itr.hasMoreTokens())
				{
						extract+=itr.nextToken()+" ";
				}
				
				oc.collect(new Text(token),new Text(extract));		/* (attribute value)	(remaining line after removing that attribute column)	*/
				
			}
		} 
		
		
																	/*PROGRED4*/
																	
		public static class progred4 extends MapReduceBase implements Reducer<Text, Text,Text,Text>
		{
			
			public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text,Text>oc,Reporter rep)throws IOException
			{
				
				while(values.hasNext())
				{					
					oc.collect(key,values.next());					/* 	(attribute value)		(extract line after removing the column) */
				}
			}
		}
		
		 static class MultiFileOutput extends MultipleTextOutputFormat<Text, Text> {
			 
                 protected String generateFileNameForKeyValue(Text key, Text value, String leaf) {
					
					 
						return new Path(key+"/input/"+leaf).toString();
			}
		
		protected Text generateActualKey(Text key, Text value) {
			return null;
		}
        }
        public static void main(String [] args)throws Exception{
			
			splitRec("/");
		}
		public static void splitRec(String basedir)throws Exception{
			
			JobConf conf01=new JobConf(Dectree.class);
						
			DFSClient dfs=new DFSClient(conf01);
			DFSClient.DFSInputStream in=dfs.open(basedir+"input/part-00000");
			DFSClient.DFSDataInputStream din=new DFSClient.DFSDataInputStream(in);
			String val=din.readLine();
			StringTokenizer attr=new StringTokenizer(val);
			int count=attr.countTokens();
			
			if(count==1) return;												/* no attribute left to split, so end further processing */
			
			conf01.setOutputKeyClass(Text.class);
			conf01.setOutputValueClass(Text.class);
			conf01.setMapperClass(Classmap1.class);
			conf01.setReducerClass(Classred1.class);
			FileInputFormat.addInputPath(conf01, new Path(basedir+"input/"));
			FileOutputFormat.setOutputPath(conf01, new Path(basedir+"Classoutput1/"));
			JobClient.runJob(conf01);
			
			
			
			JobConf conf02=new JobConf(Dectree.class);
			
			conf02.setOutputKeyClass(Text.class);
			conf02.setOutputValueClass(Text.class);
			conf02.setMapperClass(Classmap2.class);
			conf02.setReducerClass(Classred2.class);
			FileInputFormat.addInputPath(conf02, new Path(basedir+"Classoutput1/"));
			FileOutputFormat.setOutputPath(conf02, new Path(basedir+"Classoutput2/"));
			JobClient.runJob(conf02);
			
			
			dfs=new DFSClient(conf01);
			in=dfs.open(basedir+"Classoutput2/part-00000");
			din=new DFSClient.DFSDataInputStream(in);
			val=din.readLine();
			float entropytree=Float.parseFloat(val);
			
			if(entropytree==0.0) return;															// The tree is pure, so end further spliting 
			
			
			JobConf conf=new JobConf(Dectree.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(progmap.class);
			conf.setReducerClass(progred.class);
			FileInputFormat.addInputPath(conf, new Path(basedir+"input/"));
			
			/*	 					Running for all attributes					*/
			while(--count>=1)
			{		
				FileOutputFormat.setOutputPath(conf, new Path(basedir+"output"+count));			//set the output directory for every attribute
				conf.set("attrno",""+count);													//set the attribute number
				conf.setJobName("ArticleCounter"+count);										//set the Job name
				JobClient.runJob(conf);	
			}
			
			
			
			/* 						Running the second job							*/
			
			JobConf conf2=new JobConf(Dectree.class);

			/*					Setting the attributes of conf2 object				*/
			
			conf2.setOutputKeyClass(Text.class);
			conf2.setOutputValueClass(Text.class);
			conf2.setMapperClass(progmap2.class);
			conf2.setReducerClass(progred2.class);
			
			count=attr.countTokens();												//count has become zero after above while block
			while(--count>=1)
			{
				FileInputFormat.addInputPath(conf2, new Path(basedir+"output"+count));
			}
			
			FileOutputFormat.setOutputPath(conf2, new Path(basedir+"2output"));
			conf2.setJobName("EntropyCounter");
			conf2.set("attrno",entropytree+"");
			JobClient.runJob(conf2);
			
			
			
			/* 						Running the third job							 */
			
			JobConf conf3=new JobConf(Dectree.class);

			/*					Setting the attributes of conf3 object				 */
			
			conf3.setOutputKeyClass(Text.class);
			conf3.setOutputValueClass(Text.class);
			conf3.setMapperClass(progmap3.class);
			conf3.setReducerClass(progred3.class);
			FileInputFormat.addInputPath(conf3, new Path(basedir+"2output"));
			FileOutputFormat.setOutputPath(conf3, new Path(basedir+"3output"));
			conf3.setJobName("MaxEntropyFinder");
			JobClient.runJob(conf3);
			
			dfs=new DFSClient(conf01);
			in=dfs.open(basedir+"3output/part-00000");
			din=new DFSClient.DFSDataInputStream(in);
			val=din.readLine();
			attr=new StringTokenizer(val);
			
			if(attr.nextToken().equals("dummy"))	return;
			
			
			JobConf conf4=new JobConf(Dectree.class);

			/*					Setting the attributes of conf4 object				 */
			
			conf4.setOutputKeyClass(Text.class);
			conf4.setOutputValueClass(Text.class);
			conf4.setMapperClass(progmap4.class);
			conf4.setReducerClass(progred4.class);
			conf4.setOutputFormat(MultiFileOutput.class);
			
			dfs=new DFSClient(conf);
			in=dfs.open(basedir+"3output/part-00000");
			din=new DFSClient.DFSDataInputStream(in);
			val=din.readLine();
			
			conf4.set("attrno",val.charAt(0)+"");
			
			FileInputFormat.addInputPath(conf4, new Path(basedir+"input/"));
			FileOutputFormat.setOutputPath(conf4, new Path(basedir+"split1"));
			conf4.setJobName("DataSplitter");
			JobClient.runJob(conf4);

			/*								Recursion 								*/
			FileSystem fs = FileSystem.get(conf4);
			FileStatus[] status= fs.listStatus(new Path(basedir+"split1/"));
			String recpath;
			
			for(int i=0;i<status.length;i++)
			{
				recpath=status[i].getPath().toString();
				recpath=recpath.replace("hdfs://master:54310","");
				
				if(recpath.charAt(basedir.length()+7)!='_')
				{
					splitRec(recpath+"/");
				}
			}
		
		}
}