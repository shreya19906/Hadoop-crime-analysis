package CrimeData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimesAnalysis {
	public static void main(String[] args) throws Exception {
		
		
		/*For both HDFS and MapReduce the reason you use the configuration object 
		 * is because there are basic parameters that need to be set prior to using 
		 * the libraries.  In the case of HDFS you must set which file system you want 
		 * to use and the client code must know where the name node is.  Same thing 
		 * with MapReduce code, at a minimum it needs to know where the daemons are 
		 * running.  Given you can just instantiate a Configuration object without any 
		 * parameters (and thus use the defaults), this is not much of a burden and very common in API design. */
		
		
		Configuration config = new Configuration();
		//
		// if (args.length != 4) {
		// System.out
		// .printf("Usage: CrimesAnalysis <opreration opt> <input dir1> <input dir2> <output dir> \n");
		// System.exit(-1);
		// }
		/*
		 * opt: 0: join table 
		 *      1: year analysis 
		 *      2: time slot analysis 
		 *      3: type analysis
		 *      4: income analysis
		 * 
		 * opt = 0, Usage: CrimesAnalysis <opreration opt> <input dir1> <input
		 * dir2> <output dir> \n"); opt in (1,2,3,4)Usage: CrimesAnalysis
		 * <opreration opt> <input dir> <output dir> \n");
		 */
		Job job = new Job();
		job.setJarByClass(CrimesAnalysis.class);
		job.setJobName("CrimesAnalysis");
		int opt = Integer.parseInt(args[1]);

		/*Hadoop’s org.apache.hadoop.fs.FileSystem is generic class to 
		 * access and manage HDFS files/directories located in distributed environment.
		 * FileSystem read and stream by accessing blocks in sequence order. FileSystem 
		 * first get blocks information from NameNode then open, read and close one by one. 
		 * It opens first blocks once it complete then close and open next block.
		 *  HDFS replicate the block to give higher reliability and scalability and if 
		 *  client is one of the datanode then it tries to access block locally if fail 
		 *  then move to other cluster datanode.FileSystem uses FSDataOutputStream and 
		 *  FSDataInputStream to write and read the contents in stream */
		
		
		FileSystem fs = FileSystem.get(config);
		Path outpath = null;

		if (opt == 0) {
			MultipleInputs.addInputPath(job, new Path(args[2]),
					TextInputFormat.class, CrimesJoinMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[3]),
					TextInputFormat.class, CommunityJoinMapper.class);

			outpath = new Path(args[4]);
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}

			FileOutputFormat.setOutputPath(job, outpath);
			job.setReducerClass(CrimesJoinReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);//
			job.setOutputValueClass(Text.class);
		} else {
			outpath = new Path(args[3]);
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, new Path(args[3]));
			FileInputFormat.setInputPaths(job, new Path(args[2]));

			job.setReducerClass(ShareCrimeReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			switch (opt) {
			case 1: {// crimes year distribution

				job.setMapperClass(YearCrimeMapper.class);
				break;
			}

			case 2: {//time slot distribution
				job.setMapperClass(TimeslotCrimeMapper.class);
				break;
			}

			case 3: {//crimes type distribution 
				job.setMapperClass(TypeofCrimeMapper.class);
				break;
			}

			case 4: {//income crime relations
				job.setMapperClass(IncomeCrimeMapper.class);
				break;
			}
			case 5: {//unemployed crime relations
				job.setMapperClass(UnemployMapper.class);
				break;
			}
			case 6: {//diploma crime relations
				job.setMapperClass(DiplomaMapper.class);
				break;
			}

			default: {
				break;
			}

			}
		}

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
