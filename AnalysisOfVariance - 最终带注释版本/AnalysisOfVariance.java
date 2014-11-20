import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;   
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.mapred.lib.ChainMapper;    //在0.20.2中ChainMapper和ChainReducer类在哪个包中
//import org.apache.hadoop.mapred.lib.ChainReducer;



/**
 * 
 * @author peterpan
 * 输入命令格式例子：hjar AnalysisOfVariance_fat.jar psq/input psq/output1 psq/output2 psq/output3
 * 注：本程序中输入目录中只有一个数据文件，就是统计数据的表
 *    
 */

public class AnalysisOfVariance     //输入的是矩阵数据
{	
	public static class Map1 extends Mapper<LongWritable, Text, IntWritable, FloatWritable>  
    {	  	
    	public void map(LongWritable key, Text value, Context context)
    	throws IOException, InterruptedException   //输入为txt文件，输入后自动将每一行作为map输入的一个键值对，
    	{                                          //键是该行起始位置相对于文件起始位置的偏移量，这里为无用信息   
    		/*
    		 * map()输入说明：
    		 *     map()输入为数据文件，格式为.txt，且为单数据文件
    		 * map()输出说明：
    		 *     key为正数：key表示列的序号，即水平；value为每列下的数据结合
    		 *     key==0：    value为所有数据的集合
    		 *     key==-1: value为所有行每一行的平均值集合
    		 *     key==-2: value为数据的列数，即水平数，因为写入了很多遍，所以值读取value的第一个值就可以了 
    		 * */
    		
    		int level = 0;
    		float element = 0;
    		float rowaverage = 0;
    		float rowsum = 0;
    		String line = value.toString();
    		StringTokenizer strtok = new StringTokenizer(line);//StringTokenizer默认分隔符为空格
    	
    		
    		while(strtok.hasMoreTokens()) //每个map函数的输出key是水平类型，value是每个水平的每个元素的集合
    	    {
    	    	element = Float.parseFloat(strtok.nextToken());
    	    	
    	    	
    	    	level+=1;
    	    	context.write(new IntWritable(level), new FloatWritable(element));  //用于计算每个的平均值
    	        context.write(new IntWritable(0), new FloatWritable(element));    //key==0的value的值为所有数据集合
    	    	//以下求每行的平均值，最后所有行的平均值都输出在key=0的values中
    	    	rowsum += element;
    	    	
    	    	System.out.println("element:"+element);
    	    	
    	    }
            rowaverage = rowsum/level;
            context.write(new IntWritable(-1), new FloatWritable(rowaverage));//key==-1的values为每一行的平均值集合
            context.write(new IntWritable(-2), new FloatWritable((float)level));//key==-2的value为数据列数,因为写入reduce中很多遍，所以只要读取values中的第一个值就可以了
            
            System.out.println("level:"+level);
            System.out.println("rowaverage:"+rowaverage);
    	}
    }
    
    public static class Reduce1 extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>    //该reduce函数就是求各个水平以及整体的均值，之后的处理，交由下一组map和reduce进行
    {
    	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
    	throws IOException, InterruptedException  //可以把不同map输出的键值对的值进行运算吗，应该可以吧？
    	{
    		/*
    		 * reduce()输入说明：
    		 *     key为正数：key表示列的序号，即水平；value为每列下的数据结合
    		 *     key==0：    value为所有数据的集合
    		 *     key==-1: value为所有行每一行的平均值集合
    		 *     key==-2: value为数据的列数，及水平数，因为写入了很多遍，所以值读取value的第一个值就可以了
    		 * reduce()输出说明：
    		 *     key为正数：key表示列的序号，即水平；value为该列的平均值
    		 *     key==0:  value为所有数据的平均值
    		 *     key==-1: value为所有行平均值的集合
    		 *     key==-2: value为所有数据的个数
    		 *     key==-3: value为数据的行数
    		 *     key==-4: value为数据的列数    
    		 * */
    		
    		float sum = 0;       //values中所有数据的和,即每列的和
    		float sum_alldata = 0;    //所有数据的和
    		float average = 0;   //values中所有数据的平均值
    		float average_alldata = 0; //所有数据的平均值
    		int num = 0;       //values中的数据的个数
    		int rownum_alldata = 0;    //所有数据的行数
    		int allnum_alldata = 0;    //所有数据的个数
    	    float tempvalue; 
    	    
    	    System.out.println("key:"+key);
    	    
    	    if(key.equals(new IntWritable(0)))    //key==0的输入是values为所有数据集合
    	    {         //Writable 类型的变量不能直接比较！！！！
    	    	System.out.println("key == 0 da yin ma!!!!!!");
    	    	for(FloatWritable valuealldata : values)
    	    	{
    	    		sum_alldata += valuealldata.get();
    	    		allnum_alldata++;
    	    	}
    	    	average_alldata = sum_alldata/allnum_alldata;
    	    	context.write(new IntWritable(-2), new FloatWritable((float)allnum_alldata));
    	    	context.write(new IntWritable(0), new FloatWritable(average_alldata));
    	    	
    	    	System.out.println("allnum_alldata:"+allnum_alldata);
    	    	System.out.println("average_alldata"+average_alldata);
    	    	
    	    }
    	    else if(key.equals(new IntWritable(-1)))   //key==-1的values为每一行的平均值集合
    	    {
    	    	System.out.println("key == -1 da yin ma!!!!!!");
    	    	for(FloatWritable valuerowaverage : values)
    	    	{
    	    		rownum_alldata++;
    	    		context.write(new IntWritable(-1), valuerowaverage);
    	    		
    	    		System.out.println("valuerowaverage:"+valuerowaverage.get());
    	    	}
    	    	
    	    	System.out.println("rownum_alldata:"+rownum_alldata);
    	    	
    	        context.write(new IntWritable(-3), new FloatWritable((float)rownum_alldata));
    	        
    	        System.out.println("rownum_alldata:"+rownum_alldata);
    	    }
    	    else if(key.equals(new IntWritable(-2)))    //key==-2的value为数据列数,因为写入reduce中很多遍，所以只要读取values中的第一个值就可以了
    	    {
    	    	System.out.println("key == -2 da yin ma!!!!!!");
    	    	int flagline = 0;
    	    	for(FloatWritable valueline : values)//key==-2的value为数据列数，只有一个值
    	    	{
    	    		if(flagline == 0)
    	    		    context.write(new IntWritable(-4), valueline);
    	    		flagline = 1;
    	    		System.out.println("valueline:"+valueline.get());
    	    	}
    	    	
    	    }
    	    else     //对每一列数据求平均值
    	    {
    	    	System.out.println("key == else da yin ma!!!!!!");
    	    	for(FloatWritable value : values)
    		    {
    			    tempvalue = value.get();
    			    sum += tempvalue;
    			    num++;
    		    }
    		    average = sum / num;  
    		        context.write(key, new FloatWritable(average));	
    	    }
    	}
    }          
    
    public static class Map2 extends Mapper<LongWritable, Text, IntWritable, FloatWritable>  
    {	  	
    	public void map(LongWritable key, Text value, Context context)//map2仍然以初始数据文件未输入文件，就是为了得到各个数据
    	throws IOException, InterruptedException   //输入为txt文件，输入后自动将每一行作为map输入的一个键值对，
    	{                
    		//键是该行起始位置相对于文件起始位置的偏移量，这里为无用信息   
    		float element2,temp;
    		float allaveragemap2;  //用于存放从main()函数传入的总数据平均值
    		
            Configuration confmap2 = context.getConfiguration(); //调用main()函数传过来的数据
    		allaveragemap2 = confmap2.getFloat("dataallaverage",-1); 
    		
    		System.out.println("allaveragemap2:"+allaveragemap2);
    		
    		String line2 = value.toString();
    		StringTokenizer strtok = new StringTokenizer(line2);//StringTokenizer默认分隔符为空格   		
    		
    		while(strtok.hasMoreTokens()) 
    	    {
    	    	element2 = Float.parseFloat(strtok.nextToken());
    	    	
    	    	System.out.println("element2:"+element2);
    	    	
    	    	temp = (element2-allaveragemap2)*(element2-allaveragemap2);
    	    	System.out.println("temp:"+temp);
    	    	context.write(new IntWritable(-2), new FloatWritable(temp)); //所有value的key相同，用于最后reduce的求和计算 
    	    }
    	}
    }
    
    public static class Reduce2 extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>    //该reduce求SST，只设置了一个reduce任务
    {
    	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
    	throws IOException, InterruptedException  //可以把不同map输出的键值对的值进行运算吗，应该可以吧？
    	{
    		float SST = 0;      //总离差平方和   key=-2
    		
    		for(FloatWritable value : values)
    		{
    			SST += value.get();
    		}
    		
    		context.write(new IntWritable(-2), new FloatWritable(SST));
    	}
    }         
    
    
    public static class Map3 extends Mapper<LongWritable, Text, IntWritable, FloatWritable>  
    {	  	
    	public void map(LongWritable key, Text value, Context context)//map3以job1的输出文件作为输入，是为了得到每个水平（每一列）的平均值
    	throws IOException, InterruptedException   //输入为txt文件，输入后自动将每一行作为map输入的一个键值对，
    	{                                          //键是该行起始位置相对于文件起始位置的偏移量，这里为无用信息   
    		int tempkey;   //因为tempkey在if语句中，所以如果if语句不成立，特姆坡可以可能不会被使用，因此报了警告
    		float tempvalue, temp3;
    	    int rownummap3;    //用于存放从main()函数中传入的数据行数
    	    float allaveragemap3; //用于存放从main()函数中传入的总数据平均值
    	    int flag = 0;
    	    
    	    String line3 = value.toString();   //每一行数据就是形如：“1 3.1”, 即一个key值，一个value值
    	    StringTokenizer strtok2 = new StringTokenizer(line3);
    	    
    	    Configuration confmap3 = context.getConfiguration();  //调用从main()函数冲传入的数据
    	    rownummap3 = confmap3.getInt("datarownum", -1);
    	    allaveragemap3 = confmap3.getFloat("dataallaverage", -2);
    	    
    	    System.out.println("rownummap3:"+rownummap3);
    	    System.out.println("allaveragemap3:"+allaveragemap3);
    	    
    	    while(strtok2.hasMoreTokens() && flag == 0)
    	    {
    	        tempkey = Integer.parseInt(strtok2.nextToken());  //这个读取的是key,我们想要要的是value
    	        
    	        System.out.println("tempkey:"+tempkey);
                if(tempkey > 0)
                {
    	            while(strtok2.hasMoreTokens()) 
    	            {
    	                tempvalue = Float.parseFloat(strtok2.nextToken());
    	                
    	                System.out.println("tempvalue:"+tempvalue);
    	                
    	                temp3 = rownummap3*(tempvalue-allaveragemap3)*(tempvalue-allaveragemap3);
    	                System.out.println("temp3:"+temp3);
    	                context.write(new IntWritable(-4), new FloatWritable(temp3));
    	            }
                }
                else
                {
                	flag = 1;
                }
    	    }
    	}
    }
    
    public static class Reduce3 extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>    //该reduce求SSA，只设置了一个reduce任务
    {
    	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
    	throws IOException, InterruptedException  //可以把不同map输出的键值对的值进行运算吗，应该可以吧？
    	{
    		float SSA = 0;   //水平项离差平方和（组间）key=-4.因为SSE不好划分任务，因此选择求SST和SSA，然后利用SSE=SST-SSA来求SSE   		
    		
    		for(FloatWritable value : values)
    		{
    			System.out.println("value:"+value.get());
    			SSA += value.get();
    		}

    		context.write(new IntWritable(-4), new FloatWritable(SSA));
    	}
    }   
    
    public static class Map4 extends Mapper<LongWritable, Text, IntWritable, FloatWritable>  
    {	  	
    	public void map(LongWritable key, Text value, Context context)//map4以job1的输出文件作为输入，是为了得到每行的平均值
    	throws IOException, InterruptedException   //输入为txt文件，输入后自动将每一行作为map输入的一个键值对，
    	{                                          //键是该行起始位置相对于文件起始位置的偏移量，这里为无用信息   
    		
    		int tempkey; //因为tempkey在if语句中，所以如果if语句不成立，特姆坡可以可能不会被使用，因此报了警告
    		float tempvalue, temp4;
    		int linenummap4;    //用于存放从main()函数中传入的数据行数
    		float allaveragemap4; //用于存放从main()函数中传入的总数据平均值
    	    int flag = 0;
    	    
    	    String line4 = value.toString();   //每一行数据就是形如：“1 3.1”, 即一个key值，一个value值
    	    StringTokenizer strtok4 = new StringTokenizer(line4);
    	    
    	    Configuration confmap4 = context.getConfiguration();  //调用从main()函数冲传入的数据
    	    linenummap4 = confmap4.getInt("datalinenum", -1);
    	    allaveragemap4 = confmap4.getFloat("dataallaverage", -2);
    	    
    	    System.out.println("linenummap4:"+linenummap4);
    	    System.out.println("allaveragemap4:"+allaveragemap4);
    	    
    	    while(strtok4.hasMoreTokens() && flag == 0)
    	    {
    	    	tempkey = Integer.parseInt(strtok4.nextToken());  //这个读取的是key,我们想要要的是value
    	    
    	    	System.out.println("tempkey:"+tempkey);
    	    	
    	    	if(tempkey == -1)
                {
    	            while(strtok4.hasMoreTokens()) 
    	            {
    	                tempvalue = Float.parseFloat(strtok4.nextToken());
    	                
    	                System.out.println("tempvalue:"+tempvalue);
    	                
    	                temp4 = linenummap4*(tempvalue-allaveragemap4)*(tempvalue-allaveragemap4);
    	                System.out.println("temp4"+temp4);
    	                context.write(new IntWritable(-5), new FloatWritable(temp4));
    	            }
                }
                else
                {
                	flag = 1;
                }	    	
    	    }
    	}
    }
    
    public static class Reduce4 extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>    //该reduce求SSA，只设置了一个reduce任务
    {
    	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
    	throws IOException, InterruptedException  //可以把不同map输出的键值对的值进行运算吗，应该可以吧？
    	{
    		float SSR = 0;   //行因素误差平方和,key=-5.因为SSE不好划分任务，因此选择求SST和SSC（即SSA），然后利用SSE=SST-SSC-SSR来求SSE   		
    		
    		for(FloatWritable value : values)
    		{
    			System.out.println("value:"+value.get());
    			SSR += value.get();
    		}

    		context.write(new IntWritable(-5), new FloatWritable(SSR));
    	}
    }   
    
    
    public static void finaljudge(float SSA, float SSE, float SSR, int linenum, int rownum, int allnum)    //进行最后的F临界值验证函数,入口参数为F分布表在hdfs上的路径
    {
    	try
    	{
    	    String line;
    	    float listdata;
    	    int frow, fline;
    	    
    	    float[][] flist = new float[122][122]; //存放α=0.05时的F表，数据严格按照F表坐标进行存放，无穷大的数据存放在flist[121][j]和flist[i][121]出
    	    BufferedReader readStream = new BufferedReader(new FileReader("/home/tseg/psq/flist.txt"));
    	    
    	    
    	    
    	    for(frow=0; frow<122; frow++)
    	    	for(fline=0; fline<122; fline++)
    	    	    flist[frow][fline] = 0;
    	    
    	    frow = 0;
    	    fline = 0;
    	    	    
    	    while((line=readStream.readLine()) != null)  //我写的表是部分F分布表，不是完整的
    	    {
    	    	frow++;
    	    	StringTokenizer  strlist = new StringTokenizer(line); //取一行数据，默认空格为分隔符
    	    	while(strlist.hasMoreTokens())
    	    	{
    	    	    fline++;
    	    		listdata = Float.parseFloat(strlist.nextToken());
    	    		
    	    		//System.out.print(listdata+" ");
    	    		
    	    		if(frow < 22)
    	    		{
    	    		    if(fline<11)
    	    			    flist[frow+9][fline] = listdata;
    	    		    else if(fline == 11)
    	    		        flist[frow+9][12] = listdata;
    	    		    else if(fline == 12)
    	    		    	flist[frow+9][15] = listdata;
    	    		    else if(fline == 13)
    	    		    	flist[frow+9][20] = listdata;
    	    		    else if(fline == 14)
    	    		    	flist[frow+9][24] = listdata;
    	    		    else if(fline == 15)
    	    		    	flist[frow+9][30] = listdata;
    	    		    else if(fline == 16)
    	    		    	flist[frow+9][40] = listdata;
    	    		    else if(fline == 17)
    	    		    	flist[frow+9][60] = listdata;
    	    		    else if(fline == 18)
    	    		    	flist[frow+9][120] = listdata;
    	    		    else if(fline == 19)
    	    		    	flist[frow+9][121] = listdata;
    	    		}
    	    		else if (frow == 22)
    	    		{
    	    			if(fline<11)
    	    			    flist[40][fline] = listdata;
    	    		    else if(fline == 11)
    	    		        flist[40][12] = listdata;
    	    		    else if(fline == 12)
    	    		    	flist[40][15] = listdata;
    	    		    else if(fline == 13)
    	    		    	flist[40][20] = listdata;
    	    		    else if(fline == 14)
    	    		    	flist[40][24] = listdata;
    	    		    else if(fline == 15)
    	    		    	flist[40][30] = listdata;
    	    		    else if(fline == 16)
    	    		    	flist[40][40] = listdata;
    	    		    else if(fline == 17)
    	    		    	flist[40][60] = listdata;
    	    		    else if(fline == 18)
    	    		    	flist[40][120] = listdata;
    	    		    else if(fline == 19)
    	    		    	flist[40][121] = listdata;	
    	    		}
    	    		else if (frow == 23)
    	    		{
    	    			if(fline<11)
    	    			    flist[60][fline] = listdata;
    	    		    else if(fline == 11)
    	    		        flist[60][12] = listdata;
    	    		    else if(fline == 12)
    	    		    	flist[60][15] = listdata;
    	    		    else if(fline == 13)
    	    		    	flist[60][20] = listdata;
    	    		    else if(fline == 14)
    	    		    	flist[60][24] = listdata;
    	    		    else if(fline == 15)
    	    		    	flist[60][30] = listdata;
    	    		    else if(fline == 16)
    	    		    	flist[60][40] = listdata;
    	    		    else if(fline == 17)
    	    		    	flist[60][60] = listdata;
    	    		    else if(fline == 18)
    	    		    	flist[60][120] = listdata;
    	    		    else if(fline == 19)
    	    		    	flist[60][121] = listdata;	
    	    		}
    	    		else if (frow == 24)
    	    		{
    	    			if(fline<11)
    	    			    flist[120][fline] = listdata;
    	    		    else if(fline == 11)
    	    		        flist[120][12] = listdata;
    	    		    else if(fline == 12)
    	    		    	flist[120][15] = listdata;
    	    		    else if(fline == 13)
    	    		    	flist[120][20] = listdata;
    	    		    else if(fline == 14)
    	    		    	flist[120][24] = listdata;
    	    		    else if(fline == 15)
    	    		    	flist[120][30] = listdata;
    	    		    else if(fline == 16)
    	    		    	flist[120][40] = listdata;
    	    		    else if(fline == 17)
    	    		    	flist[120][60] = listdata;
    	    		    else if(fline == 18)
    	    		    	flist[120][120] = listdata;
    	    		    else if(fline == 19)
    	    		    	flist[120][121] = listdata;	
    	    		}
    	    		else if (frow == 25)
    	    		{
    	    			if(fline<11)
    	    			    flist[121][fline] = listdata;
    	    		    else if(fline == 11)
    	    		        flist[121][12] = listdata;
    	    		    else if(fline == 12)
    	    		    	flist[121][15] = listdata;
    	    		    else if(fline == 13)
    	    		    	flist[121][20] = listdata;
    	    		    else if(fline == 14)
    	    		    	flist[121][24] = listdata;
    	    		    else if(fline == 15)
    	    		    	flist[121][30] = listdata;
    	    		    else if(fline == 16)
    	    		    	flist[121][40] = listdata;
    	    		    else if(fline == 17)
    	    		    	flist[121][60] = listdata;
    	    		    else if(fline == 18)
    	    		    	flist[121][120] = listdata;
    	    		    else if(fline == 19)
    	    		    	flist[121][121] = listdata;	
    	    		}	    		
    	    	}
    	    	fline = 0;
    	    }
    	    /*		
    	    int tempflag = 0;
    	    System.out.println("flist:");
    	    for(frow=0; frow<122; frow++)
    	    {
    	    	for(fline=0; fline<122; fline++)
    	    	{
    	    		if(flist[frow][fline] != 0)
    	    		{
    	    			System.out.print(flist[frow][fline]+" ");
    	                tempflag = 1;		
     	    		}
    	    	}
    	    	if(tempflag == 1)
    	    	    System.out.print("\n");
    	    	tempflag = 0;
    	    }
    	    */
    	    
    	    if(SSR == -1 && rownum ==-1)   //即为单因素方差分析
    	    {
    	    	int df1, df2;      //df1为第一自由度， df2为第二自由度
    	    	float MSA, MSE, F;   //MSA为组间均方，MSE为组内均方,F为F值
    	    	df1 = linenum - 1;
    	    	df2 = allnum - linenum;
    	    	MSA = SSA/df1;
    	    	MSE = SSE/df2;
    	    	F = MSA/MSE;
    	    
    	    	if(F < flist[df2][df1])
    	    		System.out.println("The hypothesis H0 proved to be true: the factor has a significant impact on the statistical data");
    	    	else
    	    		System.out.println("The hypothesis H0 proved to be wrong: the factor has no significant impact on the statistical data");
    	    }
    	    else   //即为双因素方差分析
    	    {
    	    	int dfline, dfrow, df;   
    	    	float MSC, MSR, MSE, Fr, Fc;
    	    	
    	    	dfline = linenum-1; 
    	    	dfrow = rownum-1;
    	    	df = (linenum-1)*(rownum-1);
    	    	MSC = SSA/dfline;
    	    	MSR = SSR/dfrow;
    	    	MSE = SSE/df;
    	    	Fr = MSR/MSE;
    	    	Fc = MSC/MSE;
    	    	
    	    	if(Fc > flist[df][dfline])  //列因素F值检验
    	    		System.out.println("the line factor has a significant impact on the statistical data");
    	    	else
    	    		System.out.println("the line factor has no significant impact on the statistical data");
    	        
    	    	if(Fr > flist[df][dfrow])    //行因素F值检验
    	    		System.out.println("the row factor has a significant impact on the statistical data");
    	    	else
    	    		System.out.println("the row factor has no significant impact on the statistical data");
    	    }
    	}catch(FileNotFoundException e)  //捕获文件异常
    	{
    	    e.printStackTrace();	
    	}
    	catch(IOException exp)     //捕获输入输出异常
    	{ 
    		exp.printStackTrace();
    	} 	
    }
 
    
    public static void main(String[] args) throws Exception
    {
    	/*
    	 *运行命令实例：hjar /home/tseg/psq/AnalysisOfVariance_fat.jar psq/input psq/output1 psq/output2 psq/output3
    	 *命令行输入参数说明：
    	 *args[0]为第一个job的输入文件在hdfs上的路径,该路径同时也是第二个job的输入路径
    	 *args[1]为第一个job在hdfs上的输出路径,同时也是第三个job的输入路径
    	 *args[2]为第二个job的输出路径
    	 *args[3]为第三个job的输出路径
    	 *
    	 *一共四个job,job1求平均值，job2求SST,job3求SSA,job4求SSR，finaljudge()进行结果验证
    	 *由于并不是每次都需要双因素方差分析，因此job4的输出路径就在程序中指定了，不需要重新输入路径
    	 */
    	
    	int datarownum=0;          //记录一共有多少行数据，即每个水平中数据的个数
    	int datalinenum=0;         //记录一共有多少列数据，即一共有多少个水平
    	int dataallnum=0;          //记录一共有多少个数据
    	float dataallaverage=0;    //记录所有数据的平均值
    	float dataSST=0;               //总离差平方和
    	float dataSSA=0;               //水平项离差平方和（组间），也是列因素误差平方和
    	float dataSSE=0;               //单因素方差分析：误差项离差平方和（组内）
    	float dataSSR=0;               //行因素误差平方和
    	float datadoubleSSE=0;         //双因素方差分析：随机误差项平方和
    	int choice;                    //choice==1为单因素方差分析，choice==2为双因素方差分析
 
    	if(args.length != 4)   //这个错误白痴啊！！！！！！怎么就写成 !=2 了啊！！！！！！
    	{
    		System.err.println("Pan Siqun250: usage: AnalysisOfVariance <input path> <output path>");
    		System.exit(-1);
    	}
    	
    	BufferedReader readStream_choice = new BufferedReader(new InputStreamReader(System.in));
    	
    	System.out.println("You want the analysis of variance for single factor or two factors?");
    	System.out.println("Please input your choice(1 or 2):");
    	System.out.println("    1: Analysis of variance for single factor");
    	System.out.println("    2: Analysis of variance for two factors");
    	System.out.print("Your choice:");
    	
    	String inputStr = readStream_choice.readLine();
    	while(inputStr.length() != 1)
    	{
    		System.out.println("Input error, please input your choice again(1 or 2)");
    		System.out.print("Your choice:");
    		inputStr = readStream_choice.readLine();
    	}
    	choice = Integer.parseInt(inputStr);
    	
    	System.out.println("choice:"+choice);
    	
    	while((choice != 1) && (choice != 2))
    	{
    		System.out.println("no such choice, please input your choice again(1 or 2)");
    		System.out.print("Your choice:");	
    		inputStr = readStream_choice.readLine();
    		while(inputStr.length() != 1)
        	{
        		System.out.println("Input error, please input your choice again(1 or 2)");
        		System.out.print("Your choice:");
        		inputStr = readStream_choice.readLine();
        	}
    		choice = Integer.parseInt(inputStr);
    	}	
    	if(choice == 1)
    	    System.out.println("You have chosen the analysis of variance for single factor.");
    	else if(choice == 2)
    		System.out.println("You have chosen the analysis of variance for two factors.");
    	
    	
    	Job job1 = new Job();
    	job1.setJarByClass(AnalysisOfVariance.class);    
    	FileInputFormat.addInputPath(job1, new Path(args[0]));  //不同的job的输入输出路径可以设为不同的args[i]，最后只要命令行输入相应的路径就可以了
    	FileOutputFormat.setOutputPath(job1, new Path(args[1])); 	
    	
    	job1.setMapperClass(Map1.class);
    	job1.setReducerClass(Reduce1.class);
    	job1.setOutputKeyClass(IntWritable.class);
    	job1.setOutputValueClass(FloatWritable.class);
   
    	System.out.println("Job1 starts!");
    	job1.waitForCompletion(true);    	
    	System.out.println("Job1 ends!");
    	
    	
    	//将job1的结果文件拷贝到本地，进行简单的数据统计
    	String args1_temp = args[1];
    	String strdstPath = "/home/tseg/psq";
    	
    	args1_temp = args1_temp.concat("/part-r-00000");
    	
    	System.out.println("args1_temp:"+args1_temp);
    	
    	Configuration conf = new Configuration();
    	Path srcPath = new Path(args1_temp);  //复制的path必须指定为一个文件，不能是文件夹
    	Path dstPath = new Path(strdstPath);
    	
    	FileSystem srcFs;
    	srcFs = srcPath.getFileSystem(conf);
    	if(srcFs.exists(srcPath)){
			srcFs.copyToLocalFile(srcPath, dstPath);
		}
		else System.out.println("srcPath:" + srcFs.exists(srcPath));
    	try
    	{
    	    strdstPath = strdstPath.concat("/part-r-00000");
    	    
    	    System.out.println("strdstPath"+strdstPath);
    	    
    		BufferedReader readStream = new BufferedReader(new FileReader(strdstPath));
    	    String line;
    	    int flag = 0;
    	    int tempkey;
    	    
    	    while((line=readStream.readLine()) != null)
    	    {
    	    	StringTokenizer  strlist = new StringTokenizer(line); //取一行数据，默认空格为分隔符
    	    	while(strlist.hasMoreTokens() && flag == 0)
    	    	{
    	    		tempkey = Integer.parseInt(strlist.nextToken());   //读取key
    	    		
    	    		System.out.println("tempkey:"+tempkey);
    	    		
    	    		if(tempkey == 0)
    	    		{
    	    			dataallaverage = Float.parseFloat(strlist.nextToken());
    	    		}
    	    		else if(tempkey == -2)
    	    		{
    	    			dataallnum = (int)Float.parseFloat(strlist.nextToken());
    	    		}
    	    		else if(tempkey == -3)
    	    		{
    	    			datarownum = (int)Float.parseFloat(strlist.nextToken()); //因为重复写入，values里有很多linenum，所以只读第一个就行了
    	    			flag = 1;
    	    		}
    	    		else if(tempkey == -4)
    	    		{
    	    			datalinenum = (int)Float.parseFloat(strlist.nextToken());
    	    		}
    	    		else if(tempkey > 0 || tempkey == -1)
    	    		{
    	    			flag = 1;
    	    		}
    	    	}
    	    	flag = 0;
    	    }
    	}catch(FileNotFoundException e)  //捕获文件异常
    	{
    	    e.printStackTrace();	
    	}
    	 catch(IOException exp)     //捕获输入输出异常
    	{ 
    		exp.printStackTrace();
    	} 	
    	
    	System.out.println("datalinenum:"+datalinenum);
    	System.out.println("datarownum:"+datarownum);
    	System.out.println("dataallnum:"+dataallnum);
    	System.out.println("dataallaverage:"+dataallaverage);
    	  	
    	
    	
    	Job job2 = new Job();  //计算SST
    	job2.setJarByClass(AnalysisOfVariance.class);
    	
    	Configuration conf2 = job2.getConfiguration();
    	conf2.setFloat("dataallaverage", dataallaverage);
    	
    	FileInputFormat.addInputPath(job2, new Path(args[0]));   //job2的输入路径仍然是初始数据文件
    	FileOutputFormat.setOutputPath(job2, new Path(args[2])); //设置一个存放job2输出的路径文件
    		
    	job2.setMapperClass(Map2.class);
    	job2.setReducerClass(Reduce2.class);
    	job2.setOutputKeyClass(IntWritable.class);
    	job2.setOutputValueClass(FloatWritable.class); 	
    	
    	System.out.println("Job2 starts!");
    	job2.waitForCompletion(true);
    	System.out.println("Job2 ends!");
    	
    	
    	Job job3 = new Job();   //计算SSA
    	job3.setJarByClass(AnalysisOfVariance.class);
    	
    	Configuration conf3 = job3.getConfiguration();
    	conf3.setInt("datarownum", datarownum);
    	conf3.setFloat("dataallaverage",dataallaverage);
    	
    	FileInputFormat.addInputPath(job3, new Path(args[1]));   //job3的输入路径是job1的输出文件，用来读取每个水平（每列）的平均值
    	FileOutputFormat.setOutputPath(job3, new Path(args[3])); //设置一个存放job3输出的路径文件
    	
    	job3.setMapperClass(Map3.class);
    	job3.setReducerClass(Reduce3.class);
    	job3.setOutputKeyClass(IntWritable.class);
    	job3.setOutputValueClass(FloatWritable.class);
    	
    	System.out.println("Job3 starts!");
    	job3.waitForCompletion(true);
    	System.out.println("Job3 ends!");
    	
    	if(choice == 2)    //当进行的是双因素方差分析时
    	{
    	    Job job4 = new Job();   //计算SSR
    	    job4.setJarByClass(AnalysisOfVariance.class);
    	
    	    Configuration conf4 = job4.getConfiguration();
    	    conf4.setInt("datalinenum", datalinenum);
    	    conf4.setFloat("dataallaverage",dataallaverage);
    	
    	    FileInputFormat.addInputPath(job4, new Path(args[1]));   //job3的输入路径是job1的输出文件，用来读取每个水平（每列）的平均值
    	    FileOutputFormat.setOutputPath(job4, new Path("psq/output4")); //设置一个存放job3输出的路径文件
    	
    	    job4.setMapperClass(Map4.class);
    	    job4.setReducerClass(Reduce4.class);
    	    job4.setOutputKeyClass(IntWritable.class);
    	    job4.setOutputValueClass(FloatWritable.class);
    	
    	    System.out.println("Job4 starts!");
    	    job4.waitForCompletion(true);
    	    System.out.println("Job4 ends!");
    	}
    	
    	//用于从HDFS上读取SST的值用于处理
    	String args2_temp = args[2];
    	String strdstPathSST = "/home/tseg/psq/";
    	
    	args2_temp = args2_temp.concat("/part-r-00000");
    	Configuration confSST = new Configuration();
    	Path srcPathSST = new Path(args2_temp);
    	Path dstPathSST = new Path(strdstPathSST);
    	
    	FileSystem srcFsSST;
    	srcFsSST = srcPathSST.getFileSystem(confSST);
    	if(srcFsSST.exists(srcPathSST)){
			srcFsSST.copyToLocalFile(srcPathSST, dstPathSST);
		}
		else System.out.println("srcPathSST:" + srcFsSST.exists(srcPathSST));
    	try
    	{
    		strdstPathSST = strdstPathSST.concat("/part-r-00000");
    		BufferedReader readStreamSST = new BufferedReader(new FileReader(strdstPathSST));
    	    
    		
    		String lineSST;
    		int tempkeySST;
    	    while((lineSST=readStreamSST.readLine()) != null)
    	    {
	    	    StringTokenizer  strlistSST = new StringTokenizer(lineSST); //取一行数据，默认空格为分隔符
	    	                                                            //该文件只有一行数据，且只有两个值，即（key value），value是我们要的SST
	    	    tempkeySST = Integer.parseInt(strlistSST.nextToken());   //读取key，我们这里用不到key
	    	    dataSST = Float.parseFloat(strlistSST.nextToken());
	    	    
	    	    System.out.println("tempkeySST:"+tempkeySST);
	    	    System.out.println("dataSST:"+dataSST);
	    	    System.out.println("args2_temp:"+args2_temp);
	    	    System.out.println("strdstPathSST:"+strdstPathSST);
    	    }
    	}catch(FileNotFoundException e)  //捕获文件异常
    	{
    	    e.printStackTrace();	
    	}
    	 catch(IOException exp)     //捕获输入输出异常
    	{ 
    		exp.printStackTrace();
    	}
    	
    	
    	//用于从HDFS上读取SSA的值用于处理
    	String args3_temp = args[3];
    	String strdstPathSSA = "/home/tseg/psq/";
    	
    	args3_temp = args3_temp.concat("/part-r-00000");
    	Configuration confSSA = new Configuration();
    	Path srcPathSSA = new Path(args3_temp);
    	Path dstPathSSA = new Path(strdstPathSSA);
    	
    	FileSystem srcFsSSA;
    	srcFsSSA = srcPathSSA.getFileSystem(confSSA);
    	if(srcFsSSA.exists(srcPathSSA)){
			srcFsSSA.copyToLocalFile(srcPathSSA, dstPathSSA);
		}
		else System.out.println("srcPathSSA:" + srcFsSSA.exists(srcPathSSA));
    	try
    	{
    		strdstPathSSA = strdstPathSSA.concat("/part-r-00000");
    		BufferedReader readStreamSSA = new BufferedReader(new FileReader(strdstPathSSA));
    	    
    		String lineSSA;
    		int tempkeySSA;
    	    while((lineSSA=readStreamSSA.readLine()) != null)
    	    {
	    	    StringTokenizer  strlistSSA = new StringTokenizer(lineSSA); //取一行数据，默认空格为分隔符
	    	                                                            //该文件只有一行数据，且只有两个值，即（key value），value是我们要的SSA
	    	    tempkeySSA = Integer.parseInt(strlistSSA.nextToken());   //读取key，我们这里用不到key  	    
	    	    dataSSA = Float.parseFloat(strlistSSA.nextToken());
	    	    
	    	    dataSSE = dataSST-dataSSA;
	    	    
	    	    System.out.println("tempkeySSA:"+tempkeySSA);
	    	    System.out.println("dataSSA:"+dataSSA);
	    	    System.out.println("dataSSE:"+dataSSE);
	    	    System.out.println("args3_temp:"+args3_temp);
	    	    System.out.println("strdstPathSSA:"+strdstPathSSA);
    	    }
    	}catch(FileNotFoundException e)  //捕获文件异常
    	{
    	    e.printStackTrace();	
    	}
    	 catch(IOException exp)     //捕获输入输出异常
    	{ 
    		exp.printStackTrace();
    	} 
    	
    	if(choice == 2)
    	{
    		//用于从HDFS上读取SSR的值用于处理
    		String args4_temp = "psq/output4";
    		String strdstPathSSR = "/home/tseg/psq/";
    	
    		args4_temp = args4_temp.concat("/part-r-00000");
    		Configuration confSSR = new Configuration();
    		Path srcPathSSR = new Path(args4_temp);
    		Path dstPathSSR = new Path(strdstPathSSR);
    	
    		FileSystem srcFsSSR;
    		srcFsSSR = srcPathSSR.getFileSystem(confSSR);
    		if(srcFsSSR.exists(srcPathSSR)){
    			srcFsSSR.copyToLocalFile(srcPathSSR, dstPathSSR);
    		}
    		else System.out.println("srcPathSSR:" + srcFsSSR.exists(srcPathSSR));
    		try
    		{
    			strdstPathSSR = strdstPathSSR.concat("/part-r-00000");
    			BufferedReader readStreamSSR = new BufferedReader(new FileReader(strdstPathSSR));
    	    
    			String lineSSR;
    			int tempkeySSR;
    			while((lineSSR=readStreamSSR.readLine()) != null)
    			{
    				StringTokenizer  strlistSSR = new StringTokenizer(lineSSR); //取一行数据，默认空格为分隔符
	    	                                                            //该文件只有一行数据，且只有两个值，即（key value），value是我们要的SSR
    				tempkeySSR = Integer.parseInt(strlistSSR.nextToken());   //读取key，我们这里用不到key  	    
    				dataSSR = Float.parseFloat(strlistSSR.nextToken());
	    	    
    				datadoubleSSE = dataSST-dataSSA-dataSSR;
	    	    
    				System.out.println("tempkeySSR:"+tempkeySSR);
    				System.out.println("dataSSR:"+dataSSR);
    				System.out.println("datadoubleSSE:"+datadoubleSSE);
    				System.out.println("args4_temp:"+args4_temp);
    				System.out.println("strdstPathSSR:"+strdstPathSSR);
    			}
    		}catch(FileNotFoundException e)  //捕获文件异常
    		{
    			e.printStackTrace();	
    		}
    		catch(IOException exp)     //捕获输入输出异常
    		{ 
    			exp.printStackTrace();
    		} 	
        }
    	
        //进行F分布检验
    	float flagSSR = -1;
    	int flagrownum = -1;
    	if(choice == 1)  //当是单因素方差分析时，实参flagSSR以及flagrownum都是-1,这样函数就可以通过对这两个值对是否为双因素方差分析进行验证
    		finaljudge(dataSSA, dataSSE, flagSSR, datalinenum, flagrownum, dataallnum);
    	else if(choice == 2)    //
    	{
    		flagSSR = dataSSR;
    		flagrownum = datarownum;
    	    finaljudge(dataSSA, datadoubleSSE, flagSSR, datalinenum, flagrownum, dataallnum);
    	}   		
    }
}
