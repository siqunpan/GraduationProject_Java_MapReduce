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
//import org.apache.hadoop.mapred.lib.ChainMapper;    //��0.20.2��ChainMapper��ChainReducer�����ĸ�����
//import org.apache.hadoop.mapred.lib.ChainReducer;



/**
 * 
 * @author peterpan
 * ���������ʽ���ӣ�hjar AnalysisOfVariance_fat.jar psq/input psq/output1 psq/output2 psq/output3
 * ע��������������Ŀ¼��ֻ��һ�������ļ�������ͳ�����ݵı�
 *    
 */

public class AnalysisOfVariance     //������Ǿ�������
{	
	public static class Map1 extends Mapper<LongWritable, Text, IntWritable, FloatWritable>  
    {	  	
    	public void map(LongWritable key, Text value, Context context)
    	throws IOException, InterruptedException   //����Ϊtxt�ļ���������Զ���ÿһ����Ϊmap�����һ����ֵ�ԣ�
    	{                                          //���Ǹ�����ʼλ��������ļ���ʼλ�õ�ƫ����������Ϊ������Ϣ   
    		/*
    		 * map()����˵����
    		 *     map()����Ϊ�����ļ�����ʽΪ.txt����Ϊ�������ļ�
    		 * map()���˵����
    		 *     keyΪ������key��ʾ�е���ţ���ˮƽ��valueΪÿ���µ����ݽ��
    		 *     key==0��    valueΪ�������ݵļ���
    		 *     key==-1: valueΪ������ÿһ�е�ƽ��ֵ����
    		 *     key==-2: valueΪ���ݵ���������ˮƽ������Ϊд���˺ܶ�飬����ֵ��ȡvalue�ĵ�һ��ֵ�Ϳ����� 
    		 * */
    		
    		int level = 0;
    		float element = 0;
    		float rowaverage = 0;
    		float rowsum = 0;
    		String line = value.toString();
    		StringTokenizer strtok = new StringTokenizer(line);//StringTokenizerĬ�Ϸָ���Ϊ�ո�
    	
    		
    		while(strtok.hasMoreTokens()) //ÿ��map���������key��ˮƽ���ͣ�value��ÿ��ˮƽ��ÿ��Ԫ�صļ���
    	    {
    	    	element = Float.parseFloat(strtok.nextToken());
    	    	
    	    	
    	    	level+=1;
    	    	context.write(new IntWritable(level), new FloatWritable(element));  //���ڼ���ÿ����ƽ��ֵ
    	        context.write(new IntWritable(0), new FloatWritable(element));    //key==0��value��ֵΪ�������ݼ���
    	    	//������ÿ�е�ƽ��ֵ����������е�ƽ��ֵ�������key=0��values��
    	    	rowsum += element;
    	    	
    	    	System.out.println("element:"+element);
    	    	
    	    }
            rowaverage = rowsum/level;
            context.write(new IntWritable(-1), new FloatWritable(rowaverage));//key==-1��valuesΪÿһ�е�ƽ��ֵ����
            context.write(new IntWritable(-2), new FloatWritable((float)level));//key==-2��valueΪ��������,��Ϊд��reduce�кܶ�飬����ֻҪ��ȡvalues�еĵ�һ��ֵ�Ϳ�����
            
            System.out.println("level:"+level);
            System.out.println("rowaverage:"+rowaverage);
    	}
    }
    
    public static class Reduce1 extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>    //��reduce�������������ˮƽ�Լ�����ľ�ֵ��֮��Ĵ���������һ��map��reduce����
    {
    	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
    	throws IOException, InterruptedException  //���԰Ѳ�ͬmap����ļ�ֵ�Ե�ֵ����������Ӧ�ÿ��԰ɣ�
    	{
    		/*
    		 * reduce()����˵����
    		 *     keyΪ������key��ʾ�е���ţ���ˮƽ��valueΪÿ���µ����ݽ��
    		 *     key==0��    valueΪ�������ݵļ���
    		 *     key==-1: valueΪ������ÿһ�е�ƽ��ֵ����
    		 *     key==-2: valueΪ���ݵ���������ˮƽ������Ϊд���˺ܶ�飬����ֵ��ȡvalue�ĵ�һ��ֵ�Ϳ�����
    		 * reduce()���˵����
    		 *     keyΪ������key��ʾ�е���ţ���ˮƽ��valueΪ���е�ƽ��ֵ
    		 *     key==0:  valueΪ�������ݵ�ƽ��ֵ
    		 *     key==-1: valueΪ������ƽ��ֵ�ļ���
    		 *     key==-2: valueΪ�������ݵĸ���
    		 *     key==-3: valueΪ���ݵ�����
    		 *     key==-4: valueΪ���ݵ�����    
    		 * */
    		
    		float sum = 0;       //values���������ݵĺ�,��ÿ�еĺ�
    		float sum_alldata = 0;    //�������ݵĺ�
    		float average = 0;   //values���������ݵ�ƽ��ֵ
    		float average_alldata = 0; //�������ݵ�ƽ��ֵ
    		int num = 0;       //values�е����ݵĸ���
    		int rownum_alldata = 0;    //�������ݵ�����
    		int allnum_alldata = 0;    //�������ݵĸ���
    	    float tempvalue; 
    	    
    	    System.out.println("key:"+key);
    	    
    	    if(key.equals(new IntWritable(0)))    //key==0��������valuesΪ�������ݼ���
    	    {         //Writable ���͵ı�������ֱ�ӱȽϣ�������
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
    	    else if(key.equals(new IntWritable(-1)))   //key==-1��valuesΪÿһ�е�ƽ��ֵ����
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
    	    else if(key.equals(new IntWritable(-2)))    //key==-2��valueΪ��������,��Ϊд��reduce�кܶ�飬����ֻҪ��ȡvalues�еĵ�һ��ֵ�Ϳ�����
    	    {
    	    	System.out.println("key == -2 da yin ma!!!!!!");
    	    	int flagline = 0;
    	    	for(FloatWritable valueline : values)//key==-2��valueΪ����������ֻ��һ��ֵ
    	    	{
    	    		if(flagline == 0)
    	    		    context.write(new IntWritable(-4), valueline);
    	    		flagline = 1;
    	    		System.out.println("valueline:"+valueline.get());
    	    	}
    	    	
    	    }
    	    else     //��ÿһ��������ƽ��ֵ
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
    	public void map(LongWritable key, Text value, Context context)//map2��Ȼ�Գ�ʼ�����ļ�δ�����ļ�������Ϊ�˵õ���������
    	throws IOException, InterruptedException   //����Ϊtxt�ļ���������Զ���ÿһ����Ϊmap�����һ����ֵ�ԣ�
    	{                
    		//���Ǹ�����ʼλ��������ļ���ʼλ�õ�ƫ����������Ϊ������Ϣ   
    		float element2,temp;
    		float allaveragemap2;  //���ڴ�Ŵ�main()���������������ƽ��ֵ
    		
            Configuration confmap2 = context.getConfiguration(); //����main()����������������
    		allaveragemap2 = confmap2.getFloat("dataallaverage",-1); 
    		
    		System.out.println("allaveragemap2:"+allaveragemap2);
    		
    		String line2 = value.toString();
    		StringTokenizer strtok = new StringTokenizer(line2);//StringTokenizerĬ�Ϸָ���Ϊ�ո�   		
    		
    		while(strtok.hasMoreTokens()) 
    	    {
    	    	element2 = Float.parseFloat(strtok.nextToken());
    	    	
    	    	System.out.println("element2:"+element2);
    	    	
    	    	temp = (element2-allaveragemap2)*(element2-allaveragemap2);
    	    	System.out.println("temp:"+temp);
    	    	context.write(new IntWritable(-2), new FloatWritable(temp)); //����value��key��ͬ���������reduce����ͼ��� 
    	    }
    	}
    }
    
    public static class Reduce2 extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>    //��reduce��SST��ֻ������һ��reduce����
    {
    	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
    	throws IOException, InterruptedException  //���԰Ѳ�ͬmap����ļ�ֵ�Ե�ֵ����������Ӧ�ÿ��԰ɣ�
    	{
    		float SST = 0;      //�����ƽ����   key=-2
    		
    		for(FloatWritable value : values)
    		{
    			SST += value.get();
    		}
    		
    		context.write(new IntWritable(-2), new FloatWritable(SST));
    	}
    }         
    
    
    public static class Map3 extends Mapper<LongWritable, Text, IntWritable, FloatWritable>  
    {	  	
    	public void map(LongWritable key, Text value, Context context)//map3��job1������ļ���Ϊ���룬��Ϊ�˵õ�ÿ��ˮƽ��ÿһ�У���ƽ��ֵ
    	throws IOException, InterruptedException   //����Ϊtxt�ļ���������Զ���ÿһ����Ϊmap�����һ����ֵ�ԣ�
    	{                                          //���Ǹ�����ʼλ��������ļ���ʼλ�õ�ƫ����������Ϊ������Ϣ   
    		int tempkey;   //��Ϊtempkey��if����У��������if��䲻��������ķ�¿��Կ��ܲ��ᱻʹ�ã���˱��˾���
    		float tempvalue, temp3;
    	    int rownummap3;    //���ڴ�Ŵ�main()�����д������������
    	    float allaveragemap3; //���ڴ�Ŵ�main()�����д����������ƽ��ֵ
    	    int flag = 0;
    	    
    	    String line3 = value.toString();   //ÿһ�����ݾ������磺��1 3.1��, ��һ��keyֵ��һ��valueֵ
    	    StringTokenizer strtok2 = new StringTokenizer(line3);
    	    
    	    Configuration confmap3 = context.getConfiguration();  //���ô�main()�����崫�������
    	    rownummap3 = confmap3.getInt("datarownum", -1);
    	    allaveragemap3 = confmap3.getFloat("dataallaverage", -2);
    	    
    	    System.out.println("rownummap3:"+rownummap3);
    	    System.out.println("allaveragemap3:"+allaveragemap3);
    	    
    	    while(strtok2.hasMoreTokens() && flag == 0)
    	    {
    	        tempkey = Integer.parseInt(strtok2.nextToken());  //�����ȡ����key,������ҪҪ����value
    	        
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
    
    public static class Reduce3 extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>    //��reduce��SSA��ֻ������һ��reduce����
    {
    	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
    	throws IOException, InterruptedException  //���԰Ѳ�ͬmap����ļ�ֵ�Ե�ֵ����������Ӧ�ÿ��԰ɣ�
    	{
    		float SSA = 0;   //ˮƽ�����ƽ���ͣ���䣩key=-4.��ΪSSE���û����������ѡ����SST��SSA��Ȼ������SSE=SST-SSA����SSE   		
    		
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
    	public void map(LongWritable key, Text value, Context context)//map4��job1������ļ���Ϊ���룬��Ϊ�˵õ�ÿ�е�ƽ��ֵ
    	throws IOException, InterruptedException   //����Ϊtxt�ļ���������Զ���ÿһ����Ϊmap�����һ����ֵ�ԣ�
    	{                                          //���Ǹ�����ʼλ��������ļ���ʼλ�õ�ƫ����������Ϊ������Ϣ   
    		
    		int tempkey; //��Ϊtempkey��if����У��������if��䲻��������ķ�¿��Կ��ܲ��ᱻʹ�ã���˱��˾���
    		float tempvalue, temp4;
    		int linenummap4;    //���ڴ�Ŵ�main()�����д������������
    		float allaveragemap4; //���ڴ�Ŵ�main()�����д����������ƽ��ֵ
    	    int flag = 0;
    	    
    	    String line4 = value.toString();   //ÿһ�����ݾ������磺��1 3.1��, ��һ��keyֵ��һ��valueֵ
    	    StringTokenizer strtok4 = new StringTokenizer(line4);
    	    
    	    Configuration confmap4 = context.getConfiguration();  //���ô�main()�����崫�������
    	    linenummap4 = confmap4.getInt("datalinenum", -1);
    	    allaveragemap4 = confmap4.getFloat("dataallaverage", -2);
    	    
    	    System.out.println("linenummap4:"+linenummap4);
    	    System.out.println("allaveragemap4:"+allaveragemap4);
    	    
    	    while(strtok4.hasMoreTokens() && flag == 0)
    	    {
    	    	tempkey = Integer.parseInt(strtok4.nextToken());  //�����ȡ����key,������ҪҪ����value
    	    
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
    
    public static class Reduce4 extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>    //��reduce��SSA��ֻ������һ��reduce����
    {
    	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
    	throws IOException, InterruptedException  //���԰Ѳ�ͬmap����ļ�ֵ�Ե�ֵ����������Ӧ�ÿ��԰ɣ�
    	{
    		float SSR = 0;   //���������ƽ����,key=-5.��ΪSSE���û����������ѡ����SST��SSC����SSA����Ȼ������SSE=SST-SSC-SSR����SSE   		
    		
    		for(FloatWritable value : values)
    		{
    			System.out.println("value:"+value.get());
    			SSR += value.get();
    		}

    		context.write(new IntWritable(-5), new FloatWritable(SSR));
    	}
    }   
    
    
    public static void finaljudge(float SSA, float SSE, float SSR, int linenum, int rownum, int allnum)    //��������F�ٽ�ֵ��֤����,��ڲ���ΪF�ֲ�����hdfs�ϵ�·��
    {
    	try
    	{
    	    String line;
    	    float listdata;
    	    int frow, fline;
    	    
    	    float[][] flist = new float[122][122]; //��Ŧ�=0.05ʱ��F�������ϸ���F��������д�ţ����������ݴ����flist[121][j]��flist[i][121]��
    	    BufferedReader readStream = new BufferedReader(new FileReader("/home/tseg/psq/flist.txt"));
    	    
    	    
    	    
    	    for(frow=0; frow<122; frow++)
    	    	for(fline=0; fline<122; fline++)
    	    	    flist[frow][fline] = 0;
    	    
    	    frow = 0;
    	    fline = 0;
    	    	    
    	    while((line=readStream.readLine()) != null)  //��д�ı��ǲ���F�ֲ�������������
    	    {
    	    	frow++;
    	    	StringTokenizer  strlist = new StringTokenizer(line); //ȡһ�����ݣ�Ĭ�Ͽո�Ϊ�ָ���
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
    	    
    	    if(SSR == -1 && rownum ==-1)   //��Ϊ�����ط������
    	    {
    	    	int df1, df2;      //df1Ϊ��һ���ɶȣ� df2Ϊ�ڶ����ɶ�
    	    	float MSA, MSE, F;   //MSAΪ��������MSEΪ���ھ���,FΪFֵ
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
    	    else   //��Ϊ˫���ط������
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
    	    	
    	    	if(Fc > flist[df][dfline])  //������Fֵ����
    	    		System.out.println("the line factor has a significant impact on the statistical data");
    	    	else
    	    		System.out.println("the line factor has no significant impact on the statistical data");
    	        
    	    	if(Fr > flist[df][dfrow])    //������Fֵ����
    	    		System.out.println("the row factor has a significant impact on the statistical data");
    	    	else
    	    		System.out.println("the row factor has no significant impact on the statistical data");
    	    }
    	}catch(FileNotFoundException e)  //�����ļ��쳣
    	{
    	    e.printStackTrace();	
    	}
    	catch(IOException exp)     //������������쳣
    	{ 
    		exp.printStackTrace();
    	} 	
    }
 
    
    public static void main(String[] args) throws Exception
    {
    	/*
    	 *��������ʵ����hjar /home/tseg/psq/AnalysisOfVariance_fat.jar psq/input psq/output1 psq/output2 psq/output3
    	 *�������������˵����
    	 *args[0]Ϊ��һ��job�������ļ���hdfs�ϵ�·��,��·��ͬʱҲ�ǵڶ���job������·��
    	 *args[1]Ϊ��һ��job��hdfs�ϵ����·��,ͬʱҲ�ǵ�����job������·��
    	 *args[2]Ϊ�ڶ���job�����·��
    	 *args[3]Ϊ������job�����·��
    	 *
    	 *һ���ĸ�job,job1��ƽ��ֵ��job2��SST,job3��SSA,job4��SSR��finaljudge()���н����֤
    	 *���ڲ�����ÿ�ζ���Ҫ˫���ط�����������job4�����·�����ڳ�����ָ���ˣ�����Ҫ��������·��
    	 */
    	
    	int datarownum=0;          //��¼һ���ж��������ݣ���ÿ��ˮƽ�����ݵĸ���
    	int datalinenum=0;         //��¼һ���ж��������ݣ���һ���ж��ٸ�ˮƽ
    	int dataallnum=0;          //��¼һ���ж��ٸ�����
    	float dataallaverage=0;    //��¼�������ݵ�ƽ��ֵ
    	float dataSST=0;               //�����ƽ����
    	float dataSSA=0;               //ˮƽ�����ƽ���ͣ���䣩��Ҳ�����������ƽ����
    	float dataSSE=0;               //�����ط����������������ƽ���ͣ����ڣ�
    	float dataSSR=0;               //���������ƽ����
    	float datadoubleSSE=0;         //˫���ط����������������ƽ����
    	int choice;                    //choice==1Ϊ�����ط��������choice==2Ϊ˫���ط������
 
    	if(args.length != 4)   //�������׳հ���������������ô��д�� !=2 �˰�������������
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
    	FileInputFormat.addInputPath(job1, new Path(args[0]));  //��ͬ��job���������·��������Ϊ��ͬ��args[i]�����ֻҪ������������Ӧ��·���Ϳ�����
    	FileOutputFormat.setOutputPath(job1, new Path(args[1])); 	
    	
    	job1.setMapperClass(Map1.class);
    	job1.setReducerClass(Reduce1.class);
    	job1.setOutputKeyClass(IntWritable.class);
    	job1.setOutputValueClass(FloatWritable.class);
   
    	System.out.println("Job1 starts!");
    	job1.waitForCompletion(true);    	
    	System.out.println("Job1 ends!");
    	
    	
    	//��job1�Ľ���ļ����������أ����м򵥵�����ͳ��
    	String args1_temp = args[1];
    	String strdstPath = "/home/tseg/psq";
    	
    	args1_temp = args1_temp.concat("/part-r-00000");
    	
    	System.out.println("args1_temp:"+args1_temp);
    	
    	Configuration conf = new Configuration();
    	Path srcPath = new Path(args1_temp);  //���Ƶ�path����ָ��Ϊһ���ļ����������ļ���
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
    	    	StringTokenizer  strlist = new StringTokenizer(line); //ȡһ�����ݣ�Ĭ�Ͽո�Ϊ�ָ���
    	    	while(strlist.hasMoreTokens() && flag == 0)
    	    	{
    	    		tempkey = Integer.parseInt(strlist.nextToken());   //��ȡkey
    	    		
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
    	    			datarownum = (int)Float.parseFloat(strlist.nextToken()); //��Ϊ�ظ�д�룬values���кܶ�linenum������ֻ����һ��������
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
    	}catch(FileNotFoundException e)  //�����ļ��쳣
    	{
    	    e.printStackTrace();	
    	}
    	 catch(IOException exp)     //������������쳣
    	{ 
    		exp.printStackTrace();
    	} 	
    	
    	System.out.println("datalinenum:"+datalinenum);
    	System.out.println("datarownum:"+datarownum);
    	System.out.println("dataallnum:"+dataallnum);
    	System.out.println("dataallaverage:"+dataallaverage);
    	  	
    	
    	
    	Job job2 = new Job();  //����SST
    	job2.setJarByClass(AnalysisOfVariance.class);
    	
    	Configuration conf2 = job2.getConfiguration();
    	conf2.setFloat("dataallaverage", dataallaverage);
    	
    	FileInputFormat.addInputPath(job2, new Path(args[0]));   //job2������·����Ȼ�ǳ�ʼ�����ļ�
    	FileOutputFormat.setOutputPath(job2, new Path(args[2])); //����һ�����job2�����·���ļ�
    		
    	job2.setMapperClass(Map2.class);
    	job2.setReducerClass(Reduce2.class);
    	job2.setOutputKeyClass(IntWritable.class);
    	job2.setOutputValueClass(FloatWritable.class); 	
    	
    	System.out.println("Job2 starts!");
    	job2.waitForCompletion(true);
    	System.out.println("Job2 ends!");
    	
    	
    	Job job3 = new Job();   //����SSA
    	job3.setJarByClass(AnalysisOfVariance.class);
    	
    	Configuration conf3 = job3.getConfiguration();
    	conf3.setInt("datarownum", datarownum);
    	conf3.setFloat("dataallaverage",dataallaverage);
    	
    	FileInputFormat.addInputPath(job3, new Path(args[1]));   //job3������·����job1������ļ���������ȡÿ��ˮƽ��ÿ�У���ƽ��ֵ
    	FileOutputFormat.setOutputPath(job3, new Path(args[3])); //����һ�����job3�����·���ļ�
    	
    	job3.setMapperClass(Map3.class);
    	job3.setReducerClass(Reduce3.class);
    	job3.setOutputKeyClass(IntWritable.class);
    	job3.setOutputValueClass(FloatWritable.class);
    	
    	System.out.println("Job3 starts!");
    	job3.waitForCompletion(true);
    	System.out.println("Job3 ends!");
    	
    	if(choice == 2)    //�����е���˫���ط������ʱ
    	{
    	    Job job4 = new Job();   //����SSR
    	    job4.setJarByClass(AnalysisOfVariance.class);
    	
    	    Configuration conf4 = job4.getConfiguration();
    	    conf4.setInt("datalinenum", datalinenum);
    	    conf4.setFloat("dataallaverage",dataallaverage);
    	
    	    FileInputFormat.addInputPath(job4, new Path(args[1]));   //job3������·����job1������ļ���������ȡÿ��ˮƽ��ÿ�У���ƽ��ֵ
    	    FileOutputFormat.setOutputPath(job4, new Path("psq/output4")); //����һ�����job3�����·���ļ�
    	
    	    job4.setMapperClass(Map4.class);
    	    job4.setReducerClass(Reduce4.class);
    	    job4.setOutputKeyClass(IntWritable.class);
    	    job4.setOutputValueClass(FloatWritable.class);
    	
    	    System.out.println("Job4 starts!");
    	    job4.waitForCompletion(true);
    	    System.out.println("Job4 ends!");
    	}
    	
    	//���ڴ�HDFS�϶�ȡSST��ֵ���ڴ���
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
	    	    StringTokenizer  strlistSST = new StringTokenizer(lineSST); //ȡһ�����ݣ�Ĭ�Ͽո�Ϊ�ָ���
	    	                                                            //���ļ�ֻ��һ�����ݣ���ֻ������ֵ������key value����value������Ҫ��SST
	    	    tempkeySST = Integer.parseInt(strlistSST.nextToken());   //��ȡkey�����������ò���key
	    	    dataSST = Float.parseFloat(strlistSST.nextToken());
	    	    
	    	    System.out.println("tempkeySST:"+tempkeySST);
	    	    System.out.println("dataSST:"+dataSST);
	    	    System.out.println("args2_temp:"+args2_temp);
	    	    System.out.println("strdstPathSST:"+strdstPathSST);
    	    }
    	}catch(FileNotFoundException e)  //�����ļ��쳣
    	{
    	    e.printStackTrace();	
    	}
    	 catch(IOException exp)     //������������쳣
    	{ 
    		exp.printStackTrace();
    	}
    	
    	
    	//���ڴ�HDFS�϶�ȡSSA��ֵ���ڴ���
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
	    	    StringTokenizer  strlistSSA = new StringTokenizer(lineSSA); //ȡһ�����ݣ�Ĭ�Ͽո�Ϊ�ָ���
	    	                                                            //���ļ�ֻ��һ�����ݣ���ֻ������ֵ������key value����value������Ҫ��SSA
	    	    tempkeySSA = Integer.parseInt(strlistSSA.nextToken());   //��ȡkey�����������ò���key  	    
	    	    dataSSA = Float.parseFloat(strlistSSA.nextToken());
	    	    
	    	    dataSSE = dataSST-dataSSA;
	    	    
	    	    System.out.println("tempkeySSA:"+tempkeySSA);
	    	    System.out.println("dataSSA:"+dataSSA);
	    	    System.out.println("dataSSE:"+dataSSE);
	    	    System.out.println("args3_temp:"+args3_temp);
	    	    System.out.println("strdstPathSSA:"+strdstPathSSA);
    	    }
    	}catch(FileNotFoundException e)  //�����ļ��쳣
    	{
    	    e.printStackTrace();	
    	}
    	 catch(IOException exp)     //������������쳣
    	{ 
    		exp.printStackTrace();
    	} 
    	
    	if(choice == 2)
    	{
    		//���ڴ�HDFS�϶�ȡSSR��ֵ���ڴ���
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
    				StringTokenizer  strlistSSR = new StringTokenizer(lineSSR); //ȡһ�����ݣ�Ĭ�Ͽո�Ϊ�ָ���
	    	                                                            //���ļ�ֻ��һ�����ݣ���ֻ������ֵ������key value����value������Ҫ��SSR
    				tempkeySSR = Integer.parseInt(strlistSSR.nextToken());   //��ȡkey�����������ò���key  	    
    				dataSSR = Float.parseFloat(strlistSSR.nextToken());
	    	    
    				datadoubleSSE = dataSST-dataSSA-dataSSR;
	    	    
    				System.out.println("tempkeySSR:"+tempkeySSR);
    				System.out.println("dataSSR:"+dataSSR);
    				System.out.println("datadoubleSSE:"+datadoubleSSE);
    				System.out.println("args4_temp:"+args4_temp);
    				System.out.println("strdstPathSSR:"+strdstPathSSR);
    			}
    		}catch(FileNotFoundException e)  //�����ļ��쳣
    		{
    			e.printStackTrace();	
    		}
    		catch(IOException exp)     //������������쳣
    		{ 
    			exp.printStackTrace();
    		} 	
        }
    	
        //����F�ֲ�����
    	float flagSSR = -1;
    	int flagrownum = -1;
    	if(choice == 1)  //���ǵ����ط������ʱ��ʵ��flagSSR�Լ�flagrownum����-1,���������Ϳ���ͨ����������ֵ���Ƿ�Ϊ˫���ط������������֤
    		finaljudge(dataSSA, dataSSE, flagSSR, datalinenum, flagrownum, dataallnum);
    	else if(choice == 2)    //
    	{
    		flagSSR = dataSSR;
    		flagrownum = datarownum;
    	    finaljudge(dataSSA, datadoubleSSE, flagSSR, datalinenum, flagrownum, dataallnum);
    	}   		
    }
}
