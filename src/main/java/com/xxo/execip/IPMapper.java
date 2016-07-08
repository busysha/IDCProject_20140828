package com.xxo.execip;

import com.xxo.utils.Metadata;
import com.xxo.utils.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 异常IP过滤Mapper
 * Created by xiaoxiaomo on 2014/6/30.
 */
public class IPMapper extends Mapper<Object , Text , Text , NullWritable> {

    Pattern pattern = Pattern.compile("\\t") ;
    
    //初始化字段位置
    static Integer COMMAND_ID = 0 ; //指令ID
    static Integer HOUSE_ID   = 0 ; //机房ID
    static Integer SRCIP_ID   = 0 ; //用户IP
    static Integer DESTIP_ID  = 0 ; //目的IP
    private Map<String, String> map ;
    private Text v2 ;
    StringBuffer sb ;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //1. 初始化
        Metadata metadata = new Metadata("/MoFrdpiSecurityLogs.properties");
        COMMAND_ID = Integer.valueOf(metadata.getValue("COMMAND_ID")) ;
        HOUSE_ID   = Integer.valueOf(metadata.getValue("HOUSE_ID")) ;
        SRCIP_ID   = Integer.valueOf(metadata.getValue("SRCIP_ID")) ;
        DESTIP_ID  = Integer.valueOf(metadata.getValue("DESTIP_ID")) ;

        //2. 查询Redis数据
        map = new HashMap<String, String>();
        map = RedisUtil.findCode2CodeIDMap("house_ip_info");

        v2 = new Text() ;

    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = pattern.split(key.toString(), -1);
        if( split != null && split.length ==25 ){
            sb = new StringBuffer();
            sb.append( split[COMMAND_ID] ).append("|"); //非空
            sb.append( split[HOUSE_ID] ).append("|");   //非空
            sb.append( StringUtils.isBlank(split[SRCIP_ID] )?"0.0.0.0":split[SRCIP_ID]).append("|") ;


            //关联redis表确定是否为异常ip
            if( !map.containsValue( split[ DESTIP_ID ] ) ){

                //如果合法ip表中不包含该ip，写出该条记录
                sb.append( split[DESTIP_ID] );
                v2.set( sb.toString() );
                context.write( v2, NullWritable.get() );
            }
        }

        else{
            System.out.println("linedatas.length == "+split.length);
        }




    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
