package com.xxo.access;

import java.io.IOException;
import java.util.regex.Pattern;

import com.xxo.utils.Metadata;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AcsLogMapper extends Mapper<Object, Text, Text, AcsLogRecord> {
	// 定义分隔符
	private Pattern pattern = Pattern.compile("\\|");
	private Text text_key = new Text();
//	private Text text_value = new Text();

	AcsLogRecord acr = new AcsLogRecord();
	// 字段位置
	static int HOUSE_ID = 0;
	static int SRCIP = 0;
	static int DESTIP = 0;
	static int SRC_PORT = 0;
	static int DEST_PORT = 0;
	static int DOMAIN_NAME = 0;
	static int URL = 0;
	static int DURATION = 0;
	static int ACCESS_TIME = 0;

	/*
	 * 初始化参数
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// 获取配置文件 初始化字段位置
		Metadata md = new Metadata("/accesslogs.properties");
		HOUSE_ID = Integer.parseInt(md.getValue("HOUSE_ID"));
		SRCIP = Integer.parseInt(md.getValue("SRCIP"));
		DESTIP = Integer.parseInt(md.getValue("DESTIP"));
		SRC_PORT = Integer.parseInt(md.getValue("SRC_PORT"));
		DEST_PORT = Integer.parseInt(md.getValue("DEST_PORT"));
		DOMAIN_NAME = Integer.parseInt(md.getValue("DOMAIN_NAME"));
		URL = Integer.parseInt(md.getValue("URL"));
		DURATION = Integer.parseInt(md.getValue("DURATION"));
		ACCESS_TIME = Integer.parseInt(md.getValue("ACCESS_TIME"));
	}

	/*
	 * 业务逻辑
	 */
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		// 切分数据行
		String[] linedata = pattern.split(value.toString(), -1);

		if (linedata.length == 9) {//校验数据字段
			// 拼key
			StringBuffer key_sb = new StringBuffer();
			key_sb.append(linedata[HOUSE_ID]).append("|"); // house_id
			key_sb.append(StringUtils.isBlank(linedata[SRCIP])?"0.0.0.0":linedata[SRCIP]).append("|"); //数据回填
			key_sb.append(StringUtils.isBlank(linedata[DESTIP])?"0.0.0.0":linedata[DESTIP]).append("|");
			key_sb.append(StringUtils.isBlank(linedata[SRC_PORT])?"99999":linedata[SRC_PORT]).append("|");
			key_sb.append(StringUtils.isBlank(linedata[DEST_PORT])?"99999":linedata[DEST_PORT]).append("|");
			key_sb.append(StringUtils.isBlank(linedata[DOMAIN_NAME])?"9999999999":linedata[DOMAIN_NAME]).append("|");
			key_sb.append(StringUtils.isBlank(linedata[URL])?"9999999999":linedata[URL]);
			text_key.set(key_sb.toString());
			
			//将指标字段封装到Record类中
			acr.duration = StringUtils.isBlank(linedata[DURATION])?"000000":linedata[DURATION];
			
			//写入Reduce端
			context.write(text_key, acr);
			
		}else{
			System.out.println("linedata.length==" + linedata.length);
		}

	}

	/*
	 * 释放资源
	 */
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
