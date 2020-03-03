package com.tct.bigdata.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import static com.tct.bigdata.etl.TimeUtil.DATE_FORMAT_YMD;
import static com.tct.bigdata.etl.TimeUtil.ONE_DAY_MILLISECOND;

public class MidEventEtl {

    private static final String PRESTO_URL = "jdbc:presto://localhost:8889/hive/default?user=root&password=";

    private static final String SQL_TEMPLATE = "select packagename, teyeid, appversion, basic_devicename, " +
            "format_datetime(from_unixtime(try_cast(happenTime as bigint)/1000),'yyyy-MM-dd') as eventTime " +
            "from teye_mobile_event " +
            "where info ='%s' and happentime >= '%d' and happentime < '%d' and packagename is not null " +
            "and teyeid is not null " +
            "group by packagename, teyeid, appversion, basic_devicename, format_datetime(from_unixtime(try_cast(happenTime as bigint)/1000),'yyyy-MM-dd')";


    private static final String TEST_SQL = "select format_datetime(from_unixtime(try_cast('1577814763000' as bigint)/1000), 'yyyy-MM-dd HH:mm:ss')";

    private static final String TABLE_NAME = "kylin_mobile_event";
    private static final Long ALLOW_MAX_DELAY = ONE_DAY_MILLISECOND * 6;

    /**
     * parquet file schema
     */
    private static final String schemaStr = "message hive_schema {\n" +
            "  optional binary packagename (UTF8);\n" +
            "  optional binary teyeid (UTF8);\n" +
            "  optional binary appversion (UTF8);\n" +
            "  optional binary basic_devicename (UTF8);\n" +
            "}";

    private static final String SAVE_PATH_TEMPLATE = "/user/hive/warehouse/kylin_mobile_event/info=%s/%s";

    private static final String HIVE_URL = "jdbc:hive2://10.90.18.7:10000/default";

    /**
     * 平均150w个mid_event对象占用512m内存
     */
    private static final Integer CACHED_MAP_SIZE_LIMIT = 1500000;

    /**
     * 单个分区缓存对象列表大小
     */
    private static final Integer MAX_SINGE_CACHED_SZIE = CACHED_MAP_SIZE_LIMIT / 4;



    public static FileSystem getFileSystem() throws Exception {
        FileSystem fileSystem = FileSystem.get(getConf());
        return fileSystem;
    }

    /**
     * write to file system
     * @param eventList
     * @throws Exception
     */
    public static void write(String info, List<MidEvent> eventList)  throws Exception{
        String fileName = Long.toString(System.currentTimeMillis());
        String filePathStr = String.format(SAVE_PATH_TEMPLATE, info, fileName);
        Set<String> partitionSet = getHiveTablePartitions();
        String partition = "info=" + info;
        if (!partitionSet.contains(partition)){
            addTablePartition(info);
        }

        MessageType schema = MessageTypeParser.parseMessageType(schemaStr);
        Path out = new Path(filePathStr);
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(out)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(getConf())
                .withType(schema);
        ParquetWriter<Group> writer = builder.build();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        for (MidEvent midEvent: eventList){
            Group group = groupFactory.newGroup();
            group.append("packagename", midEvent.getAppName())
                 .append("teyeid", midEvent.getTeyeId())
                 .append("appversion", StringUtil.getIfNotEmpty(midEvent.getAppVersion(), StringUtil.NULL))
                 .append("basic_devicename", StringUtil.getIfNotEmpty(midEvent.getDeviceName(), StringUtil.NULL));
            writer.write(group);
        }
        writer.close();
        /*String filePath = "/user/hive/warehouse/test/part-00001";
        FileSystem fileSystem = getFileSystem();
        Path path = new Path(filePath);
        FSDataOutputStream fos = fileSystem.create(path);
        InputStream inputStream = new ByteArrayInputStream("abcdefghijklmn".getBytes("UTF-8"));
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        IOUtils.copyBytes(inputStream, fos, 4096, false);
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(fos);*/
    }

    public static Configuration getConf(){
        Configuration conf = new Configuration();
        System.setProperty("HADOOP_USER_NAME", "ubuntu");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return conf;
    }

    public static void addTablePartition(String partition) throws Exception{
        Connection connection = ConnectionUtil.getHiveConn(HIVE_URL, "root", "");
        String SQL = "alter table kylin_mobile_event add if not exists partition (info='%s')";
        Statement statement = connection.createStatement();
        statement.execute(String.format(SQL, partition));
    }

    public static Set<String> getHiveTablePartitions() throws Exception{
        Connection connection = ConnectionUtil.getHiveConn(HIVE_URL, "root", "");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show partitions kylin_mobile_event");
        Set<String> result = new HashSet<>();
        while (resultSet.next()){
            String partition = resultSet.getString(1);
            result.add(partition);
        }

        resultSet.close();
        statement.close();
        connection.close();
        return result;
    }

    public static void test() throws Exception{
        Connection connection = ConnectionUtil.getPrestoConn(PRESTO_URL);
        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(TEST_SQL);
        if (resultSet.next()){
            String date = resultSet.getString(1);
            System.out.println(date);
        }
        resultSet.close();
        stmt.close();
        connection.close();
    }

    public static void main(String[] args) throws Exception{

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));

        Long yesterdayStartTimestamp = TimeUtil.getYesterdayStartTimestamp();
        Long todayStartTimestamp = TimeUtil.getTodayStartTimestamp();

        if (args.length >= 1){
            String date = args[0];
            yesterdayStartTimestamp = TimeUtil.getTimestamp(date, DATE_FORMAT_YMD);
            todayStartTimestamp = yesterdayStartTimestamp + ONE_DAY_MILLISECOND;
        }

        if (args.length >= 2){
            test();
            return;
        }

        Map<String, List<MidEvent>> bufferMap = new HashMap<>();
        //========parameter prepare========
        String partition = TimeUtil.formatDateByTimestamp(yesterdayStartTimestamp, DATE_FORMAT_YMD);

        Long startTime = yesterdayStartTimestamp - ALLOW_MAX_DELAY;
        Long endTime = todayStartTimestamp + ONE_DAY_MILLISECOND;

        Connection connection = ConnectionUtil.getPrestoConn(PRESTO_URL);
        Statement stmt = connection.createStatement();
        String SQL = String.format(SQL_TEMPLATE, partition, startTime, endTime);
        System.out.println("SQL=========: " + SQL);

        ResultSet resultSet = stmt.executeQuery(SQL);

        int count = 0;

        while (resultSet.next())
        {

            if (count > CACHED_MAP_SIZE_LIMIT){
                count -= writeBufferedMap(bufferMap);
            }

            String appName = resultSet.getString(1);
            String teyeId = resultSet.getString(2);
            String appVersion = resultSet.getString(3);
            String deviceName = resultSet.getString(4);
            String eventTime = resultSet.getString(5);

            //cache data
            MidEvent midEvent = new MidEvent(appName, appVersion, teyeId, deviceName);
            if (bufferMap.containsKey(eventTime)){
                bufferMap.get(eventTime).add(midEvent);
            }else {
                List<MidEvent> midEventList = new ArrayList<>();
                midEventList.add(midEvent);
                bufferMap.put(eventTime, midEventList);
            }

            count++;
        }

        resultSet.close();
        stmt.close();


        Set<Map.Entry<String, List<MidEvent>>> entrySet = bufferMap.entrySet();
        Iterator<Map.Entry<String, List<MidEvent>>> iterator = entrySet.iterator();

        while (iterator.hasNext()){
            Map.Entry<String, List<MidEvent>> entry = iterator.next();
            String info = entry.getKey();
            List<MidEvent> eventList = entry.getValue();
            if (eventList.size() == 0){
                continue;
            }
            write(info, eventList);
        }

        // kylin refresh segment
        KylinRest.login();
        String cubeName = "app_active_count";
        CubeDes cubeDes = KylinRest.getCube(cubeName);

        List<Segment> segments = cubeDes.getSegments();

        Segment candidateSegment = null;

        for (int i = segments.size() - 1; i > 0; i--){
            Segment segment = segments.get(i);
            String status = segment.getStatus();
            if (KylinRest.SEGMENT_STATUS_REDAY.equalsIgnoreCase(status)){
                candidateSegment = segment;
                break;
            }
        }

        if (null != candidateSegment){
            KylinRest.refreshSegment(cubeName, candidateSegment.getDate_range_start(), candidateSegment.getDate_range_end());
        }
    }

    private static int writeBufferedMap(Map<String, List<MidEvent>> bufferMap) throws Exception{
        Set<String> candidates = new HashSet<>();
        Integer totalWriteNum = 0;
        String candidatePartition = null;
        Integer maxSize = 0;
        for (String info : bufferMap.keySet()){
            List<MidEvent> eventList = bufferMap.get(info);
            if (eventList.size() > maxSize){
                maxSize = eventList.size();
                candidatePartition = info;
            }
            if (eventList.size() > MAX_SINGE_CACHED_SZIE){
                candidates.add(info);
            }
        }
        candidates.add(candidatePartition);

        for (String info : candidates){
            List<MidEvent> midEventList = bufferMap.get(info);
            write(info, midEventList);
            totalWriteNum += midEventList.size();
            //after writing to fs, cached eventList should be cleared, otherwise java heap space would be consumed up
            midEventList.clear();
        }
        return totalWriteNum;
    }
}
