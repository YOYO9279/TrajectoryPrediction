### original to ods 洗数据+去重
```hiveql
insert into table ods_track partition (dt)
select map_longitude, map_latitude, gps_time, gps_speed, direction, event, alarm_code, gps_longitude, gps_latitude, altitude, tachometer_speed, miles, error_type, access_code, receiving_time, color, province, car_number,dt
from (select cast(map_longitude / 600000 as double)          as map_longitude,
             cast(map_latitude / 600000 as double)           as map_latitude,
             unix_timestamp(
                     concat(concat_ws('-', substr(gps_time, 1, 4), substr(gps_time, 5, 2), substr(gps_time, 7, 2)), ' ',
                            concat_ws(':', substr(gps_time, 10, 2), substr(gps_time, 12, 2),
                                      substr(gps_time, 14, 2))))     as gps_time,
             gps_speed,
             direction,
             event,
             nvl(alarm_code, 0) as alarm_code,
             cast(gps_longitude / 600000 as double)          as gps_longitude,
             cast(gps_latitude / 600000 as double)           as gps_latitude,
             altitude,
             tachometer_speed,
             miles,
             error_type,
             access_code,
             unix_timestamp(concat(
                     concat_ws('-', substr(receiving_time, 1, 4), substr(receiving_time, 5, 2),
                               substr(receiving_time, 7, 2)),
                     ' ', concat_ws(':', substr(receiving_time, 10, 2), substr(receiving_time, 12, 2),
                                    substr(receiving_time, 14, 2)))) as receiving_time,
             color,
             province,
             car_number,
             dt,
             row_number() over (partition by car_number,map_longitude,map_latitude order by  gps_time) as rk
      from spark.ori2) a
where rk = 1 ;
```

### track数据
```hiveql
CREATE TABLE ods_track
(
    map_longitude    Double,
    map_latitude     Double,
    gps_time         BIGINT,
    gps_speed        int,
    direction        int,
    event            int,
    alarm_code       int,
    gps_longitude    Double,
    gps_latitude     Double,
    altitude         int,
    tachometer_speed int,
    miles            int,
    error_type       int,
    access_code      int,
    receiving_time   BIGINT,
    color            int,
    province         String,
    car_number       String
)
partitioned by (dt STRING)
row format delimited fields terminated by ':';


```



### coor建表语句

```clickhouse
drop table transfer
create table transfer
(
    from_id Float64,
    to_id   Float64,
    cnt     Float64
) engine = SummingMergeTree(cnt)
      order by (from_id, to_id);
```

### ADJ建表语句
```clickhouse

drop table adjacent
create table adjacent
(
    cur  Float64,
    next Float64
) engine = ReplacingMergeTree()
      order by (cur, next)

```

### ck原表数据
```clickhouse

create table kafka_ck_source_track
(
    map_longitude    Float64,
    map_latitude     Float64,
    gps_time         Int64,
    gps_speed        Int8,
    direction        Int8,
    event            Int8,
    alarm_code       Int8,
    gps_longitude    Float64,
    gps_latitude     Float64,
    altitude         Int8,
    tachometer_speed Int8,
    miles            Int8,
    error_type       Int8,
    access_code      Int8,
    receiving_time   Int64,
    color            Int8,
    province         String,
    car_number       String
) engine = Kafka SETTINGS kafka_broker_list = 'node01:9092,node02:9092,node03:9092',
    kafka_topic_list = 'kafka_track_test01',
    kafka_group_name = 'group2',
    kafka_format = 'CSV',
    kafka_num_consumers = 4;
```

### ck 物化视图
```clickhouse
CREATE MATERIALIZED VIEW kafka_ck_mid_track TO kafka_ck_sink_track
    AS SELECT map_longitude, map_latitude, fromUnixTimestamp(gps_time) as gps_time, gps_speed, direction, event, alarm_code, gps_longitude, gps_latitude, altitude, tachometer_speed, miles, error_type, access_code, fromUnixTimestamp(receiving_time) as receiving_time, color, province, car_number
    FROM kafka_ck_source_track;
```