### original 转换语句
```sql


insert overwrite table track partition (dt = '2018-10-08')
select cast(map_longitude / 600000 as decimal(10, 8)) as map_log,
       cast(map_latitude / 600000 as decimal(10, 8))  as map_la,
       unix_timestamp(
               concat(concat_ws('-', substr(gps_time, 1, 4), substr(gps_time, 5, 2), substr(gps_time, 7, 2)), ' ',
                      concat_ws(':', substr(gps_time, 10, 2), substr(gps_time, 12, 2), substr(gps_time, 14, 2)))),
       gps_speed,
       direction,
       event,
       alarm_code,
       cast(gps_longitude / 600000 as decimal(10, 8)),
       cast(gps_latitude / 600000 as decimal(10, 8)),
       altitude,
       tachometer_speed,
       miles,
       error_type,
       access_code,
       unix_timestamp(concat(
               concat_ws('-', substr(receiving_time, 1, 4), substr(receiving_time, 5, 2), substr(receiving_time, 7, 2)),
               ' ', concat_ws(':', substr(receiving_time, 10, 2), substr(receiving_time, 12, 2),
                              substr(receiving_time, 14, 2)))),
       color,
       province,
       car_number
from original;
```

### track数据
```sql

CREATE TABLE track
(
    map_longitude       Double,
    map_latitude        Double,
    GPS_time            bigint,
    GPS_speed           String,
    direction           String,
    event               String,
    alarm_code          String,
    GPS_longitude       Double,
    GPS_latitude        Double,
    altitude            String,
    tachometer_speed    String,
    miles               String,
    error_type          String,
    access_code         String,
    receiving_time      bigint,
    color               String,
    province            String,
    car_number          String,
    dt                  String
)
row format delimited fields terminated by ',';
```



### coor建表语句

```sql
create table spark.coor_01
(
    id            int auto_increment
        primary key,
    map_longitude double null,
    map_latitude  double null,
    gps_longitude double null,
    gps_latitude  double null,
    constraint `map_longitude_map_latitude_uindex`
        unique (map_longitude, map_latitude)
);
```

### ADJ建表语句
```sql

create table spark.adj_02
(
    a_id            bigint null,
    a_map_longitude double null,
    a_map_latitude  double null,
    a_gps_longitude double null,
    a_gps_latitude  double null,
    b_id            bigint null,
    b_map_longitude double null,
    b_map_latitude  double null,
    b_gps_longitude double null,
    b_gps_latitude  double null,
    isAdj           text   null,
    constraint adj_uindex
        unique (a_id, b_id)
);



```





```