### coor建表语句

```sql
create table spark.xxx
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

create table spark.area02_adj
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
    isAdj           text   null
);


```





```