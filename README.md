
```
sbt -J-Xmx5g run
```

Latest execution on 2016-09-06, with 20 000 000 rows :
```
parquet write duration : 26s   => 194Mb
file write duration : 17s      => 677Mb
root
 |-- value: long (nullable = true)
 |-- square: long (nullable = true)
 |-- comment: string (nullable = true)

count=20000000
parquet read duration : 0s
count=20000000
file read duration : 11s
```
