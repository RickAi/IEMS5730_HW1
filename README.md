## a. You need to design one or more MapReduce job(s) to perform the matrix multiplication described above in the small dataset

Commands:

```bash
# compile source code and generate jar
mvn compile
mvn install

# download and prepare data
hadoop dfs -mkdir -p hw1/input
hadoop dfs -mkdir -p hw1/tmp
hadoop dfs -mkdir -p hw1/output

hadoop dfs -copyFromLocal *.dat hw1/input

# execute job
hadoop jar ybai-1.0-SNAPSHOT.jar Main hw1/input/M_small.dat hw1/input/N_small.dat hw1/tmp/small hw1/output/small
```

Results:

```

```

## b. Perform the matrix multiplication in the median dataset[2] and large dataset[3].

Commands:

```bash
# median dataset
hadoop jar ybai-1.0-SNAPSHOT.jar Main hw1/input/M_median.dat hw1/input/N_median.dat hw1/tmp/median hw1/output/median

# large dataset
hadoop jar ybai-1.0-SNAPSHOT.jar Main hw1/input/M_large.dat hw1/input/N_large.dat hw1/tmp/large hw1/output/large
```

Results:

## c. Re-run your program in b) multiple times using different number of mappers and reducers for your MapReduce job each time. You need to examine and report the run-time performance statistics of your program for at least 4 different combinations of number of mappers and reducers.

|Mapper Count|Reducer Count|Mapper Time Cost|Reducer Time Cost|Total Time Cost|
|---|---|---|---|---|
|1|1||||
|10|1||||
|1|10||||
|10|10||||
|20|20||||

|Maximum mapper time|Minimum mapper time|Averager mapper time|Maximum reducer time|Minimum reducer time|Average reducer time|Total job time|
|---|---|---|---|---|---|---|
||||||||

## Notes

### Immediate data size

|Type|Immediate Data Size(MB)|
|---|---|
|Small|8.45|
|Median|874.59|
|Large|6995.32|



