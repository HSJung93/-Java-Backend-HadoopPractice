# 하둡 코드 저장소 
하둡 맵리듀스, 스파크, 카프카 실습 코드 저장소 입니다.
## 1. 하둡 맵리듀스
* 대용량 데이터를 분산 처리할 수 있는 자바 기반의 오픈소스 프레임워크.
* HDFS: 대규모 데이터를 관리하기 위한 분산 파일 시스템. 저장하고자 하는 파일을 블록 단위로 나누어 분산된 서버에 저장한다. 
* MapReduce: 대규모 데이터를 관리하기 위한 프로그래밍 모델, 소프트웨어 프레임워크. Map과 Reduce 함수를 이용하여 데이터를 처리한다. 클러스터에서 데이터를 읽고 동작을 실행한 뒤 결과를 기록하는 작업을 반복한다. 
* SK planet T아카데미에서 주관하고 2020.09.25에 열린 제 83차 토크ON 세미나 강의에 기반하였습니다. 
(https://www.youtube.com/watch?v=OPodJE1jYbg&list=PL9mhQYIlKEheGLT1V_PEby_I9pOXr1o3r)

### 환경 및 세팅
* java version "1.8.0_301"
* hadoop 3.3.0(hadoop 3.3.1로도 시도)

### 코드 및 사용법

* hadoop 버전에 맞게 pom.xml 파일의 dependency를 수정한다. 

* WordCountMapper 구현
  * LongWritable과 Text를 인풋으로 Text와 intWritable을 아웃풋으로 하는 Mapper를 상속 받아서 구현
  * while loop를 돌면서 키와 밸류를 만든다.

* WordCount 구현
  * 실제로 실행하는 main 클래스(드라이브 클래스)
  * hadoop 작업에 대한 설정: inputFormat, OutputFormat 등
  
* WordCountReducer 구현
  * reduce는 인풋으로 키와 밸류를 가짐.
  * 같은 키를 가지고 있는 인풋 끼리 더해준다.
  
* IntelliJ 의 install로 jar 파일 생성 후 HADOOP_HOME 디렉토리로 복사

* WordCount 실행
  * "hadoop jar hadoop_edu-1.0-SNAPSHOT.jar com.sk.hadoop.WordCount /input/LICENSE.txt output" 명령어를 사용하여 hadoop_edu-1.0-SNAPSHOT.jar 파일을 실행한다.
 
다음과 같은 로그와 함께 잠시 기다린다.
~~~
2021-07-27 11:49:51,258 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2021-07-27 11:49:51,866 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at /0.0.0.0:8032
2021-07-27 11:49:52,307 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
2021-07-27 11:49:52,323 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/mac/.staging/job_1627274989067_0004
2021-07-27 11:49:52,535 INFO input.FileInputFormat: Total input files to process : 1
2021-07-27 11:49:52,608 INFO mapreduce.JobSubmitter: number of splits:1
2021-07-27 11:49:52,731 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1627274989067_0004
2021-07-27 11:49:52,731 INFO mapreduce.JobSubmitter: Executing with tokens: []
2021-07-27 11:49:52,913 INFO conf.Configuration: resource-types.xml not found
2021-07-27 11:49:52,913 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2021-07-27 11:49:52,983 INFO impl.YarnClientImpl: Submitted application application_1627274989067_0004
2021-07-27 11:49:53,025 INFO mapreduce.Job: The url to track the job: http://macs-MacBook-Pro.local:8088/proxy/application_1627274989067_0004/
2021-07-27 11:49:53,025 INFO mapreduce.Job: Running job: job_1627274989067_0004
~~~

다음 오류 발생
~~~
오류: 기본 클래스 org.apache.hadoop.mapreduce.v2.app.mrappmaster을(를) 찾거나 로드할 수 없습니다.
~~~
  
* "hadoop dfs -text output/part-r-000000" 명령어로 실행 후 결과를 확인한다.
* "hadoop dfs -copyToLocal output/part-r-00000" 명령어로 로컬에 복사한 후 conditions 단어가 LICENSE.txt.와 part-r00000에 각각 4번씩 존재하는지 확인한다. 
  
## 2. 하둡 스파크
* 디스크 기반의 MapReduce의 느린 속도를 메모리 기반으로 작업하여 보완하는 인메모리 분석 프레임워크.
* MapReduce는 데이터를 단계별로 처리하고 실행하는 반면, 스파크는 전체 데이터 셋에서 데이터를 처리하기 때문에 맵리듀스-하둡보다 빠르다. 
* SK planet T아카데미에서 주관하고 2020.12.29에 열린 토크ON 세미나 강의에 기반하였습니다. (https://www.youtube.com/watch?v=tw7bzfWS7cM&t=98s)

### 환경 및 세팅
* 도커 zepplin 0.8.1 으로 실습.
* zepplin 별로 스파크를 지원한다. 0.8.1의 경우 6.2를 지원한다.
```
docker run -p 4040:4040 -p 8080:8080 --privileged=true -v $PWD/logs:/logs -v $PWD/notebook:/notebook -e ZEPPELIN_NOTEBOOK_DIR='/notebook' -e ZEPPELIN_LOG_DIR='/logs' -d apache/zeppelin:0.8.1 /zeppelin/bin/zeppelin.sh
```
* `docker ps` 명령어로 제대로 구동되었는지 container를 확인하거나, localhost:8080 포트로 들어가서 확인한다.
* 도커가 종료되면 log도 사라지기 때문에, logs 디렉토리를 따로 만든다.

### RDD
* RDD(Resilient Distributed Datasets, 탄력적 분산 데이터셋)는 데이터 컨테이너이다. 
* Spark은 RDD 기반으로 데이터를 조작한다.
* 데이터를 transformation(Map 단계)하면 지속적으로 새로운 id가 생긴다. 
* Fault Tolerance하다. 맵리듀스는 데이터를 읽고 쓰는 일이 잦고 에러가 잦다. 에러가 나면 맵리듀스는 데이터를 다시 읽는다. 반면 Spark는 lineage id를 가지고 있다. 문제가 생긴 transform 전 단계부터 작업을 재개한다. 

### Scala vs Java, Python
* JVM 위에서 돌아가기 때문에 자바로도 가능하지만, 함수형 프로그래밍에 적합한 Scala로 코드 경량화가 가능하다. 
* 지금은 Python API가 많아졌고, 분석과 연동이 쉬워 Python을 유지하는 것도 좋은 선택이다. 

### Operations
* Transformation: Map, filter, groupBy, join 등 RDD로부터 RDD를 만드는 Lazy 작업. Spark는 이 시점에서는 일을 하지 않는다. 최적의 DAG(실행되는 프로세스를 보여준다)를 액션 시점에서 찾기 위해서 실행된다. 
  * Narrow: map과 같이 순서를 유지. vs Wide: groubBykey와 같이 Shuffling 순서를 바꾼다. 
* Actions: count, collection, save 등 결과를 리턴하거나 저장소나 HDFS에 쓰는 작업


## 3. 하둡 카프카