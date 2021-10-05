# 하둡 코드 저장소 
* 하둡이란? 대량의 자료를 처리할 수 있는 큰 컴퓨터 클러스터에서 동작하는 분산 응용 프로그램을 지원하는 프리웨어 자바 소프트웨어 프레임워크이다. 
* 이 저장소에서는 카프카, 맵리듀스, 스파크의 코드를 실습하고 모아보았습니다. 
* 카프카: 실시간 처리를 위한 분산 메시징 시스템
  * SK planet T아카데미에서 주관하고 2020.07.10에 열린 토크ON 77차 세미나 강의에 기반하였습니다. (https://www.youtube.com/watch?v=VJKZvOASvUA&list=PL9mhQYIlKEheZvqoJj_PkYGA2hhBhgha8)
* 맵리듀스: 하둡 클라스터의 데이터를 처리하기 위한 시스템으로 맵과 리듀스로 구성된다.
  * SK planet T아카데미에서 주관하고 2020.09.25에 열린 제 83차 토크ON 세미나 강의에 기반하였습니다. (https://www.youtube.com/watch?v=OPodJE1jYbg&list=PL9mhQYIlKEheGLT1V_PEby_I9pOXr1o3r)
* 스파크: 오픈 소스 분산 클러스터 컴퓨팅 프레임워크
  * SK planet T아카데미에서 주관하고 2020.12.29에 열린 토크ON 세미나 강의에 기반하였습니다. (https://www.youtube.com/watch?v=tw7bzfWS7cM&t=98s)

## 카프카
### 개요
* 데이터를 보내는 소스 어플리케이션과 데이터를 받는 타겟 어플리케이션이 많아지면, 전송 라인이 늘어나 배포 장애에 대응하기 어려워진다.
* 전송 시에도 프로토콜과 포맷의 파편화가 심해진다. 
* 소스 어플리케이션은 카프카에 데이터를 보내고, 타겟 어플리케이션은 카프카로부터 데이터를 받는다. 
* 카프카 안에는 토픽이라는 큐가 있어 프로듀서가 데이터를 넣고 컨슈머가 데이터를 가져간다. 
* 토픽 안의 파티션에 데이터를 저장한다. 
* 프로듀서와 컨슈머는 라이브러리로 되어 있어서 어플리케이션에서 구현이 가능하다. 
* fault-tolerant. 고가용성. 손실없는 데이터 복구에 용이하다
* 낮은 지연과 높은 처리량으로 빅데이터를 처리한다. 
### 토픽
* 일반적인 AMQP와는 다르게 동작
* 0으로 시작하는 여러 파티션으로 구성되어 있다.
* 하나의 파티션은 큐처럼 끝부터 쌓이게 된다. 
* 컨슈머는 데이터를 오래된 순서대로 가져간다. 이 때 더이상 데이터가 없으면 기다리고, 데이터를 가져가더라도 삭제되지 않는다. 
  * 다른 컨슈머 그룹이 auto.offset.reset = earliest일 때 가져갈 수 있다. 
  * 일정한 시간과 크기에 삭제된다. 
* 파티션이 두 개 이상인 경우, 기본 파티셔너 인데 데이터가 들어오면,
  * 키가 null일 경우: 라운드 로빈 방식
  * 키가 있을 경우: 키의 해시값을 구하고 특정 파티션에 할당한다. 
* 파티션을 늘릴 수는 있지만 줄일 수는 없다!

### 프로듀서
* 프로듀서의 역할
  1. 토픽에 해당하는 메시지를 생성한다.
  2. 특정 토픽으로 데이터를 publish한다.
  3. 카프카 브로커로 데이터를 전송할 때, 전송 처리 여부를 파악하고 실패할 경우 재시도한다.
* 자바 프로퍼티로 프로듀서 옵션을 설정한다.
  * 부트스트랩 서버 설정을 로컬호스트의 카프카 9092 포트를 바라보도록 설정한다. 
    * 카프카의 포트는 2개 이상의 아이디와 포트들을 배정할 것을 권장된다. 비정상인 브로커가 발생했을 때를 대비
  * 시리얼라이저는 키와 밸류를 직렬화한다. Byte array, String, Integer 시리얼라이즈를 사용할 수 있다. 
    * 키는 메세지를 보내면 토픽의 파티션이 지정될 때 사용된다. 키를 해쉬화 하여 파티션과 1대1 매칭한다. 
* 카프카 프로듀서 인스턴스를 만든다.
* 카프카 클라이언트에서 제공하는 ProducerRecord 클래스로 객체를 작성한다. 키와 밸류를 적거나 적지 않으면 맞춰서 오버로딩된다.
* send 메소드에 객체를 넣으면 전송된다.

### 컨슈머
* 카프카는 다른 데이터 메시징 시스템들과는 달리, 컨슈머가 데이터를 가져가도 데이터가 사라지지 않는다. 
* 컨슈머의 역할
  1. 폴링: 컨슈머가 토픽 속 파티션에서 데이터를 가져오는 것
  2. 파티션의 offset 위치 기록(커밋). 오프셋은 파티션에 있는 데이터의 번호이다.
  3. 컨슈머 그룹을 통한 병렬 처리
* 자바 라이브러리를 지원한다. 카프카 브로커와의 버전 호환성을 확인해야 한다. 
* 자바 프로퍼티로 컨슈머 옵션을 지정한다. 
  * bootstrap servers 옵션으로 카프카 브로커를 설정한다. 
  * 마찬가지로 키와 밸류에 대한 직렬화 설정을 한다.
* KafkaConsumer 클래스로 만든 객체에 subscribe 메소드로 대상 토픽을 전달한다. 
  * assign을 사용하면 특정 토픽의 일부 파티션의 데이터만 가져온다. key가 있는 데이터의 순서를 지킬 때 사용한다. 
* 컨슈머 API의 핵심 로직인 폴링 루프로 데이터를 가져온다. 
  * records 변수는 데이터 배치로서 레코드의 묶음 list이다. 
  * value() 메소드로 변환한다. 
* offset : 파티션 내의 데이터의 고유 번호
  * 중지가 되더라도 처리 시점을 복구할 수 있다. 
* 컨슈머의 개수는 파티션의 개수보다 적거나 같아야 한다. 
* 단 컨슈머 그룹이 다르면, 서로 영향을 미치지 않는다. offset이 컨슈머 그룹별로 토픽 별로 저장되기 때문이다.
## 맵리듀스
### 개요
* 대용량 데이터를 분산 처리할 수 있는 자바 기반의 오픈소스 프레임워크.
* HDFS: 대규모 데이터를 관리하기 위한 분산 파일 시스템. 저장하고자 하는 파일을 블록 단위로 나누어 분산된 서버에 저장한다. 
* MapReduce: 대규모 데이터를 관리하기 위한 프로그래밍 모델, 소프트웨어 프레임워크. Map과 Reduce 함수를 이용하여 데이터를 처리한다. 클러스터에서 데이터를 읽고 동작을 실행한 뒤 결과를 기록하는 작업을 반복한다. 

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
  
## 스파크
### 개요
* 디스크 기반의 MapReduce의 느린 속도를 메모리 기반으로 작업하여 보완하는 인메모리 분석 프레임워크.
* MapReduce는 데이터를 단계별로 처리하고 실행하는 반면, 스파크는 전체 데이터 셋에서 데이터를 처리하기 때문에 맵리듀스(하둡)보다 빠르다. 

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

### DF VS Dataset
* Dataframe
  * JVM 위에서 돌아가기 때문에 자바로도 가능하지만, 함수형 프로그래밍에 적합한 Scala로 코드 경량화가 가능하다. 
  * 지금은 Python API가 많아졌고, 분석과 연동이 쉬워 Python을 유지하는 것도 좋은 선택이다. 
* Dataset
  * Dataset 같은 경우 JVM 위에서 돌아가는 자바와 Scala에서만 쓸 수 있다
  * 자바 빈 객체를 통하여 dataset을 정의한다.  
  * dataset의 row 기반 rowtype이 dataframe이다. 
  * dataframe은 다양한 언어를 지원하지만 dataset은 JVM 기반만 제공한다.
  * job이 run되었을때가 아니라 compile에 error를 미리 잡을 수 있다. 
  * df만으로는 어렵고 커스터마이즈가 필요할 때, 방어코드를 위해서 타입 안정성있는 데이터를 처리하고 싶을 때 사용한다. 

### Operations
* Transformation: Map, filter, groupBy, join 등 RDD로부터 RDD를 만드는 Lazy 작업. Spark는 이 시점에서는 일을 하지 않는다. 최적의 DAG(실행되는 프로세스를 보여준다)를 액션 시점에서 찾기 위해서 실행된다. 
  * Narrow: map과 같이 순서를 유지. vs Wide: groubBykey와 같이 Shuffling 순서를 바꾼다. 
* Actions: count, collection, save 등 결과를 리턴하거나 저장소나 HDFS에 쓰는 작업
* 분산 인메모리나 디스크에서 올리고 싶으면 RDD에서는 persist, dataFrame은 cache 사용.

### Job
* RDD를 계산하기 위해서 필요한 작업
* Job에는 하나 이상의 파이프라인화된 RDD가 존재하게 되고 이를 Stage라고 한다.
* 각각의 Stage는 그 안에 task-unit을 가진다. 
* Stage간 데이터의 이동을 Shuffle이라고 한다. 

### Structured API Execution
* SQL, DataFrames, Datasets 등 실행 계획들을 Spark 내부적으로 최적화를 한다.
* Catalyst Optimizer가 물리적인 실행계획을 관장해서 최적화를 해준다. 
* 따라서 JBDC, Scala, SparkShell, Python, R 등을 사용해도 동일한 결과를 가져다 준다.
* 최적화 시에 SQL과 같이 쿼리를 날리지 말고, 파티션을 나누어서 검색하는 것이 필요하다.
* 가끔 하이브에서 잘 돌아가던 것이 최적화의 결과 스파크에서는 돌아가지 않는 경우가 존재한다. 