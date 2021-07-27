# 하둡 맵리듀스 코드 저장소 

## 프로젝트 설명
하둡 맵리듀스 실습 코드 저장소 입니다. SK planet T아카데미에서 주관하고 2020.09.25에 열린 제 83차 토크ON 세미나 강의에 기반하였습니다. 
(https://www.youtube.com/watch?v=OPodJE1jYbg&list=PL9mhQYIlKEheGLT1V_PEby_I9pOXr1o3r)

## 환경 및 세팅
* java version "1.8.0_301"
* hadoop 3.3.0(hadoop 3.3.1로도 시도)

## 코드 및 사용법

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
  
