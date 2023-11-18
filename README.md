# hadoop_apriori_mapred

이 프로젝트는 Hadoop MapReduce를 이용하여 Apriori 알고리즘을 구현한 것입니다. Apriori 알고리즘은 트랜잭션 데이터에서 빈번하게 발생하는 아이템셋을 찾는 데 사용되는 알고리즘입니다.

## Prerequisites

- Eclipse IDE with Maven
- Hadoop 3.3.1

## Usage

다음은 이 프로젝트를 사용하는 방법에 대한 설명입니다.

1. **jar 파일 실행**
   
   다음과 같은 형식으로 jar 파일을 실행합니다.
   
   ```
   yarn jar {jar파일경로} {MainClass} {입력데이터경로} {출력경로} {minsup}
   ```
   
   - `{jar파일경로}`: 실행할 jar 파일의 경로를 입력합니다.
   - `{MainClass}`: 실행할 메인 클래스. 이 프로젝트에서는 `cse.AprioriMR`를 입력합니다.
   - `{입력데이터경로}`: 입력 데이터가 저장된 HDFS 경로를 입력합니다.
   - `{출력경로}`: 결과를 저장할 HDFS 경로를 입력합니다.
   - `{minsup}`: 빈발 아이템셋으로 고려할 최소 지지도를 입력합니다.

   예시:
   
   ```
   yarn jar cse-0.0.1-SNAPSHOT.jar cse.AprioriMR /user/hadoop/test /user/hadoop/test_output 3
   ```

2. **결과 확인**
   
   `{출력경로}`에는 각 빈발집합 계산 사이클마다 생성된 결과가 저장됩니다. 예를 들어, `test_output/1`, `test_output/2` 등의 디렉토리가 생성됩니다.
   
   모든 계산이 완료된 후, 최종적인 빈발집합 결과는 `test_output/final_result`에 저장됩니다.
   
   결과를 확인하려면 다음 명령을 사용합니다.
   
   ```
   hdfs dfs -cat /user/hadoop/test_output/final_result
   ```

