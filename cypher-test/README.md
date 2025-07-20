### Cypher 테스트 
## Cypher 란?
- Cypher는 Neo4j (Property Garph DB)를 위한 그래프 쿼리 언어
- SQL처럼 읽기 쉽고, 화살표(()-[]->)로 노드와 관계를 시각적으로 표현
- MATCH, CREATE, RETURN 같은 키워드를 사용해 패턴을 찾거나 생성
- 소셜 그래프, 추천 시스템, 경로 탐색 등에 적합

## Property Graph 란?
- Property Graph는 노드와 관계에 **속성(Property)**을 직접 붙일 수 있는 그래프 데이터 모델
- 노드와 관계는 이름뿐만 아니라 age=30, since=2020 같은 키-값 쌍의 속성을 가질 수 있음
- Neo4j가 대표적인 Property Graph DB
- 시각적으로 이해하기 쉽고, Cypher라는 쿼리 언어로 다룸

## 실행 방법
1. neo4j 도커 실행
```bash
 docker run -it --rm \
  -p7474:7474 -p7687:7687 \
  -e NEO4J_AUTH=neo4j/neo4jtest \
  neo4j:5
```

2. 테스트 실행 및 출력 확인
```bash
python3 test.py 
```

3. 다음 주소에서 neo4j UI 확인
```
http://localhost:7474
```
