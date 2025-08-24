# 내용 정리

## 1) 복제의 목적

- **가용성(Availability)**  
  - 한 노드가 장애여도 다른 복제본이 요청을 처리해 **서비스 중단을 최소화**.
  - 계획/비계획 정지(업그레이드, 재시작, 하드 장애) 동안 **연속 운영** 가능.
- **지연 단축(Latency)**  
  - 사용자에게 가까운 리전/데이터센터(Edge, PoP)에 복제본을 두어 **왕복 시간 단축**.
  - 특히 **읽기 지연**을 크게 줄일 수 있음.
- **읽기 확장(Read scale-out)**  
  - 동일 데이터에 대한 읽기 부하를 여러 복제본에 분산해 **처리량 증가**.
- (부가) **내구성(Durability)**  
  - 단일 장비/디스크 실패로부터 **데이터 유실** 위험을 낮춤(단, 동기/비동기에 따라 RPO가 달라짐).
  - RPO(Recovery Point Objective): 얼마나 최근 시점까지의 데이터가 보존되어야 하는가를 시간으로 정의한 목표치 

---

## 2) 단일 리더 복제 (Single-Leader / Primary–Replica)

### 구조와 기본 동작
- 하나의 **리더(primary)** 가 **모든 쓰기**를 수용(직접 라우팅 or 프록시).
- 여러 **팔로워(replica)** 가 리더의 **변경 로그(change log)** 를 받아 **동일한 순서**로 적용.
- **읽기**는 리더 또는 팔로워에서 수행(읽기 스케일아웃).  
  - UX 요건(예: 내 글 수정 직후 바로 반영) 따라 **리더 고정**이 필요할 수도 있음.

### 리더→팔로워 전파 방식(복제 로그 구현)
1) **문장 기반(Statement-based)**  
   - 실행된 SQL/명령문을 텍스트로 전송해 팔로워가 **재실행**.  
   - **문제점**:  
     - 비결정 함수(NOW(), RAND())가 **각 노드에서 다르게 평가**될 수 있음.  
     - 트리거/스토어드 프로시저/부작용이 **다중 반영**될 가능성.  
     - `UPDATE ... WHERE some_condition` 처럼 **정확히 같은 행 집합**이 보장되지 않을 수 있음.
2) **물리 로그 전달(WAL / Log Shipping)**  
   - 저장소 레벨의 **페이지/블록 변경**을 그대로 복사.  
   - **장점**: 빠르고, 리더의 상태를 바이트 수준으로 **충실히 복제**.  
   - **단점**: **엔진/버전 결합**이 강함(이기종 복제/테이블 부분 복제/필터링 어려움). 버전 업그레이드 시 호환성 이슈.
3) **로지컬(논리)/행 기반(Logical/Row-based)**  
   - “키 X에 INSERT/UPDATE/DELETE” 같은 **레코드 수준 이벤트**를 전송.  
   - **장점**: 스키마 진화/다운스트림 처리(CDC)/이기종 시스템 연동에 **유연**.  
   - **구현 팁**: 열 기반 인코딩, 변경 컬럼만 전송, 스키마 버전 태깅 등.
4) **트리거 기반(앱/DB 레벨 캡처)**  
   - 트리거나 애플리케이션 레벨에서 **변경을 캡처**해 이벤트 스트림으로 발행.  
   - **장점**: 유연(필터링/변환 가능).  
   - **단점**: 성능/운영 복잡도↑, 장애/재처리 시 **정확히 한 번** 의미론 확보가 어려움.

### 팔로워 초기화 및 재동기화
- **일관된 스냅샷** 생성 → 스냅샷 생성 시점의 **로그 위치(LSN 등)** 를 기록.  
- 이후 **스냅샷 시점 이후의 변경 로그**를 순서대로 재생해 **최신화**.  
- 장애 복귀 팔로워는 자신의 **마지막 적용 위치** 이후만 재적용(빠른 복구).

### 동기 vs 비동기 복제
- **동기(Synchronous)**  
  - 리더 커밋 성공을 **하나 이상 팔로워의 ACK** 이후에만 반환.  
  - **장점**: 내구성/일관성↑, 리더 장애 시 **데이터 손실 가능성↓**.  
  - **단점**: 네트워크 지연/팔로워 상태에 **민감**(느려지거나, 팔로워 고장 시 **쓰기 중단** 위험).  
  - **현실적 제약**: 리전 간 동기는 지연/불안정으로 **거의 불가**.
- **비동기(Asynchronous)**  
  - 리더가 먼저 커밋 성공 반환 → 나중에 팔로워로 전파.  
  - **장점**: 지연↓, 처리량↑, 쓰기 가용성↑.  
  - **단점**: 리더가 즉시 장애 나면 **최근 커밋 유실(RPO>0)** 위험.
- **절충(Semi-sync 등)**  
  - “최소 1개 노드는 동기, 나머지는 비동기” 같은 **타협안**.  
  - RPO·지연·가용성 사이의 **정책적 균형**을 맞춤.

### 리더 장애와 Failover
- **감지**: 타임아웃/헬스체크/관측 기반으로 리더 다운 판단(망 분할과의 구분이 어려움).  
- **승격**: 적합한 팔로워를 **새 리더로 승격**.  
- **재구성**: 나머지 팔로워의 **추종 대상을 갱신**, 클라이언트/라우팅을 새 리더로 전환.  
- **함정/주의**:  
  - **스플릿 브레인(split brain)**: 옛 리더가 살아났는데 **새 리더와 동시에** 쓰기를 받는 상황 → **이중 커밋/충돌**.  
  - **데이터 손실**: 비동기 지연 중이던 커밋이 **사라질 수 있음**(정본 결정 필요).  
  - **ID/순서 문제**: 자동 증가/시퀀스/타임스탬프가 **역행/중복**될 수 있음(특히 재선출 직후).  
  - 운영적으로는 **자동 전환의 보수적 기준**(충분히 긴 타임아웃, 재시도)과 **재진입 금지 장치**가 필요.

---

## 3) 복제 지연과 읽기 일관성 (팔로워 읽기)

### 문제: Stale Read(오래된 읽기)
- 팔로워는 리더보다 **뒤처질 수** 있어, 방금 쓴 데이터를 **못 보거나 이전 값**을 볼 수 있음.
- 읽기 확장(팔로워 읽기)의 이점을 얻는 대신, **사용자 체감 일관성** 문제가 발생.

### 세션 보장(Session Guarantees)
1) **자신이 쓴 내용 읽기 (RYW)** — *내가 쓴 건 내가 즉시 본다*  
   - 예: 프로필 수정 직후 **내 화면**에는 반드시 최신 값이 보여야 함.  
   - **전략**:  
     - 해당 사용자/키의 읽기는 **일정 기간 리더 고정**(sticky to leader).  
     - 세션이 **마지막 쓰기 타임스탬프/LSN** 을 기억 → 그 **이후까지 적용된 복제본**에서만 읽기 허용.  
     - 캐시/리버스 프록시 레벨에서도 **버전/타임스탬프 힌트**로 우회 가능.
2) **단조 읽기** — *같은 사용자가 시간 역행하지 않기*  
   - 연속 읽기에서 이전보다 **오래된 상태**가 보이면 안 됨.  
   - **전략**:  
     - 같은 세션은 **같은 복제본**(혹은 리더)에 라우팅(세션 고정).  
     - 세션이 ‘마지막으로 본 **최대 LSN**’을 기억 → 그보다 **뒤처진 복제본**으로 라우팅하지 않기.
3) **일관된 순서로 읽기** — *원인→결과 순서 유지*  
   - 예: “댓글이 본문보다 먼저 보이는” 모순 방지.  
   - **전략**:  
     - **관련 쓰기**(원인과 결과)가 **같은 리더/파티션**을 통하도록 키 설계.  
     - **총순서 로그**(단일 로그)에 의존하거나, 멀티 파티션 트랜잭션으로 **순서 보장**.

---

## 4) 다중 리더 복제 (Multi-Leader / Multi-Primary)

### 언제 유용한가
- **다중 리전/데이터센터**: 각 지역에서 **로컬 쓰기** 허용으로 지연↓, **리전 장애**에도 지속 운영.  
  - 리더 간 복제는 현실적으로 **비동기**(WAN 지연/망 품질).
- **오프라인/모바일 동기화**: 클라이언트/지점이 오프라인 동안 **로컬 쓰기**, 나중에 **센터와 병합**.  

### 핵심 이슈: 쓰기 충돌(Conflict)
- 서로 다른 리더에서 **동일 레코드 동시 쓰기**가 가능 .
- **해결 전략**:  
  - **LWW(Last-Write-Wins)**: 가장 최신 타임스탬프 승자. **간단**하지만 **업데이트 소실** 위험.  
  - **도메인 병합 규칙**:  
    - 카운터: **합산**(증가/감소 반영)  
    - 집합: **합집합/차집합** 정책  
    - 문서: **필드별 병합**(예: 더 최근 수정 필드만 채택)  
    - 예약/좌석/재고: **명시적 제약/재시도/보상 트랜잭션**  
  - **충돌 회피 설계**: 동일 키의 쓰기가 **항상 같은 리더**로 향하도록 **라우팅/파티셔닝**(테넌트/지역 고정).

### 토폴로지와 루프 방지
- **원형(ring)**, **전체 연결(all-to-all)**, **별 모양** 등 토폴로지에 따라 **지연/복원력/운영 난이도**가 달라짐.
- **복제 루프**(A→B→C→A로 같은 이벤트 재적용) 방지:  
  - 변경 이벤트에 **고유 ID**와 **원산지(origin/DC)** 태그를 넣어 **중복 적용 차단**.  
  - 재전파 시 “이미 처리함” 판별 필요.

---

## 5) 리더리스 복제 (Leaderless, Dynamo 계열)

### 기본 아이디어: 정족수 (N, R, W)
- 각 키를 **N개의 복제본**에 저장.  
- **쓰기 성공 조건**: N 중 **W개**의 ACK 수신.  
- **읽기 전략**: N 중 **R개**에서 값을 가져와 **가장 최신 버전**을 선택.  
- **R + W > N** 이면 최신 쓰기와 읽기가 **최소 한 노드에서 겹친다** → 어느 정도 일관성 확보.  
  - 예: N=3, W=2, R=2.

### 내결함성 기법
- **느슨한 정족수**  
  - 지정된 N개의 “정식” 복제본이 아니라도 **가용한 임시 노드**로 W/R을 충족해 **가용성 우선**.  
  - 쿼럼이 **원래 집합**과 어긋나 **일관성 약화** 가능.
  - 추후 가용 임시 노드에서 복구된 노드로 암시된 핸드오프(일시적으로 수용한 쓰기를 전송) 진행
- **읽기 복구(Read Repair)**  
  - 읽을 때 R개에서 받은 버전을 비교해 **뒤처진 복제본을 동기화**(백그라운드 라그 해소).  
- **안티-엔트로피(Anti-Entropy)**  
  - 배치로 전체 키공간을 비교/동기화. **머클 트리** 등으로 **차이만 효율적 동기화**.

### 동시 쓰기에 대한 대응
- **멀티버전 보관**(siblings)  
  - 서로 **동시성**인 업데이트는 **둘 다 저장** — 나중에 선택/병합.  
- **버전 벡터**  
  - 업데이트 간 **선후관계 vs 동시성**을 판별하는 메타데이터.  
- **병합은 애플리케이션 책임**  
  - 카운터 **합산**, 집합 **합집합/차집합**, 문서 **필드별 정책** 등 **도메인 규칙**으로 최종 상태 확정.  
  - 단순 **LWW**(특히 **클라이언트 타임스탬프**)는 **시계 불일치**로 최신 업데이트를 **덮어쓸 위험**.

# 실습 해보기
## 1. 의존성 설치 (루트에서)
```shell
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```
## 2. 클러스터 띄우기
```shell
cd replication

# MongoDB (리더 기반)
docker compose up -d mongo1 mongo2 mongo3 mongosetup

# Cassandra (리더리스)
docker compose up -d cassandra1 cassandra2 cassandra3

# CouchDB (다중 리더)
docker compose up -d couch1 couch2
```
- MongoDB: 아래 명령어로 PRIMARY/SECONDARY 상태가 보이는지 확인
    ```shell
    docker compose exec mongo1 mongosh --quiet --eval 'rs.status().members.map(m => m.name + " " + m.stateStr)'
    ```
- Cassandra: nodetool status에서 노드가 UN(Up/Normal)이 되는지 확인 (처음엔 1분쯤 걸릴 수 있음)
    ```shell
    docker compose exec cassandra1 nodetool status
    ```
- CouchDB: 브라우저에서 http://admin:pass@localhost:5984/ 접속하면 환영 JSON 노출


## 3. 벤치
### MongoDB (리더 기반)
1. 빠른 응답 우선: 팔로워에서 읽고, 쓰기는 가볍게 확인
```shell
docker run --rm -it --network replication_default -v "$PWD":/work -w /work python:3.11 bash -lc '
  pip install -q pymongo cassandra-driver requests &&
  python bench.py mongo \
    --mongo-uri "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0" \
    --db bench --coll kv \
    --write-concern 1 \
    --read-from secondary \
    --ops 5000 --keys 1000 --write-ratio 0.3
```
- 쓰기 avg/p95 가 낮음(빠름)
- stale_read_rate, ryw_violation_rate가 0이 아닐 가능성(팔로워가 복제 중이라 늦게 보임)

2. 신선함 우선: 리더에서 읽고, 다수 복제본이 저장했는지 확인
```shell
docker run --rm -it --network replication_default -v "$PWD":/work -w /work python:3.11 bash -lc '
  pip install -q pymongo cassandra-driver requests &&
  python bench.py mongo \
    --mongo-uri "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0" \
    --db bench --coll kv \
    --write-concern majority \
    --read-from primary \
    --ops 5000 --keys 1000 --write-ratio 0.3
```
- 쓰기 avg/p95 올라감(느려짐)
- stale/ryw는 거의 0에 근접 (바로바로 최신이 보임)


3. 절충: majority로 쓰되, 읽기는 팔로워에서
```shell
docker run --rm -it --network replication_default -v "$PWD":/work -w /work python:3.11 bash -lc '
  pip install -q pymongo cassandra-driver requests &&
  python bench.py mongo \
    --mongo-uri "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0" \
    --db bench --coll kv \
    --write-concern majority \
    --read-from secondary \
    --ops 5000 --keys 1000 --write-ratio 0.3
```
- 쓰기 지연은 2와 비슷.
- stale/RYW는 A1보다 낮지만 0은 아닐 수 있음(팔로워 읽기 특성).



### Cassandra (리더리스)
1. 적당히 안전 + 빠른 읽기: 쓰기는 다수 확인, 읽기는 한 군데만
```shell
docker run --rm -it --network replication_default -v "$PWD":/work -w /work python:3.11 bash -lc '
  pip install -q pymongo cassandra-driver requests &&
  python bench.py cassandra \
    --hosts "cassandra1" \
    --keyspace bench --table kv --rf 3 \
    --write-cl QUORUM --read-cl ONE \
    --ops 5000 --keys 1000 --write-ratio 0.3
'
```
- 읽기가 빠른 편(ONE), 쓰기는 QUORUM이라 중간 속도
- stale/RYW는 소량 있을 수 있음(읽은 노드가 뒤처졌다면 과거 값)


2. 신선함 쪽으로 더 기울기: 쓰기/읽기 모두 다수 확인
```shell
docker run --rm -it --network replication_default -v "$PWD":/work -w /work python:3.11 bash -lc '
  pip install -q pymongo cassandra-driver requests &&
  python bench.py cassandra \
    --hosts "cassandra1" \
    --keyspace bench --table kv --rf 3 \
    --write-cl QUORUM --read-cl QUORUM \
    --ops 5000 --keys 1000 --write-ratio 0.3
'
```
- avg/p95가 올라감(느려짐)
- stale/RYW는 더 줄어듦


3. 최대 속도 실험(위험): 쓰기/읽기 한 군데만 확인
```shell
docker run --rm -it --network replication_default -v "$PWD":/work -w /work python:3.11 bash -lc '
  pip install -q pymongo cassandra-driver requests &&
  python bench.py cassandra \
    --hosts "cassandra1" \
    --keyspace bench --table kv --rf 3 \
    --write-cl ONE --read-cl ONE \
    --ops 5000 --keys 1000 --write-ratio 0.3
'
```
- 수치상 가장 빠름
- stale/RYW가 눈에 띄게 늘 수 있음 (안전장치가 거의 없음)

### CouchDB (다중 리더)
1. A/B에 반반으로 쓰기, A에서 읽기
```shell
python bench.py couch \
  --couch-a "http://admin:pass@localhost:5984" \
  --couch-b "http://admin:pass@localhost:5985" \
  --db bench \
  --write-to both --read-from a \
  --ops 5000 --keys 1000 --write-ratio 0.3
```
- conflicts가 0보다 커질 수 있음 — 서로 다른 노드에서 같은 문서를 거의 동시에 고쳐 충돌
- stale/RYW도 복제 지연으로 조금 보일 수 있음

2. B에만 쓰고, A에서 읽기
```shell
python bench.py couch \
  --couch-a "http://admin:pass@localhost:5984" \
  --couch-b "http://admin:pass@localhost:5985" \
  --db bench \
  --write-to b --read-from a \
  --ops 5000 --keys 500 --write-ratio 0.5
```
- A가 B의 변경을 늦게 받아 stale/RYW가 상대적으로 더 보일 수 있음
- conflicts는 1보다 적게 나올 수 있지만, 상황에 따라 생김