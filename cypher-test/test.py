from util import run

# 1. 기존 데이터 모두 삭제
run("MATCH (n) DETACH DELETE n")
print("✅ 전체 데이터 초기화 완료")

# 1. 전체 데이터 초기화
run("MATCH (n) DETACH DELETE n")
print("✅ 전체 데이터 초기화 완료")

# 2. 노드 생성 (CREATE 하나로 묶기)
run("""
CREATE
  (alice:Person {name:'Alice', age:30})-[:WORKS_AT]->(neo4j:Company {name:'Neo4j Inc'}),
  (bob:Person   {name:'Bob',   age:32})-[:WORKS_AT]->(neo4j),
  (carol:Person {name:'Carol', age:29})-[:WORKS_AT]->(acme:Company {name:'Acme Corp'}),
  (dave:Person  {name:'Dave',  age:40})-[:WORKS_AT]->(acme),
  (alice)-[:FRIEND]->(bob),
  (bob)-[:FRIEND]->(carol),
  (carol)-[:FRIEND]->(dave),
  (alice)-[:LIKES]->(:Topic {name:'GraphDB'})<-[:LIKES]-(bob),
  (carol)-[:LIKES]->(:Topic {name:'RDF'}),
  (dave)-[:LIKES]->(:Topic {name:'Distributed Systems'});
""")

print("✅ 노드와 관계 생성 완료")

# 4. 삭제 전 조회

print("\n🔍 RDF를 좋아하는 사람:")
rdf_likers = run("""
MATCH (p:Person)-[:LIKES]->(t:Topic {name: 'RDF'})
RETURN p.name AS person
""")
for r in rdf_likers:
    print(" -", r["person"])

print("\n🔍 30세 미만 사람의 연결 관계:")
under30_rel = run("""
MATCH (p:Person)-[r]-()
WHERE p.age < 30
RETURN p.name AS person, type(r) AS rel_type, endNode(r).name AS connected
""")
for r in under30_rel:
    print(f" - {r['person']} -[{r['rel_type']}]-> {r['connected']}")

print("\n🔍 Bob이 좋아하는 관심사:")
bob_likes = run("""
MATCH (:Person {name: 'Bob'})-[:LIKES]->(t:Topic)
RETURN t.name AS topic
""")
for r in bob_likes:
    print(" -", r["topic"])

# 5. 삭제 실행

# (1) 관심사 RDF 삭제
run("MATCH (t:Topic {name: 'RDF'}) DETACH DELETE t")
print("\n❌ RDF 관심사 삭제 완료")

# (2) 30세 미만 사람의 관계만 삭제
run("""
MATCH (p:Person)-[r]-()
WHERE p.age < 30
DELETE r
""")
print("❌ 30세 미만 사람의 관계 삭제 완료")

# (3) 'Neo4j' 회사 삭제
run("""
MATCH (c:Company)
WHERE c.name CONTAINS 'Neo4j'
DETACH DELETE c
""")
print("❌ Neo4j 회사 삭제 완료")

# (4) Bob의 LIKES 관계 삭제
run("""
MATCH (:Person {name: 'Bob'})-[r:LIKES]->(:Topic)
DELETE r
""")
print("❌ Bob의 관심사 삭제 완료")

# 6. 최종 확인
print("\n✅ 남아 있는 사람과 관심사 관계:")
results = run("""
MATCH (p:Person)-[r]->(t)
RETURN p.name AS person, type(r) AS rel, t.name AS target
""")
for r in results:
    print(f" - {r['person']} -[{r['rel']}]-> {r['target']}")
