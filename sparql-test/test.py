from rdflib import Graph, Namespace, Literal, RDF

# --------------------------------------------------
# 1) Graph() : RDF 삼중(triple)을 담는 인메모리 컨테이너
# --------------------------------------------------
g = Graph()

# --------------------------------------------------
# 2) Namespace 정의
#    - FOAF : 이미 널리 쓰이는 공개 어휘(사람 관련) (Friend Of A Friend)
#    - EX   : 내가 임의로 정하는 '내 데이터'의 베이스 URI (example.org는 관습)
# --------------------------------------------------
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
EX   = Namespace("http://example.org/")

# --------------------------------------------------
# 3) URI 리소스 정의
#    - EX.Alice 는 사실상 URIRef("http://example.org/Alice") 와 동일
#    - 굳이 미리 변수에 담지 않고 바로 쓰기도 가능
# --------------------------------------------------
alice = EX.Alice
bob   = EX.Bob
carol = EX.Carol
dave  = EX.Dave

graphdb = EX.GraphDB
rdftech = EX.RDF
dist    = EX.DistributedSystems

# --------------------------------------------------
# 4) 트리플 추가 g.add((주어, 술어, 목적어))
#    - RDF.type : "이 리소스는 어떤 클래스다" (FOAF.Person 같은)
#    - FOAF.name, EX.age, EX.likes 는 '속성(술어)'
# --------------------------------------------------
g.add((alice, RDF.type, FOAF.Person))
g.add((alice, FOAF.name, Literal("Alice")))
g.add((alice, EX.age, Literal(30)))
g.add((alice, EX.likes, graphdb))

g.add((bob, RDF.type, FOAF.Person))
g.add((bob, FOAF.name, Literal("Bob")))
g.add((bob, EX.age, Literal(32)))
g.add((bob, EX.likes, graphdb))

g.add((carol, RDF.type, FOAF.Person))
g.add((carol, FOAF.name, Literal("Carol")))
g.add((carol, EX.age, Literal(29)))
g.add((carol, EX.likes, rdftech))

g.add((dave, RDF.type, FOAF.Person))
g.add((dave, FOAF.name, Literal("Dave")))
g.add((dave, EX.age, Literal(40)))
g.add((dave, EX.likes, dist))

# 관계(사람 간 연결)도 그냥 한 트리플: (Alice foaf:knows Bob)
g.add((alice, FOAF.knows, bob))
g.add((bob, FOAF.knows, carol))
g.add((carol, FOAF.knows, dave))

# --------------------------------------------------
# 5) SPARQL 질의 예시
# --------------------------------------------------

# (A) Alice가 아는 사람 이름
q_friends = """
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ex:   <http://example.org/>

SELECT ?friendName
WHERE {
  ex:Alice foaf:knows ?f .
  ?f foaf:name ?friendName .
}
"""
print("▶ Alice가 아는 사람:")
for row in g.query(q_friends):
    print(" -", row.friendName)

# (B) 30세 이상 & GraphDB 좋아하는 사람
q_graphdb = """
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ex:   <http://example.org/>

SELECT ?name ?age
WHERE {
  ?p a foaf:Person ;
     foaf:name ?name ;
     ex:age ?age ;
     ex:likes ex:GraphDB .
  FILTER(?age >= 30)
}
"""
print("\n▶ 30세 이상이면서 GraphDB 좋아하는 사람:")
for row in g.query(q_graphdb):
    print(f" - {row.name} (age={row.age})")

# (C) 같은 관심사를 공유하는 사람 쌍 (자기 자신 제외, 중복 제거)
q_shared_interest = """
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ex:   <http://example.org/>

SELECT ?name1 ?name2 ?topic
WHERE {
  ?p1 ex:likes ?topic .
  ?p2 ex:likes ?topic .
  ?p1 foaf:name ?name1 .
  ?p2 foaf:name ?name2 .
  FILTER(?p1 != ?p2)
  FILTER(str(?p1) < str(?p2))  # 문자열 비교로 (A,B)와 (B,A) 중 하나만
}
"""
print("\n▶ 같은 관심사를 공유하는 (사람1, 사람2, 주제):")
for row in g.query(q_shared_interest):
    print(f" - {row.name1}, {row.name2} => {row.topic.split('/')[-1]}")

# --------------------------------------------------
# 6) 삭제(변경) 전 상태 일부 확인 (직접 순회: 작은 그래프라 가능)
# --------------------------------------------------
print("\n▶ RDF를 좋아하는 사람(삭제 전 확인):")
for s, p, o in g.triples((None, EX.likes, rdftech)):
    # s = 주어 (사람), p = ex:likes, o = rdftech
    print(" -", s)

print("\n▶ 30세 미만 사람 목록:")
for s, p, o in g.triples((None, EX.age, None)):
    if int(o) < 30:
        print(" -", s, "(age", o, ")")

# --------------------------------------------------
# 7) 삭제(수정) 작업
#    - RDF 관심사를 '좋아하는 관계'만 삭제
#    - 30세 미만(=Carol)인 사람의 모든 triple 삭제
# --------------------------------------------------

# (1) 특정 관심사 관계 제거
g.remove((None, EX.likes, rdftech))  # ex:likes rdftech 인 triple 모두 삭제

# (2) 30세 미만 사람 전체 triple 삭제
for s, p, o in list(g.triples((None, EX.age, None))):  # list()로 고정 스냅샷
    if int(o) < 30:
        g.remove((s, None, None))  # 그 주어(s)에 대한 모든 triple 제거

# --------------------------------------------------
# 8) 최종 남은 triple 출력
# --------------------------------------------------
print("\n▶ 최종 남은 Triple 개수:", len(g))
print("▶ 일부 출력:")
for s, p, o in list(g)[:10]:  # 많으면 앞 10개만
    print(" -", s, p, o)

# --------------------------------------------------
# 9) (선택) 파일로 직렬화 (Turtle 포맷)
# --------------------------------------------------
g.serialize("example_graph.ttl", format="turtle")
print("\n(Turtle 파일로 저장: example_graph.ttl)")