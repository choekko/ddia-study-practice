from neo4j import GraphDatabase

# Neo4j 연결 설정 (Neo4j Docker가 실행 중이어야 함)
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4jtest"))

def run(query, **params):
    """Cypher 쿼리를 실행하는 함수"""
    with driver.session() as session:
        return list(session.run(query, **params))