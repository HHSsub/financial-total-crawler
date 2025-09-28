import sqlite3

def inspect_db(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # DB내 모든 테이블명 조회
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    if not tables:
        print("DB 내 테이블이 존재하지 않습니다.")
        conn.close()
        return

    print("DB 내 테이블 목록:")
    for (table_name,) in tables:
        print(f" - {table_name}")

    # 각 테이블의 컬럼 및 샘플 데이터 출력
    for (table_name,) in tables:
        print(f"\n=== 테이블: {table_name} ===")

        # 컬럼 이름 조회
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        print(f"컬럼 ({len(columns)}개): {column_names}")

        # 샘플 데이터 3개 조회
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 3;")
        rows = cursor.fetchall()
        if rows:
            for row in rows:
                print(row)
        else:
            print("샘플 데이터가 없습니다.")

    conn.close()

if __name__ == "__main__":
    db_file = "news_fulltext_extract_20250915_171004.db"  # 필요시 경로 수정
    inspect_db(db_file)
