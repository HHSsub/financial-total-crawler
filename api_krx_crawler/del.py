import os

egg_path = r"C:\Users\User\OneDrive\문서\스터디\개인스터디\korean-grammar-rag.egg-link"

if os.path.exists(egg_path):
    os.remove(egg_path)
    print("삭제 완료")
else:
    print("파일 없음")
