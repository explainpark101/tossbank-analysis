# Python 3.12 기반 이미지 사용
FROM python:3.12-slim

# 작업 디렉토리 설정
WORKDIR /app

# 시스템 패키지 업데이트 및 필요한 패키지 설치
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 파일 복사
COPY pyproject.toml uv.lock ./

# uv를 사용하여 의존성 설치
RUN pip install uv && uv sync --frozen

# 애플리케이션 코드 복사
COPY . .

# 포트 25564 노출
EXPOSE 25564

# 애플리케이션 실행
CMD ["uv", "run", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "25564"]
