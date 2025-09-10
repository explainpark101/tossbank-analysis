import io
import re
import pandas as pd
import msoffcrypto
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Dict, Any

app = FastAPI(title="토스뱅크 거래내역 분석 API", version="1.0.0")

# 정적 파일 서빙 설정
app.mount("/static", StaticFiles(directory="static"), name="static")

# Pydantic 모델 정의
class LabelMapping(BaseModel):
    value: int
    label: str

class ProcessRequest(BaseModel):
    label_mappings: List[LabelMapping]

def process_excel_data(file_content: bytes, password: str, label_mappings: List[LabelMapping] = None) -> List[Dict[str, Any]]:
    """
    Excel 파일을 처리하여 거래내역 데이터를 분석합니다.
    """
    try:
        decrypted_workbook = io.BytesIO()

        # 메모리에서 파일 처리
        file_io = io.BytesIO(file_content)
        office_file = msoffcrypto.OfficeFile(file_io)
        office_file.load_key(password=password)
        office_file.decrypt(decrypted_workbook)

        # Excel 파일 읽기
        df = pd.read_excel(decrypted_workbook, sheet_name='토스뱅크 거래내역',
                          usecols='B:I', header=8)

        # 입금 거래만 필터링
        df = df[df["거래 유형"] == "입금"]

        # 적요 정리
        df["적요"] = df["적요"].str.replace(r"\(.*$", "", regex=True)
        df["적요"] = df["적요"].str.replace(" ", "")
        # 10자리 숫자 + 한글 패턴에서 앞의 2자리 숫자 + 한글 추출 (우선순위)
        df["적요"] = df["적요"].str.replace(r"^\d{2}(\d{2})\d{6}\s*([가-힣]{3})$", r"\1\2", regex=True)
        df["적요"] = df["적요"].str.replace(r"^\d{2}(\d{2})\d{6}\s*([가-힣]{2})$", r"\1\2", regex=True)
        # 학번 패턴에서 숫자 + 한글 추출
        df["적요"] = df["적요"].str.replace(r"^(\d{2})\s*(학번)?\s*([가-힣]{3})$", r"\1\3", regex=True)
        df["적요"] = df["적요"].str.replace(r"^(\d{2})\s*(학번)?\s*([가-힣]{2})$", r"\1\3", regex=True)


        # 적요별 거래금액 합계
        df = df.groupby("적요")["거래 금액"].sum().reset_index()

        # 적요 기준 내림차순 정렬
        df = df.sort_values(by="적요", ascending=False)

        # 적요 기준 중복 제거
        df = df.drop_duplicates(subset="적요")

        # 라벨 생성 (동적 매핑 또는 기본값)
        if label_mappings:
            # 사용자 정의 라벨 매핑 사용
            label_dict = {mapping.value: mapping.label for mapping in label_mappings}
            df["label"] = df["거래 금액"].apply(
                lambda x: label_dict.get(x, "기타")
            )
            # 기타가 아닌 라벨만 필터링
            df = df[df["label"] != "기타"]
        else:
            # 기본 라벨 매핑 (기존 로직)
            df["label"] = df["거래 금액"].apply(
                lambda x: "술안먹" if x == 15000 else "술먹음" if x == 18000 else "기타"
            )
            df = df[df["label"] != "기타"]

        # 최종 정렬
        df = df.sort_values(by=["label", "적요"], ascending=True).reset_index(drop=True)
        df.columns = ["입금자", "입금액", "구분"]

        return df.to_dict(orient="records")

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"파일 처리 중 오류가 발생했습니다: {str(e)}")

@app.get("/")
async def root():
    """메인 페이지 - 파일 업로드 폼"""
    return FileResponse("static/index.html")

@app.post("/process-transaction-data")
async def process_transaction_data(
    file: UploadFile = File(..., description="토스뱅크 거래내역 Excel 파일"),
    password: str = Form(..., description="Excel 파일 비밀번호"),
    label_mappings: str = Form("", description="라벨 매핑 JSON 문자열 (예: [{'value': 15000, 'label': '술안먹'}, {'value': 18000, 'label': '술먹음'}]")
):
    """
    토스뱅크 거래내역 Excel 파일을 업로드하고 분석합니다.

    - **file**: 토스뱅크 거래내역.xlsx 파일
    - **password**: Excel 파일의 비밀번호
    """
    # 파일 확장자 검증
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Excel 파일(.xlsx, .xls)만 업로드 가능합니다.")

    try:
        # 파일 내용 읽기
        file_content = await file.read()

        # 라벨 매핑 파싱
        parsed_label_mappings = None
        if label_mappings.strip():
            try:
                import json
                label_data = json.loads(label_mappings)
                parsed_label_mappings = [LabelMapping(**item) for item in label_data]
            except (json.JSONDecodeError, ValueError) as e:
                raise HTTPException(status_code=400, detail=f"라벨 매핑 형식이 올바르지 않습니다: {str(e)}")

        # 데이터 처리
        result = process_excel_data(file_content, password, parsed_label_mappings)

        return {
            "success": True,
            "message": "데이터 처리가 완료되었습니다.",
            "data": result,
            "total_records": len(result)
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"서버 오류가 발생했습니다: {str(e)}")

@app.get("/health")
async def health_check():
    """서버 상태 확인"""
    return {"status": "healthy", "message": "서버가 정상적으로 작동 중입니다."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
