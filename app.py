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

        # 입금과 출금 거래 분리
        deposit_df = df[df["거래 유형"] == "입금"].copy()
        withdrawal_df = df[df["거래 유형"] == "출금"].copy()

        # 적요 정리 함수
        def process_memo(row):
            if re.match(r"^\d{2}[가-힣]{2,3}$", row["적요"]):
                return row["적요"]
            else:
                # 메모 열이 있고 값이 있으면 메모 사용, 없으면 원래 적요 사용
                if "메모" in row and pd.notna(row["메모"]) and str(row["메모"]).strip():
                    return str(row["메모"]).strip()
                else:
                    return row["적요"]

        # 입금 데이터 처리
        deposit_df["적요"] = deposit_df["적요"].str.replace(r"\(.*$", "", regex=True)
        deposit_df["적요"] = deposit_df["적요"].str.replace(" ", "")
        deposit_df["적요"] = deposit_df["적요"].str.replace(r"^\d{2}(\d{2})\d{6}\s*([가-힣]{3})$", r"\1\2", regex=True)
        deposit_df["적요"] = deposit_df["적요"].str.replace(r"^\d{2}(\d{2})\d{6}\s*([가-힣]{2})$", r"\1\2", regex=True)
        deposit_df["적요"] = deposit_df["적요"].str.replace(r"^(\d{2})\s*(학번)?\s*([가-힣]{3})$", r"\1\3", regex=True)
        deposit_df["적요"] = deposit_df["적요"].str.replace(r"^(\d{2})\s*(학번)?\s*([가-힣]{2})$", r"\1\3", regex=True)
        deposit_df["적요"] = deposit_df.apply(process_memo, axis=1)

        # 출금 데이터에서 환불 패턴 찾기
        refund_pattern = r"^\d{2}[가-힣]{2,3},"
        refund_df = withdrawal_df[withdrawal_df["메모"].str.contains(refund_pattern, regex=True, na=False)].copy()
        # 환불 데이터 처리
        refund_data = {}
        if not refund_df.empty:
            for _, row in refund_df.iterrows():
                memo = str(row["메모"]).strip()
                # 환불 패턴에서 사람 이름 추출
                match = re.match(refund_pattern, memo)
                if match:
                    person_name = match.group(0).rstrip(',')  # 마지막 콤마 제거
                    refund_amount = row["거래 금액"]
                    if person_name in refund_data:
                        refund_data[person_name] += refund_amount
                    else:
                        refund_data[person_name] = refund_amount

        # 입금 데이터 그룹핑
        deposit_grouped = deposit_df.groupby("적요")["거래 금액"].sum().reset_index()
        deposit_grouped = deposit_grouped.sort_values(by="적요", ascending=False)
        deposit_grouped = deposit_grouped.drop_duplicates(subset="적요")

        # 환불 금액을 입금에서 차감
        final_data = []
        for _, row in deposit_grouped.iterrows():
            person_name = row["적요"]
            deposit_amount = row["거래 금액"]
            refund_amount = refund_data.get(person_name, 0)
            net_amount = deposit_amount + (refund_amount if refund_amount <= 0 else -refund_amount)

            # 환불이 있는 경우에만 처리
            if net_amount > 0:
                # 라벨 생성 (동적 매핑 또는 기본값)
                if label_mappings:
                    label_dict = {mapping.value: mapping.label for mapping in label_mappings}
                    label = label_dict.get(net_amount, "기타")
                else:
                    label = "술안먹" if net_amount == 15000 else "술먹음" if net_amount == 18000 else "기타"

                if label != "기타":
                    datum = {
                        "입금자": person_name,
                        "입금액": net_amount,
                        "구분": label,
                        "원래입금액": deposit_amount,
                        "환불금액": refund_amount
                    }

                    final_data.append(datum)

        # 최종 정렬
        final_data.sort(key=lambda x: (x["구분"], x["입금자"]))

        return final_data

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"파일 처리 중 오류가 발생했습니다: {str(e)}")

@app.get("/")
async def root():
    """메인 페이지 - 파일 업로드 폼"""
    return FileResponse("static/index.html")

@app.get("/user-analysis")
async def user_analysis():
    """유저별 입금액 분석 페이지"""
    return FileResponse("static/user-analysis.html")

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

@app.post("/process-user-deposits")
async def process_user_deposits(
    deposit_file: UploadFile = File(..., description="입금 기록 Excel 파일"),
    user_file: UploadFile = File(..., description="유저목록 Excel 파일"),
    password: str = Form(..., description="입금 기록 Excel 파일 비밀번호")
):
    """
    입금 기록과 유저목록을 분석하여 유저별 총 입금액을 계산합니다.

    - **deposit_file**: 입금 기록.xlsx 파일
    - **user_file**: 유저목록.xlsx 파일
    - **password**: 입금 기록 Excel 파일의 비밀번호
    """
    # 파일 확장자 검증
    if not deposit_file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="입금 기록 파일은 Excel 파일(.xlsx, .xls)만 업로드 가능합니다.")

    if not user_file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="유저목록 파일은 Excel 파일(.xlsx, .xls)만 업로드 가능합니다.")

    try:
        # 입금 기록 파일 처리
        deposit_content = await deposit_file.read()
        deposit_data = process_deposit_file(deposit_content, password)

        # 유저목록 파일 처리
        user_content = await user_file.read()
        user_data = process_user_file(user_content)

        # 유저별 입금액 계산
        result = calculate_user_deposits(deposit_data, user_data)

        return {
            "success": True,
            "message": "유저별 입금액 분석이 완료되었습니다.",
            "data": result,
            "total_users": len(result),
            "summary": {
                "over_10k": len([user for user in result if user["total_amount"] >= 10000]),
                "over_20k": len([user for user in result if user["total_amount"] >= 20000]),
                "over_30k": len([user for user in result if user["total_amount"] >= 30000])
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"서버 오류가 발생했습니다: {str(e)}")

def process_deposit_file(file_content: bytes, password: str) -> List[Dict[str, Any]]:
    """
    입금 기록 Excel 파일을 처리합니다.
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
                          usecols='B:J', header=8)

        # 입금 거래만 필터링
        df = df[df["거래 유형"] == "입금"]

        # 적요 정리 (기존 로직과 동일)
        df["적요"] = df["적요"].str.replace(r"\(.*$", "", regex=True)
        df["적요"] = df["적요"].str.replace(" ", "")
        df["적요"] = df["적요"].str.replace(r"^\d{2}(\d{2})\d{6}\s*([가-힣]{3})$", r"\1\2", regex=True)
        df["적요"] = df["적요"].str.replace(r"^\d{2}(\d{2})\d{6}\s*([가-힣]{2})$", r"\1\2", regex=True)
        df["적요"] = df["적요"].str.replace(r"^(\d{2})\s*(학번)?\s*([가-힣]{3})$", r"\1\3", regex=True)
        df["적요"] = df["적요"].str.replace(r"^(\d{2})\s*(학번)?\s*([가-힣]{2})$", r"\1\3", regex=True)

        # 패턴 매칭이 안 될 경우 메모 열의 값을 사용
        def process_memo(row):
            if re.match(r"^\d{2}[가-힣]{2,3}$", row["적요"]):
                return row["적요"]
            else:
                if "메모" in row and pd.notna(row["메모"]) and str(row["메모"]).strip():
                    return str(row["메모"]).strip()
                else:
                    return row["적요"]

        df["적요"] = df.apply(process_memo, axis=1)

        # 적요별 거래금액 합계
        df = df.groupby("적요")["거래 금액"].sum().reset_index()
        df = df.sort_values(by="적요", ascending=False)
        df = df.drop_duplicates(subset="적요")

        return df.to_dict(orient="records")

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"입금 기록 파일 처리 중 오류가 발생했습니다: {str(e)}")

def process_user_file(file_content: bytes) -> List[str]:
    """
    유저목록 Excel 파일을 처리합니다.
    """
    try:
        # Excel 파일 읽기 (비밀번호 없음)
        df = pd.read_excel(io.BytesIO(file_content))

        # 첫 번째 컬럼을 유저명으로 가정
        user_names = df.iloc[:, 0].dropna().astype(str).tolist()

        return user_names

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"유저목록 파일 처리 중 오류가 발생했습니다: {str(e)}")

def calculate_user_deposits(deposit_data: List[Dict[str, Any]], user_names: List[str]) -> List[Dict[str, Any]]:
    """
    유저별 총 입금액을 계산합니다.
    """
    result = []

    for user_name in user_names:
        # 유저명과 일치하는 입금 기록 찾기
        user_deposits = [deposit for deposit in deposit_data if deposit["적요"] == user_name]

        total_amount = sum(deposit["거래 금액"] for deposit in user_deposits)

        # 금액별 구분
        amount_category = "미입금"
        if total_amount >= 30000:
            amount_category = "3만원 이상"
        elif total_amount >= 20000:
            amount_category = "2만원 이상"
        elif total_amount >= 10000:
            amount_category = "1만원 이상"
        elif total_amount > 0:
            amount_category = "1만원 미만"

        result.append({
            "user_name": user_name,
            "total_amount": total_amount,
            "deposit_count": len(user_deposits),
            "amount_category": amount_category
        })

    # 총 입금액 기준 내림차순 정렬
    result.sort(key=lambda x: x["total_amount"], reverse=True)

    return result

@app.get("/health")
async def health_check():
    """서버 상태 확인"""
    return {"status": "healthy", "message": "서버가 정상적으로 작동 중입니다."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
