import io
import re
import pandas as pd
import msoffcrypto
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

app = FastAPI(title="토스뱅크 거래내역 분석 API", version="1.0.0")

# 정적 파일 서빙 설정
app.mount("/static", StaticFiles(directory="static"), name="static")

# Pydantic 모델 정의
class LabelMapping(BaseModel):
    value: int
    label: str

class ProcessRequest(BaseModel):
    label_mappings: List[LabelMapping]

def process_excel_data(file_content: bytes, password: str, label_mappings: Optional[List[LabelMapping]] = None) -> List[Dict[str, Any]]:
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
            net_amount = deposit_amount - abs(refund_amount)

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
                          usecols='B:I', header=8)

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

def process_user_analysis_data(file_content: bytes, password: str) -> Dict[str, Dict[str, Any]]:
    """
    사용자 분석을 위해 특정 금액(10000, 20000, 30000)의 입금 내역만 처리합니다.
    """
    try:
        decrypted_workbook = io.BytesIO()
        file_io = io.BytesIO(file_content)
        office_file = msoffcrypto.OfficeFile(file_io)
        office_file.load_key(password=password)
        office_file.decrypt(decrypted_workbook)

        df = pd.read_excel(decrypted_workbook, sheet_name='토스뱅크 거래내역', usecols='B:I', header=8)

        # 1. 입금 거래만 필터링
        deposit_df = df[df["거래 유형"] == "입금"].copy()

        # 2. 특정 금액(10000, 20000, 30000)의 거래만 필터링
        valid_amounts = [10000, 20000, 30000]
        deposit_df = deposit_df[deposit_df['거래 금액'].isin(valid_amounts)]

        if deposit_df.empty:
            return {}

        # 3. 적요 정리 (기존 로직과 유사하게)
        def clean_memo(memo_series):
            memo_series = memo_series.str.replace(r"\(.*$", "", regex=True)
            memo_series = memo_series.str.replace(" ", "")
            memo_series = memo_series.str.replace(r"^\d{2}(\d{2})\d{6}\s*([가-힣]{3})$", r"\1\2", regex=True)
            memo_series = memo_series.str.replace(r"^\d{2}(\d{2})\d{6}\s*([가-힣]{2})$", r"\1\2", regex=True)
            memo_series = memo_series.str.replace(r"^(\d{2})\s*(학번)?\s*([가-힣]{3})$", r"\1\3", regex=True)
            memo_series = memo_series.str.replace(r"^(\d{2})\s*(학번)?\s*([가-힣]{2})$", r"\1\3", regex=True)
            return memo_series

        deposit_df['적요_정리'] = clean_memo(deposit_df['적요'])

        def process_memo_fallback(row):
            # 메모가 있으면 메모를 우선하도록 함.
            if "메모" in row and pd.notna(row["메모"]) and str(row["메모"]).strip():
                return str(row["메모"]).strip()
            # if not re.match(r"^\d{2}[가-힣]{2,3}$", row["적요_정리"]):
            return row["적요_정리"]

        deposit_df['최종_입금자'] = deposit_df.apply(process_memo_fallback, axis=1)

        # 4. 입금자별로 입금액 합계 및 날짜 목록 계산
        # 거래 일시를 '월-일' 형식의 문자열로 변환
        deposit_df['거래 일시'] = pd.to_datetime(deposit_df['거래 일시']).dt.strftime('%m-%d')

        analysis = deposit_df.groupby('최종_입금자').agg(
            total_amount=('거래 금액', 'sum'),
            deposit_dates=('거래 일시', lambda x: sorted(list(set(x)))) # 중복 제거 및 정렬
        ).reset_index()

        # 결과를 {이름: {총액, 날짜목록}} 형태의 딕셔너리로 변환
        result_dict = analysis.set_index('최종_입금자').to_dict('index')
        return result_dict

    except Exception as e:
        # 여기서의 오류는 클라이언트에게 좀 더 구체적으로 전달될 수 있습니다.
        raise HTTPException(status_code=400, detail=f"사용자 분석 데이터 처리 중 오류: {str(e)}")

@app.post("/process-user-analysis")
async def api_process_user_analysis(
    deposit_file: UploadFile = File(..., description="입금 기록 Excel 파일"),
    password: str = Form(..., description="Excel 파일 비밀번호"),
    members: str = Form(..., description="줄바꿈으로 구분된 부원 명단")
):
    """
    부원 명단과 입금 기록을 받아, 특정 입금 내역을 분석하고 결과를 반환합니다.
    """
    if not deposit_file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Excel 파일(.xlsx, .xls)만 업로드 가능합니다.")

    try:
        file_content = await deposit_file.read()

        # 입금 내역 처리
        deposit_data = process_user_analysis_data(file_content, password)

        member_list = [name.strip() for name in members.split('\n') if name.strip()]

        result = []
        for member_name in member_list:
            if member_name in deposit_data:
                data = deposit_data[member_name]
                result.append({
                    "name": member_name,
                    "amount": data['total_amount'],
                    "dates": data['deposit_dates']
                })
            else:
                result.append({
                    "name": member_name,
                    "amount": 0,
                    "dates": []
                })

        # 입금액 0원인 사람을 뒤로, 나머지는 이름순으로 정렬
        result.sort(key=lambda x: (x['amount'] == 0, x['name']))

        return {
            "success": True,
            "message": "사용자 분석이 완료되었습니다.",
            "data": result
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
