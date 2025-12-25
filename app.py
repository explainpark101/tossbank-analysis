import io
import re
import json
from datetime import datetime
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

class PeriodConfig(BaseModel):
    start: str  # YYYY-MM-DD
    end: str    # YYYY-MM-DD
    name: Optional[str] = None  # 기간 이름 (선택사항)

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

def process_transaction_by_period(
    file_content: bytes, 
    password: str, 
    periods: List[PeriodConfig]
) -> List[Dict[str, Any]]:
    """
    기간별로 거래내역을 묶어서 처리합니다.
    기간에 포함되지 않은 거래는 개별적으로 출력하며, 
    수입/지출을 분리하고 잔액을 누적 계산합니다.
    """
    try:
        decrypted_workbook = io.BytesIO()
        
        # 메모리에서 파일 처리
        file_io = io.BytesIO(file_content)
        office_file = msoffcrypto.OfficeFile(file_io)
        office_file.load_key(password=password)
        office_file.decrypt(decrypted_workbook)
        
        # Excel 파일 읽기 (컬럼을 넉넉하게 읽음)
        df = pd.read_excel(decrypted_workbook, sheet_name='토스뱅크 거래내역', header=8)
        
        # 필수 컬럼 확인 및 정리
        required_cols = ['거래 일시', '거래 유형', '거래 금액', '거래 후 잔액', '적요']
        for col in required_cols:
            if col not in df.columns:
                # 컬럼명이 조금 다를 수 있으므로 확인 (공백 제거 등)
                found = False
                for existing_col in df.columns:
                    if existing_col.replace(' ', '') == col.replace(' ', ''):
                        df.rename(columns={existing_col: col}, inplace=True)
                        found = True
                        break
                if not found:
                    raise HTTPException(status_code=400, detail=f"필수 컬럼 '{col}'을(를) 찾을 수 없습니다.")

        # 날짜 변환
        df['거래 일시'] = pd.to_datetime(df['거래 일시'])
        
        # 거래 유형 공백 제거
        df['거래 유형'] = df['거래 유형'].astype(str).str.strip()
        
        # 날짜 오름차순 정렬 (과거 -> 현재) 및 인덱스 재설정
        df = df.sort_values(by='거래 일시').reset_index(drop=True)
        
        # 기초 잔액 계산 (첫 거래 이전의 잔액)
        if not df.empty:
            first_row = df.iloc[0]
            first_balance = first_row['거래 후 잔액']
            first_amount = first_row['거래 금액'] # 출금일 경우 음수일 수 있음
            first_type = str(first_row['거래 유형']).strip()
            
            # 거래 금액이 음수인 경우를 고려하여 절대값 사용
            abs_amount = abs(first_amount)
            
            if first_type == '입금':
                # 입금 전 잔액 = 현재 잔액 - 입금액
                initial_balance = first_balance - abs_amount
            else: # 출금
                # 출금 전 잔액 = 현재 잔액 + 출금액
                initial_balance = first_balance + abs_amount
        else:
            initial_balance = 0

        # 처리된 거래 리스트
        processed_transactions = []
        
        # 처리 여부 마킹 컬럼 추가
        df['is_processed'] = False
        
        # 각 기간별로 묶을 거래 처리
        for period in periods:
            start_date = pd.to_datetime(period.start)
            # 종료일의 23:59:59까지 포함하기 위해 조정
            end_date = pd.to_datetime(period.end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            
            # 기간 내 거래 중 아직 처리되지 않은 거래 필터링
            mask = (df['거래 일시'] >= start_date) & (df['거래 일시'] <= end_date) & (~df['is_processed'])
            period_df = df[mask].copy()
            
            if period_df.empty:
                continue
                
            # 처리된 행 마킹
            df.loc[mask, 'is_processed'] = True
            
            # 1. 수입(입금) 처리
            # 입금, 프로모션입금, 이자입금, 모임원송금 포함
            deposit_mask = period_df["거래 유형"].astype(str).str.contains('입금|모임원송금')
            deposit_df = period_df[deposit_mask]
            
            income_memos = [] # 비고용 메모 모음
            income_jukyos = [] # 사용처용 적요 모음
            
            if not deposit_df.empty:
                total_deposit = 0
                income_names = []
                membership_fee_amount = 0
                membership_fee_count = 0
                membership_fee_pattern = re.compile(r'^\d{2}\s*[가-힣]{2,3}$')
                
                for _, row in deposit_df.iterrows():
                    amount = abs(int(row["거래 금액"]))
                    total_deposit += amount
                    
                    memo = row.get("메모", "")
                    memo_str = str(memo).strip() if pd.notna(memo) else ""
                    jukyo = str(row.get("적요", "")).strip()
                    
                    # 비고/사용처용 데이터 수집
                    if memo_str and memo_str not in income_memos:
                        income_memos.append(memo_str)
                    if jukyo and jukyo not in income_jukyos:
                        income_jukyos.append(jukyo)
                    
                    # 회비 입금 체크 로직 변경
                    # 1. 적요 우선 검사
                    is_fee = False
                    
                    if jukyo and membership_fee_pattern.match(jukyo):
                        is_fee = True
                    # 2. 적요가 아니면 메모 검사
                    elif memo_str and membership_fee_pattern.match(memo_str):
                        is_fee = True
                    
                    if is_fee and amount % 10000 == 0:
                        membership_fee_amount += amount
                        membership_fee_count += 1
                    else:
                        # 일반 입금은 메모 > 적요 우선순위로 이름 표시
                        display_name = memo_str if memo_str else jukyo
                        if display_name and display_name not in income_names:
                            income_names.append(display_name)
                
                # 수입명 생성
                income_name_parts = []
                if period.name:
                    income_name_parts.append(period.name)
                
                if membership_fee_count > 0:
                    income_name_parts.append(f"회비 입금 ({membership_fee_count}건)")
                
                if income_names:
                    income_name_parts.extend(income_names[:2])
                    if len(income_names) > 2:
                        income_name_parts.append(f"외 {len(income_names) - 2}건")
                
                final_income_name = ", ".join(income_name_parts)
                
                # 비고/사용처 생성
                final_note = period.name or ""
                if income_memos:
                    memo_text = ", ".join(income_memos[:3])
                    if len(income_memos) > 3: 
                        memo_text += f" 외 {len(income_memos)-3}건"
                    final_note += f" ({memo_text})" if final_note else memo_text
                    
                final_usage = ", ".join(income_jukyos[:3])
                if len(income_jukyos) > 3:
                    final_usage += f" 외 {len(income_jukyos)-3}건"
                
                # 수입 거래 추가
                processed_transactions.append({
                    "raw_date": end_date, # 정렬을 위해 기간 종료일 사용
                    "날짜": period.start, # 표시는 시작일 (또는 기간 이름)
                    "수입명": final_income_name,
                    "수입금액": int(total_deposit),
                    "지출명": "",
                    "지출금액": 0,
                    "비고": final_note,
                    "사용처": final_usage
                })

            # 2. 지출(출금) 처리
            # 출금, 체크카드결제 포함
            withdrawal_mask = period_df["거래 유형"].astype(str).str.contains('출금|결제')
            withdrawal_df = period_df[withdrawal_mask]
            
            expense_memos = []
            expense_jukyos = []
            
            if not withdrawal_df.empty:
                total_withdrawal = 0
                expense_names = []
                
                for _, row in withdrawal_df.iterrows():
                    total_withdrawal += abs(int(row["거래 금액"]))
                    
                    memo = row.get("메모", "")
                    memo_str = str(memo).strip() if pd.notna(memo) else ""
                    jukyo = str(row.get("적요", "")).strip()
                    
                    # 비고/사용처용 데이터 수집
                    if memo_str and memo_str not in expense_memos:
                        expense_memos.append(memo_str)
                    if jukyo and jukyo not in expense_jukyos:
                        expense_jukyos.append(jukyo)
                    
                    display_name = memo_str if memo_str else jukyo
                    if display_name and display_name not in expense_names:
                        expense_names.append(display_name)
                
                # 지출명 생성
                expense_name_parts = []
                if period.name:
                    expense_name_parts.append(period.name)
                expense_name_parts.extend(expense_names[:2])
                if len(expense_names) > 2:
                    expense_name_parts.append(f"외 {len(expense_names)-2}건")
                
                final_expense_name = ", ".join(expense_name_parts)
                
                # 비고/사용처 생성
                final_note = period.name or ""
                if expense_memos:
                    memo_text = ", ".join(expense_memos[:3])
                    if len(expense_memos) > 3: 
                        memo_text += f" 외 {len(expense_memos)-3}건"
                    final_note += f" ({memo_text})" if final_note else memo_text
                    
                final_usage = ", ".join(expense_jukyos[:3])
                if len(expense_jukyos) > 3:
                    final_usage += f" 외 {len(expense_jukyos)-3}건"

                # 지출 거래 추가
                processed_transactions.append({
                    "raw_date": end_date, # 정렬용
                    "날짜": period.start,
                    "수입명": "",
                    "수입금액": 0,
                    "지출명": final_expense_name,
                    "지출금액": int(total_withdrawal),
                    "비고": final_note,
                    "사용처": final_usage
                })

        # 기간에 포함되지 않은 거래 중 회비 입금 패턴 찾아서 묶기
        mask_remaining = ~df['is_processed']
        remaining_df = df[mask_remaining].copy()
        
        membership_fee_pattern = re.compile(r'^\d{2}\s*[가-힣]{2,3}$')
        
        def is_membership_fee(row):
            if str(row['거래 유형']).strip() != '입금':
                return False
            amount = abs(int(row['거래 금액']))
            if amount % 10000 != 0:
                return False
            
            memo = row.get("메모", "")
            memo_str = str(memo).strip() if pd.notna(memo) else ""
            jukyo = str(row.get("적요", "")).strip()
            
            # 적요 우선 검사
            if jukyo and membership_fee_pattern.match(jukyo):
                return True
            # 적요가 아니면 메모 검사
            if memo_str and membership_fee_pattern.match(memo_str):
                return True
                
            return False

        # 회비 입금 마스크 생성 및 처리
        if not remaining_df.empty:
            fee_mask = remaining_df.apply(is_membership_fee, axis=1)
            fee_df = remaining_df[fee_mask]
            
            if not fee_df.empty:
                # 원본 df에 처리됨 표시
                df.loc[fee_df.index, 'is_processed'] = True
                
                total_fee = fee_df['거래 금액'].sum()
                count_fee = len(fee_df)
                last_date = fee_df['거래 일시'].max()
                first_date = fee_df['거래 일시'].min()
                
                date_display = last_date.strftime('%Y-%m-%d')
                if first_date.strftime('%Y-%m-%d') != date_display:
                    date_display = f"{first_date.strftime('%Y-%m-%d')} ~ {date_display}"
                
                processed_transactions.append({
                    "raw_date": last_date,
                    "날짜": date_display,
                    "수입명": f"회비 입금 ({count_fee}건)",
                    "수입금액": int(total_fee),
                    "지출명": "",
                    "지출금액": 0,
                    "비고": "기간 미지정 회비",
                    "사용처": ""
                })

        # 기간에 포함되지 않은 나머지 거래 처리 (회비 입금 제외됨)
        final_remaining_df = df[~df['is_processed']]
        
        for _, row in final_remaining_df.iterrows():
            memo = row.get("메모", "")
            memo_str = str(memo).strip() if pd.notna(memo) else ""
            memo_text = memo_str if memo_str else str(row.get("적요", "")).strip()
            
            place = row.get("거래처", "")
            place_str = str(place).strip() if pd.notna(place) else ""
            
            transaction = {
                "raw_date": row['거래 일시'],
                "날짜": row['거래 일시'].strftime('%Y-%m-%d'),
                "수입명": "",
                "수입금액": 0,
                "지출명": "",
                "지출금액": 0,
                "비고": "",
                "사용처": place_str
            }
            
            t_type = str(row['거래 유형']).strip()
            amount = abs(int(row['거래 금액']))
            
            # 비고와 사용처 채우기
            # 사용처: 적요 (입금자명/거래처)
            transaction["사용처"] = str(row.get("적요", "")).strip()
            # 비고: 메모
            memo_val = row.get("메모", "")
            transaction["비고"] = str(memo_val).strip() if pd.notna(memo_val) else ""
            
            # 거래 유형 분류
            if any(x in t_type for x in ['입금', '모임원송금']):
                transaction["수입명"] = memo_text
                transaction["수입금액"] = amount
            elif any(x in t_type for x in ['출금', '결제']):
                transaction["지출명"] = memo_text
                transaction["지출금액"] = amount
            else:
                # 알 수 없는 유형은 일단 비고에 추가하고 지출로 처리하지 않음
                if transaction["비고"]:
                    transaction["비고"] += f" (유형: {t_type})"
                else:
                    transaction["비고"] = f"유형 확인 필요: {t_type}"
            
            processed_transactions.append(transaction)
            
        # 날짜순 정렬
        processed_transactions.sort(key=lambda x: x['raw_date'])
        
        # 잔액 재계산
        current_balance = initial_balance
        final_result = []
        
        for item in processed_transactions:
            # 수입 더하고 지출 빼기
            income = item.get("수입금액", 0)
            expense = abs(item.get("지출금액", 0))
            current_balance = current_balance + income - expense
            
            # 수입금액과 지출금액이 0인 경우 빈 문자열로 변환 (명확한 분리를 위해)
            income_display = item["수입금액"] if item["수입금액"] > 0 else ""
            expense_display = item["지출금액"] if item["지출금액"] > 0 else ""
            
            # 결과 항목 생성 (raw_date 제외)
            result_item = {
                "날짜": item["날짜"],
                "수입명": item["수입명"],
                "수입금액": income_display,
                "지출명": item["지출명"],
                "지출금액": expense_display,
                "잔액": int(current_balance),
                "비고": item["비고"],
                "사용처": item["사용처"]
            }
            final_result.append(result_item)
            
        return final_result
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"파일 처리 중 오류가 발생했습니다: {str(e)}")

@app.post("/process-transaction-by-period")
async def process_transaction_by_period_api(
    file: UploadFile = File(..., description="토스뱅크 거래내역 Excel 파일"),
    password: str = Form(..., description="Excel 파일 비밀번호"),
    periods: str = Form(..., description="기간 설정 JSON 문자열 (예: [{'start': '2024-01-01', 'end': '2024-01-15', 'name': '1월 상반기'}, ...])")
):
    """
    토스뱅크 거래내역 Excel 파일을 업로드하고 기간별로 분석합니다.
    
    - **file**: 토스뱅크 거래내역.xlsx 파일
    - **password**: Excel 파일의 비밀번호
    - **periods**: 기간 설정 JSON 문자열
    """
    # 파일 확장자 검증
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Excel 파일(.xlsx, .xls)만 업로드 가능합니다.")
    
    try:
        # 파일 내용 읽기
        file_content = await file.read()
        
        # 기간 설정 파싱
        try:
            periods_data = json.loads(periods)
            parsed_periods = [PeriodConfig(**item) for item in periods_data]
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            raise HTTPException(status_code=400, detail=f"기간 설정 형식이 올바르지 않습니다: {str(e)}")
        
        if not parsed_periods:
            raise HTTPException(status_code=400, detail="최소 하나의 기간을 설정해야 합니다.")
        
        # 데이터 처리
        result = process_transaction_by_period(file_content, password, parsed_periods)
        
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

@app.get("/transaction-by-period")
async def transaction_by_period_page():
    """기간별 거래내역 분석 페이지"""
    return FileResponse("static/transaction-by-period.html")

@app.get("/health")
async def health_check():
    """서버 상태 확인"""
    return {"status": "healthy", "message": "서버가 정상적으로 작동 중입니다."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
