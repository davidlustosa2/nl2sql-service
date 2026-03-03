import base64
import io
import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from vanna.openai import OpenAI_Chat
from vanna.vanna import VannaDefault

app = FastAPI()

# =========================
# CONFIG VIA ENV VARS
# =========================

DB_URL = os.environ.get("DB_URL")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
MAX_ROWS = int(os.environ.get("MAX_ROWS", "2000"))

if not DB_URL:
    raise RuntimeError("Defina DB_URL nas variáveis de ambiente")

if not OPENAI_API_KEY:
    raise RuntimeError("Defina OPENAI_API_KEY nas variáveis de ambiente")

engine = create_engine(DB_URL, pool_pre_ping=True)

vn = VannaDefault(
    OpenAI_Chat(api_key=OPENAI_API_KEY)
)

# =========================
# MODELO REQUEST
# =========================

class AskRequest(BaseModel):
    question: str
    format: str = "csv"  # csv ou xlsx

# =========================
# HEALTH
# =========================

@app.get("/health")
def health():
    return {"status": "ok"}

# =========================
# ENDPOINT PRINCIPAL
# =========================

@app.post("/ask")
def ask(req: AskRequest):

    sql = vn.generate_sql(req.question)

    # 🔒 Segurança: só permitir SELECT
    if not re.match(r"(?is)^\s*select\b", sql.strip()):
        raise HTTPException(status_code=400, detail="Somente SELECT permitido")

    # 🔒 Forçar limite
    if "limit" not in sql.lower():
        sql += f"\nLIMIT {MAX_ROWS}"

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)

    # Exportar arquivo
    if req.format.lower() == "xlsx":
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        file_bytes = buffer.getvalue()
        file_name = "resultado.xlsx"
    else:
        file_bytes = df.to_csv(index=False).encode("utf-8")
        file_name = "resultado.csv"

    file_base64 = base64.b64encode(file_bytes).decode("utf-8")

    return {
        "answer_text": f"Consulta executada com sucesso. {len(df)} linhas retornadas.",
        "sql": sql,
        "file_name": file_name,
        "file_base64": file_base64
    }
