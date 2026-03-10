import base64
import io
import os
import re
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text

from vanna.openai.openai_chat import OpenAI_Chat
from vanna.chromadb.chromadb_vector import ChromaDB_VectorStore


class MyVanna(ChromaDB_VectorStore, OpenAI_Chat):
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)
        OpenAI_Chat.__init__(self, config=config)


# ===== ENV VARS =====
DB_URL = os.environ.get("DB_URL")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL")  # opcional
MAX_ROWS = int(os.environ.get("MAX_ROWS", "2000"))
DEFAULT_FORMAT = os.environ.get("DEFAULT_FORMAT", "csv")

if not DB_URL:
    raise RuntimeError("Defina DB_URL no EasyPanel (Environment Variables).")
if not OPENAI_API_KEY:
    raise RuntimeError("Defina OPENAI_API_KEY no EasyPanel (Environment Variables).")

engine = create_engine(DB_URL, pool_pre_ping=True)

vn_config = {"api_key": OPENAI_API_KEY}
if OPENAI_MODEL:
    vn_config["model"] = OPENAI_MODEL

vn = MyVanna(config=vn_config)

app = FastAPI(title="NL2SQL Service (Vanna 0.x + FastAPI)")


class AskRequest(BaseModel):
    question: str
    format: Optional[str] = None  # csv | xlsx
    max_rows: Optional[int] = None


def guardrails(sql: str, max_rows: int) -> str:
    s = (sql or "").strip().rstrip(";")
    if not s:
        raise HTTPException(status_code=400, detail="SQL vazio gerado pelo modelo.")
    if not re.match(r"(?is)^\s*select\b", s):
        raise HTTPException(status_code=400, detail="Somente SELECT é permitido.")
    if not re.search(r"(?is)\blimit\b", s):
        s = f"{s}\nLIMIT {max_rows}"
    return s


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ask")
def ask(req: AskRequest):
    question = (req.question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="Campo 'question' é obrigatório.")

    out_format = (req.format or DEFAULT_FORMAT).lower().strip()
    if out_format not in ("csv", "xlsx"):
        raise HTTPException(status_code=400, detail="format deve ser 'csv' ou 'xlsx'.")

    max_rows = int(req.max_rows) if req.max_rows else MAX_ROWS
    if max_rows <= 0 or max_rows > 50000:
        raise HTTPException(status_code=400, detail="max_rows inválido (1..50000).")

    try:
        sql = vn.generate_sql(question)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao gerar SQL: {e}")

    sql = guardrails(sql, max_rows)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao executar no banco: {e}")

    if out_format == "xlsx":
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="resultado")
        file_bytes = buf.getvalue()
        file_name = "resultado.xlsx"
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        file_bytes = df.to_csv(index=False).encode("utf-8-sig")
        file_name = "resultado.csv"
        mime = "text/csv"

    file_b64 = base64.b64encode(file_bytes).decode("ascii")

    return {
        "answer_text": f"OK. Retornei {len(df)} linhas e {len(df.columns)} colunas. Segue arquivo.",
        "sql": sql,
        "rows": len(df),
        "cols": list(df.columns),
        "file_name": file_name,
        "file_mime": mime,
        "file_base64": file_b64,
    }
