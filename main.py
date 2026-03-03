import base64
import io
import os
import re
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# ============================================================
# Vanna imports (compat: tenta múltiplos paths)
# ============================================================
try:
    # path comum
    from vanna.openai_chat import OpenAI_Chat
except Exception:
    # variação
    from vanna.openai.openai_chat import OpenAI_Chat  # type: ignore

try:
    # path comum
    from vanna.chromadb_vector import ChromaDB_VectorStore
except Exception:
    # variação
    from vanna.chromadb.chromadb_vector import ChromaDB_VectorStore  # type: ignore


class MyVanna(ChromaDB_VectorStore, OpenAI_Chat):
    """
    Combina VectorStore (Chroma) + Chat model (OpenAI) como recomendado na doc do Vanna.
    """
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)
        OpenAI_Chat.__init__(self, config=config)


# ============================================================
# Config via Environment Variables
# ============================================================
DB_URL = os.environ.get("DB_URL")  # ex: postgresql+psycopg2://user:pass@host:5499/odoo_powergov
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL")  # opcional (ex: gpt-4o-mini)
MAX_ROWS = int(os.environ.get("MAX_ROWS", "2000"))
DEFAULT_FORMAT = os.environ.get("DEFAULT_FORMAT", "csv")  # csv | xlsx

if not DB_URL:
    raise RuntimeError("Defina DB_URL nas variáveis de ambiente (EasyPanel -> Env Vars).")

if not OPENAI_API_KEY:
    raise RuntimeError("Defina OPENAI_API_KEY nas variáveis de ambiente (EasyPanel -> Env Vars).")


# SQLAlchemy engine
engine = create_engine(DB_URL, pool_pre_ping=True)

# Vanna instance
vn_config = {"api_key": OPENAI_API_KEY}
if OPENAI_MODEL:
    vn_config["model"] = OPENAI_MODEL

vn = MyVanna(config=vn_config)

# ============================================================
# FastAPI app
# ============================================================
app = FastAPI(title="NL2SQL Service (Vanna + FastAPI)")


class AskRequest(BaseModel):
    question: str
    format: Optional[str] = None  # csv | xlsx
    max_rows: Optional[int] = None  # override opcional


def guardrails(sql: str, max_rows: int) -> str:
    """
    Guardrails básicos:
    - Só permite SELECT
    - Impõe LIMIT se não houver
    - Remove ; no final
    """
    if not sql or not sql.strip():
        raise HTTPException(status_code=400, detail="SQL vazio gerado pelo modelo.")

    s = sql.strip().rstrip(";")

    # Bloquear não-SELECT
    if not re.match(r"(?is)^\s*select\b", s):
        raise HTTPException(status_code=400, detail="Somente consultas SELECT são permitidas.")

    # Impõe LIMIT se não existir
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

    # 1) NL -> SQL
    try:
        sql = vn.generate_sql(question)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao gerar SQL: {e}")

    # 2) Guardrails
    sql = guardrails(sql, max_rows)

    # 3) Executa SQL
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao executar SQL no banco: {e}")

    # 4) Exporta arquivo
    if out_format == "xlsx":
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="resultado")
        file_bytes = buf.getvalue()
        file_name = "resultado.xlsx"
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        file_bytes = df.to_csv(index=False).encode("utf-8")
        file_name = "resultado.csv"
        mime = "text/csv"

    file_b64 = base64.b64encode(file_bytes).decode("ascii")

    # Texto curto para WhatsApp
    answer_text = f"OK. Retornei {len(df)} linhas e {len(df.columns)} colunas. Segue arquivo."

    return {
        "answer_text": answer_text,
        "sql": sql,
        "rows": len(df),
        "cols": list(df.columns),
        "file_name": file_name,
        "file_mime": mime,
        "file_base64": file_b64,
    }
