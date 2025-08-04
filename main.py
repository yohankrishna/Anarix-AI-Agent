import os
import sqlite3
import pandas as pd
import google.generativeai as genai
import json
import traceback
import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

# Load .env
load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    raise ValueError("GOOGLE_API_KEY not found in .env")

genai.configure(api_key=api_key)

# FastAPI app
app = FastAPI(title="E-commerce AI Agent")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins, including your local HTML file
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods like GET, POST
    allow_headers=["*"],  # Allows all headers
)

db_file = "ecommerce.db"

# Schema context for Gemini
db_schema = """
CREATE TABLE product_eligibility_table (
  item_id INTEGER,
  eligibility_datetime_utc TEXT,
  eligibility BOOLEAN,
  message TEXT
);

CREATE TABLE product_ad_sales_metrics (
  date TEXT,
  item_id INTEGER,
  ad_sales REAL,
  impressions INTEGER,
  ad_spend REAL,
  clicks INTEGER,
  units_sold INTEGER
);

CREATE TABLE product_total_sales_metrics (
  date TEXT,
  item_id INTEGER,
  total_sales REAL,
  total_units_ordered INTEGER
);
"""

# Pydantic model
class QueryRequest(BaseModel):
    question: str = Field(example="Enter Your Question Here !")

# Format data for SSE
def format_stream_chunk(event_type: str, data: str):
    return f"data: {json.dumps({'event': event_type, 'data': data})}\n\n"

# Response generator
async def response_generator(question: str):
    try:
        # Generating the SQL
        yield format_stream_chunk("status", "Generating SQL Query")
        prompt = f"""
        Given this schema:
        {db_schema}
        Convert the question to a valid SQLite SQL.
        Only respond with raw SQL, no explanation, no code fences.

        Question: {question}
        SQL:
        """

        model = genai.GenerativeModel("gemini-2.5-flash")
        response = await model.generate_content_async(prompt)

        # Remove the markdown fences.
        sql = response.text.strip().replace('```sql', '').replace('```', '').strip()

        # For Debugging
        # print("Cleaned SQL to execute:\n", sql)

        # Yield the corrected SQL
        yield format_stream_chunk("sql", sql)

        # Executing the SQL
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query(sql, conn)
        conn.close()

        # Generating summary
        summary_prompt = f"The user asked: {question}\nHere is the result:\n{df.to_string(index=False)}\nGive a human-readable summary."
        summary_response = await model.generate_content_async(summary_prompt)
        summary_text = (
            summary_response.text
            .replace("**", "")
            .replace("*", "")
            .replace("\n", " ")
            .strip()
        )
        yield format_stream_chunk("text", summary_text)

        yield format_stream_chunk("done", "Done.")
    except Exception as e:
        traceback.print_exc()
        yield format_stream_chunk("error", f"Error: {str(e)}")

# POST endpoint with SSE streaming
@app.post("/ask")
async def ask(request: QueryRequest):
    return StreamingResponse(response_generator(request.question), media_type="text/event-stream")

@app.get("/")
def root():
    return {"message": "E-commerce AI Agent is running."}

if __name__ == "__main__":
    print("Swagger: http://localhost:8000/docs")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
