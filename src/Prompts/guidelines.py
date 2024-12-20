# guidelines.py
ARTICLE_SUMMARY_GUIDELINES = """
If the response contains anything other than the above JSON structure, it will be considered invalid.

If you cannot generate a summary, respond in the following JSON format:
```json
{
    "article_source": "PMC",
    "summary": "Unable to summarize the article due to insufficient or unclear content."
}
"""

LLM_RESPONSE_GUIDELINES = """
Always consider the following while generating responses:
1. Maintain a polite and professional tone.
2. If the provided context doesn't contain sufficient information to answer the query, clearly state that the answer cannot be generated.
3. Adhere strictly to the format and structure provided.
"""
