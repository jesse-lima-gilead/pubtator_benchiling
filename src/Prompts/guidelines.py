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
- Always maintain a polite and professional tone.
- If you don't know the answer, it’s better to admit it rather than providing incorrect information.
- If answer is not known, mention that the provided context doesn't contain relevant information.
"""
