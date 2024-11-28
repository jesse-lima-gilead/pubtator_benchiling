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
