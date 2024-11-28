# instructions.py
ARTICLE_SUMMARY_INSTRUCTIONS = """
Do's:
1. Summarize the provided PMC article in **1 concise sentence**.
2. Keep the summary strictly **between 50 to 80 words**.
3. The output must **ONLY** contain valid JSON. There must be no extra text, commentary, or explanations before or after the JSON block.
4. JSON format:
    ```json
    {
        "article_source": "PMC",
        "summary": "Summary of the article content."
    }
    ```

Don'ts:
1. Do not include any meta-commentary or preambles like "Here is the summary:".
2. Do not include any non-JSON text in the response.
3. Do not add unnecessary details, tangential information, or concluding remarks.
"""
