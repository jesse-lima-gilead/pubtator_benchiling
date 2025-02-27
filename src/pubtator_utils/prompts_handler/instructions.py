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

LLM_RESPONSE_INSTRUCTIONS = """
Do's:
1. Use the provided context (text passages and metadata) to generate responses.
2. Highlight and mention the relevant parts of the context you used as citations.
3. Provide the response in the format specified below:
   - "Response": A clear and concise answer to the user query.
   - "Citations": A list of citations, including the highlighted context and its corresponding PMC Id.
4. Always base your answers strictly on the provided context.

Don'ts:
1. Generate information that is not present in the provided context.
2. Omit citations from the response if context is used.
"""
