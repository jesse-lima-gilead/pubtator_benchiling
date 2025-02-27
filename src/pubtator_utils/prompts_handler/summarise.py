# SUMMARISE = """
# You are given with a chat history of a user with an AI assistant for HR Policies.
# Your task is to summarise this chat and return a summarised chat.
#
# Guidelines to summarize the chat history:
# 1. Extract key area of user query. It can be related to some leave type, some policy etc.
# 2. Identify recurring themes or topics discussed.
# 3. Highlight important information exchanged between roles (human and AI).
# 4. Provide a concise overview of the entire conversation.
# 5. DO NOT generate response like 'Here is a summary of the key points from the chat:'. Keep ONLY the summary in response.
# 6. Don't keep the full AI response in summary. Take only the key pointers.
# 7. Summary should be of at max 250 words.
# 8. Give more weightage to the most recent user messages over older messages. More late a message appears in the input context, more recent it is.
# """


SUMMARISE = """
You are given with a chat history of a user with an AI assistant .
Your task is to summarise this chat and return a summarised chat.

Guidelines to summarize the chat history:
1. Extract key area of user query..
2. Identify recurring themes or topics discussed.
3. Highlight important information exchanged between roles (human and AI).
4. Provide a concise overview of the entire conversation.
5. DO NOT generate response like 'Here is a summary of the key points from the chat:'. Keep ONLY the summary in response.
6. Don't keep the full AI response in summary. Take only the key pointers.
7. Summary should be of at max 250 words.
8. Give more weightage to the most recent user messages over older messages. More late a message appears in the input context, more recent it is.
"""
