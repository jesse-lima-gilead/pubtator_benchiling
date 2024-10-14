class SummarizeArticle:
    def __init__(self, input_file_path: str):
        self.input_file_path = input_file_path
        with open(self.input_file_path, "r") as file:
            # ToDo: Read only the <passage> tags from the input file
            self.article_text = file.read()

    def summarize_article(self):
        summary = ""
        # ToDo: Summarize the article_text
        return summary
