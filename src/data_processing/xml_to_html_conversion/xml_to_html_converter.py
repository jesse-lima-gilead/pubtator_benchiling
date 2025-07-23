from src.pubtator_utils.file_handler.base_handler import FileHandler
from src.pubtator_utils.file_handler.file_handler_factory import FileHandlerFactory
from src.pubtator_utils.config_handler.config_reader import YAMLConfigLoader
from src.pubtator_utils.logs_handler.logger import SingletonLogger

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


class XmlToHtmlConverter:
    def __init__(
        self,
        workflow_id: str,
        paths_config: dict[str, str],
        file_handler: FileHandler,
        xml_to_html_template_path: str,
    ):
        """
        Initialize the converter with source and target directories and a file handler.

        :param workflow_id: Unique identifier for the JIT workflow, used to maintain separate path for each flow.
        :param paths_config: dict with keys:
            - 'annotations_merged_path': directory containing XML files
            - 'static_html_path': directory to write HTML files
        :param file_handler: FileHandler implementation for I/O operations
        :param xml_to_html_template_path: path to read template html file
        """
        self.input_dir = paths_config["annotations_merged_path"].replace(
            "{workflow_id}", workflow_id
        )
        self.output_dir = paths_config["static_html_path"].replace(
            "{workflow_id}", workflow_id
        )
        self.file_handler = file_handler
        self.local_file_handler = FileHandlerFactory.get_handler("local")
        self.html_template_path = xml_to_html_template_path

    def xml_html_converter(self):
        """
        Convert all XML files in the input directory to HTML using the template.
        """
        html_template_content = self.local_file_handler.read_file(
            self.html_template_path
        )
        file_names = self.file_handler.list_files(self.input_dir)

        for file_name in file_names:
            if not file_name.endswith(".xml"):
                continue

            logger.info(f"Processing file: {file_name}")
            try:
                xml_file_path = self.file_handler.get_file_path(
                    self.input_dir, file_name
                )

                # Read the Article XML Content
                xml_content = self.file_handler.read_file(xml_file_path)

                # Read the HTML Template
                html_content = html_template_content.replace(
                    "{{XML_CONTENT}}", xml_content
                )

                # Replace the template with actual XML
                html_file_name = file_name.replace(".xml", ".html")

                # Write the converted HTML to output
                self._write_html_file(html_file_name, html_content)
            except Exception as e:
                logger.error(f"Failed to convert '{file_name}': {e}", exc_info=True)

        logger.info("XML to HTML conversion complete.")

    def _write_html_file(self, file_name, html_content):
        """
        Write the merged BioC document to the output directory.

        :param file_name: Name of the output file.
        :param html_content: HTML Content of the Article.
        """
        output_path = self.file_handler.get_file_path(self.output_dir, file_name)
        logger.info(f"Writing merged file to: {output_path}")
        self.file_handler.write_file(output_path, html_content)


# Example usage
if __name__ == "__main__":
    # Initialize the config loader
    config_loader = YAMLConfigLoader()

    # Retrieve paths config
    paths_config = config_loader.get_config("paths")
    storage_type = paths_config["storage"]["type"]

    # Get file handler instance from factory
    file_handler = FileHandlerFactory.get_handler(storage_type)
    # Retrieve paths from config
    paths = paths_config["storage"][storage_type]

    html_converter = XmlToHtmlConverter(paths, file_handler)
    html_converter.xml_html_converter()
