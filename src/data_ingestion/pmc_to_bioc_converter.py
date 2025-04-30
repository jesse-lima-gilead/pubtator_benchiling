from datetime import date
import bioc
from src.pubtator_utils.logs_handler.logger import SingletonLogger
from src.pubtator_utils.file_handler.base_handler import FileHandler
import copy

# Initialize the logger
logger_instance = SingletonLogger()
logger = logger_instance.get_logger()


def convert_pmc_to_bioc(pmc_file: str, bioc_output_dir: str, file_handler: FileHandler):
    # Parse the input PMC XML
    tree = file_handler.parse_xml_file(pmc_file)
    # tree = ET.parse(pmc_file)
    root = tree.getroot()

    # Create a BioC collection
    bioc_collection = bioc.BioCCollection()
    bioc_collection.source = "PubMed Central"
    bioc_collection.date = str(date.today())  # Set the current date
    bioc_collection.key = "Lung Cancer Articles"

    # Loop through articles in PMC XML
    for article in root.findall("article"):
        bioc_document = bioc.BioCDocument()

        # Extract and set document-level metadata (e.g., PMID, PMC ID)
        article_meta = article.find("front").find("article-meta")
        pmcid = article_meta.find("article-id[@pub-id-type='pmc']")
        bioc_document.id = pmcid.text if pmcid is not None else "Unknown"

        # Extract metadata from <front> (article metadata, abstracts, history, permissions)
        extract_front_data(article_meta, bioc_document)

        # Extract full text from <body>
        body = article.find("body")
        if body is not None:
            extract_body_data(body, bioc_document)

        # Extract data from <back> (source data, peer review info, supplementary info, acknowledgements)
        back = article.find("back")
        if back is not None:
            extract_back_data(back, bioc_document)

        # Extract data from <floats-group> (fig, table data)
        floats_group = article.find("floats-group")
        if floats_group is not None:
            extract_floats_group_data(floats_group, bioc_document)

        # Add document to the BioC collection
        bioc_collection.add_document(bioc_document)

    # Write BioC documents to individual XML files
    save_bioc_file(bioc_collection, bioc_output_dir, file_handler)


def extract_front_data(article_meta, bioc_document):
    # Extract metadata from <front> (article metadata, abstracts, history, permissions)
    title = article_meta.find("title-group/article-title")
    bioc_document.infons["title"] = (
        "".join(title.itertext()) if title is not None else "Unknown Title"
    )

    # Extract article identifiers (pmid, pmc_id, doi, etc.)
    for article_id in article_meta.findall("article-id"):
        id_type = article_id.get("pub-id-type", "unknown")
        bioc_document.infons[f"article_id_{id_type}"] = article_id.text

    # Extract article categories
    article_categories = article_meta.find("article-categories")
    if article_categories is not None:
        for subj_group in article_categories.findall("subj-group"):
            subject = subj_group.find("subject")
            if subject is not None:
                bioc_document.infons[
                    f'subject_{subj_group.get("subj-group-type", "unknown")}'
                ] = subject.text

    # Extract multiple abstract tags
    for abstract in article_meta.findall("abstract"):
        abstract_id = abstract.get("id", "Unknown")  # Handle abstract ID if available
        bioc_passage = bioc.BioCPassage()
        bioc_passage.infons["type"] = f"abstract_{abstract_id}"
        bioc_passage.text = "".join(abstract.itertext())
        bioc_document.add_passage(bioc_passage)

    # Extract publication date
    pub_date = article_meta.find("pub-date")
    if pub_date is not None:
        pub_day = (
            pub_date.find("day").text if pub_date.find("day") is not None else "Unknown"
        )
        pub_month = (
            pub_date.find("month").text
            if pub_date.find("month") is not None
            else "Unknown"
        )
        pub_year = (
            pub_date.find("year").text
            if pub_date.find("year") is not None
            else "Unknown"
        )
        bioc_document.infons["publication_date"] = f"{pub_year}-{pub_month}-{pub_day}"

    # Extract history
    history = article_meta.find("history")
    if history is not None:
        for date in history.findall("date"):
            bioc_document.infons[f'history_{date.attrib["date-type"]}'] = "".join(
                date.itertext()
            )

    # Extract permissions
    permissions = article_meta.find("permissions")
    if permissions is not None:
        for perm in permissions.findall("license"):
            bioc_document.infons["license"] = "".join(perm.itertext())

    # Extract funding information
    funding_group = article_meta.find("funding-group")
    if funding_group is not None:
        for funding in funding_group.findall("award-group"):
            funder_name = funding.find("funder-name")
            award_id = funding.find("award-id")
            bioc_document.infons["funder"] = (
                funder_name.text if funder_name is not None else "Unknown Funder"
            )
            bioc_document.infons["award_id"] = (
                award_id.text if award_id is not None else "Unknown Award ID"
            )


def extract_body_data(body, bioc_document):
    # Find all <boxed-text> elements in <body>
    for boxed_text in body.findall("boxed-text"):
        titles = []
        for p in boxed_text.findall(".//title"):
            title = "".join(p.itertext())
            titles.append(title)
        if titles:
            section_title = ", ".join(titles)
        else:
            section_title = "boxed_text"
        # Extract all <p> elements inside <boxed-text> (at any level)
        paragraphs = []
        for p in boxed_text.findall(".//p"):
            section_text = "".join(p.itertext())
            paragraphs.append(section_text)

        if paragraphs:  # Only add if there's text
            bioc_passage = bioc.BioCPassage()
            bioc_passage.infons["type"] = section_title
            bioc_passage.text = "\n".join(paragraphs)  # Join paragraphs with newline
            bioc_document.add_passage(bioc_passage)

    # Extract standalone <p> elements (outside <sec>)
    for p in body.findall("p"):
        section_title = "body_text"
        section_text = "".join(p.itertext())

        # Create a passage for each section of the body
        bioc_passage = bioc.BioCPassage()
        bioc_passage.infons["type"] = section_title
        bioc_passage.text = section_text
        bioc_document.add_passage(bioc_passage)

    # Find all <fig> elements in <body>
    for fig in body.findall("fig"):
        section_title = (
            fig.find("label").text if fig.find("label") is not None else "figure_text"
        )
        # Extract all <p> elements inside <fig> (at any level)
        paragraphs = []
        for p in fig.findall(".//p"):
            section_text = "".join(p.itertext())
            paragraphs.append(section_text)

        if paragraphs:  # Only add if there's text
            bioc_passage = bioc.BioCPassage()
            bioc_passage.infons["type"] = section_title
            bioc_passage.text = "\n".join(paragraphs)  # Join paragraphs with newline
            bioc_document.add_passage(bioc_passage)

    # Extract the full text from <body> with multiple sections
    for sec in body.findall("sec"):
        section_title = (
            sec.find("title").text
            if sec.find("title") is not None
            else "Untitled Section"
        )
        # deep-copy so you don’t mutate the original tree
        sec_copy = copy.deepcopy(sec)
        if sec_copy.find("title") is not None:
            sec_copy.remove(sec_copy.find("title"))

        # now this contains everything *except* the title
        section_text = "".join(sec_copy.itertext()).strip()

        # Create a passage for each section of the body
        bioc_passage = bioc.BioCPassage()
        bioc_passage.infons["type"] = section_title
        bioc_passage.text = section_text
        bioc_document.add_passage(bioc_passage)


def extract_floats_group_data(floats_group, bioc_document):
    # Find all <fig> elements in <floats-group>
    for fig in floats_group.findall("fig"):
        section_title = (
            fig.find("label").text if fig.find("label") is not None else "figure_text"
        )
        # Extract all <p> elements inside <fig> (at any level)
        paragraphs = []
        for p in fig.findall(".//p"):
            section_text = "".join(p.itertext())
            paragraphs.append(section_text)

        if paragraphs:  # Only add if there's text
            bioc_passage = bioc.BioCPassage()
            bioc_passage.infons["type"] = section_title
            bioc_passage.text = "\n".join(paragraphs)  # Join paragraphs with newline
            bioc_document.add_passage(bioc_passage)

    # Find all <boxed-text> elements in <floats-group>
    for boxed_text in floats_group.findall("boxed-text"):
        titles = []
        for p in boxed_text.findall(".//title"):
            title = "".join(p.itertext())
            titles.append(title)
        if titles:
            section_title = ", ".join(titles)
        else:
            section_title = "boxed_text"
        # Extract all <p> elements inside <boxed-text> (at any level)
        paragraphs = []
        for p in boxed_text.findall(".//p"):
            section_text = "".join(p.itertext())
            paragraphs.append(section_text)

        if paragraphs:  # Only add if there's text
            bioc_passage = bioc.BioCPassage()
            bioc_passage.infons["type"] = section_title
            bioc_passage.text = "\n".join(paragraphs)  # Join paragraphs with newline
            bioc_document.add_passage(bioc_passage)

    # Find all <table-wrap> elements in <floats-group>
    for table in floats_group.findall("table-wrap"):
        section_title = (
            table.find("label").text
            if table.find("label") is not None
            else "table_text"
        )

        # # Extract table rows and columns
        # table_text = []
        #
        # # Find the <table> inside <table-wrap>
        # table_data = table.find("table")
        # if table_data is not None:
        #     for row in table_data.findall(".//tr"):  # Find all table rows
        #         columns = [col.text.strip() if col.text else "" for col in row.findall("th") + row.findall("td")]
        #         table_text.append("\t".join(columns))  # Join columns with tab for readability

        # Extract all <p> elements inside <table-wrap> (at any level)
        paragraphs = []
        for p in table.findall(".//p"):
            section_text = "".join(p.itertext())
            paragraphs.append(section_text)
        if paragraphs:  # Only add if there's text
            bioc_passage = bioc.BioCPassage()
            bioc_passage.infons["type"] = section_title
            bioc_passage.text = "\n".join(paragraphs)  # Join paragraphs with newline
            bioc_document.add_passage(bioc_passage)


def extract_back_data(back, bioc_document):
    # Extract acknowledgements, supplementary info, peer review, and source data from <back>
    acknowledgements = back.find("ack")
    if acknowledgements is not None:
        bioc_passage = bioc.BioCPassage()
        bioc_passage.infons["type"] = "acknowledgements"
        bioc_passage.text = "".join(acknowledgements.itertext())
        bioc_document.add_passage(bioc_passage)

    # # Extract multiple supplementary material tags
    # supplementary_material = back.findall('supplementary-material')
    # for idx, supplementary in enumerate(supplementary_material, start=1):
    #     bioc_passage = bioc.BioCPassage()
    #     bioc_passage.infons['section'] = f"supplementary_info_{idx}"
    #     bioc_passage.text = ''.join(supplementary.itertext())
    #     bioc_document.add_passage(bioc_passage)
    #
    # # Extract peer review info
    # peer_review = back.find('notes[@notes-type="reviewer-comments"]')
    # if peer_review is not None:
    #     bioc_passage = bioc.BioCPassage()
    #     bioc_passage.infons['section'] = "peer_review"
    #     bioc_passage.text = ''.join(peer_review.itertext())
    #     bioc_document.add_passage(bioc_passage)
    #
    # # Extract references
    # ref_list = back.find('ref-list')
    # if ref_list is not None:
    #     bioc_passage = bioc.BioCPassage()
    #     bioc_passage.infons['section'] = "references"
    #     bioc_passage.text = ''.join(ref_list.itertext())
    #     bioc_document.add_passage(bioc_passage)


def clean_text(text):
    """
    Clean up text by splitting based on newlines, removing empty lines, and trimming spaces within each line.
    Args:
        text (str): The raw text to be cleaned.
    Returns:
        str: The cleaned-up text.
    """
    # Split text into lines
    lines = text.split("\n")

    # Process each line: remove extra white spaces and remove empty lines
    cleaned_lines = [line.strip() for line in lines if line.strip() != ""]

    # Join cleaned lines back into a single text with single newline separation
    cleaned_text = " ".join(cleaned_lines)

    return cleaned_text


def save_bioc_file(pubmed_collection, bioc_output_dir, file_handler):
    for document in pubmed_collection.documents:
        # Create a new BioC collection for each document
        single_doc_collection = bioc.BioCCollection()
        single_doc_collection.source = pubmed_collection.source
        single_doc_collection.date = pubmed_collection.date
        single_doc_collection.add_document(document)

        for passage in document.passages:
            if passage.text:
                original_text = passage.text
                processed_text = clean_text(original_text)
                passage.text = processed_text

        # Convert the collection to BioC XML format
        # bioc_xml = bioc.dumps(single_doc_collection, pretty_print=True)
        bioc_xml = bioc.dumps(single_doc_collection)

        # Use pmc_id as the filename
        bioc_file_name = f"PMC_{document.id}.xml"
        file_path = file_handler.get_file_path(bioc_output_dir, bioc_file_name)
        # file_path = os.path.join(bioc_output_dir, f"PMC_{document.id}.xml")

        file_handler.write_file(file_path, bioc_xml)
        # os.makedirs(os.path.dirname(file_path), exist_ok=True)
        #
        # with open(file_path, "w", encoding="utf-8") as f:
        #     f.write(bioc_xml)

        logger.info(f"BioC XML file saved to {file_path}")


# if __name__ == "__main__":
#     # Define paths
#     pmc_file = '../../data/golden_dataset/staging/pmc_xml/PMC_2480972.xml'  # Path to the PMC XML file
#     output_bioc_file = '../../data'  # Path for the output BioC XML file
#
#     # Convert the PMC XML file to BioC format
#     convert_pmc_to_bioc(pmc_file, output_bioc_file)
