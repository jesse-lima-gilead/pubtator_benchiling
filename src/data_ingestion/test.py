from Bio import Entrez


class PubMedIngestion:
    def __init__(self, email):
        Entrez.email = email

    def search_pubmed(self, query, retmax=5):
        handle = Entrez.esearch(db="pubmed", term=query, retmax=retmax)
        record = Entrez.read(handle)
        return record["IdList"]

    def fetch_pubmed_articles(self, id_list):
        handle = Entrez.efetch(db="pubmed", id=id_list, retmode="xml")
        pubmed_data = handle.read()
        return pubmed_data

    def fetch_full_text_from_pmc(self, pmid):
        handle = Entrez.elink(dbfrom="pubmed", db="pmc", LinkName="pubmed_pmc", id=pmid)
        record = Entrez.read(handle)

        if record[0]["LinkSetDb"]:
            pmc_id = record[0]["LinkSetDb"][0]["Link"][0]["Id"]
            return f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}/"
        else:
            return None


if __name__ == "__main__":
    print("Execution Started")
    # Usage Example
    pmid = "39228012"  # Example PubMed ID
    email = "your.email@example.com"
    pubmed_ingestion = PubMedIngestion(email)

    # Search for articles
    id_list = pubmed_ingestion.search_pubmed("cancer", retmax=5)
    print(f"PMID List: {id_list}")

    # Fetch PubMed articles
    pubmed_data = pubmed_ingestion.fetch_pubmed_articles(id_list)
    print(f"PubMed Data: {pubmed_data}")

    # Fetch Full Text from PMC
    for pmid in id_list:
        pmc_link = pubmed_ingestion.fetch_full_text_from_pmc(pmid)
        if pmc_link:
            print(f"Full text available at: {pmc_link}")
        else:
            print(f"No full text available for PMID: {pmid}")
