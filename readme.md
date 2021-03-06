# Companies House Accounts

This project aims to independently source XBRL and PDF data from the Companies House accounts website, and produce a merged data
set of processed data for build and archive. The high-level flow for each process is as follows:

### XBRL
1.	Web-scrape all XBRL data from the companies house accounts website. 
2.	Unpack xbrl files 
3.  Process and parse xml data / html method (BeautifulSoup), converting them into there csv equivalents.
4.  Append xbrl csv file on an annual basis
5.  Convert XBRL melt tables to Pivot tables.
6.  Filter csv files to produce subsets of the xbrl tags taht are required for various internal ONS stakeholders. 
### PDF
1.	Web-scrape filled accounts from companies house as pdf data.
2.	Convert each page of the pdf into separate images.
3.	Create a model of a Cascade Classifier 
4.	Apply Classifier to identify and extract the cover page and the balance sheet from converted filled accounts data.
5.	Implement Classifier performance metrics to determine the accuracy and precision of the Classifier.
6.	Apply Optical Character Recognition (OCR) to images that have been classified in step 5 to convert into text data.
7.	Apply Natural Language Processes (NLP) to text data extracted in step 7 to extract patterns from the raw text data.
8.	Merge processed XBRL data from step 2 with data generated from step 8.

## Webscraping Policy 
The webscraping done in this project is achieved by utilsing [Scrapy](https://scrapy.org/) and strictly adheres to the [ONS Web Scraping Policy](https://www.ons.gov.uk/aboutus/transparencyandgovernance/datastrategy/datapolicies/webscrapingpolicy).
For further information on Scrapy please see the following links:
- [Scrapy Tutorial, Web Scraping Using Python](https://www.accordbox.com/blog/scrapy-tutorial-series-web-scraping-using-python)
- [Datacamp, Web Scraping with Python](https://www.datacamp.com/courses/web-scraping-with-python)

Noting that the deployment of 'spiders' in this implimentation of Scrapy is automated so we will not cover how to initialise them. 
## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install this project's required modules and dependencies.

```bash
pip3 install {module}
```

## Usage

Load main pipeline, and call subsidary modules.

```python
from src.data_processing.cst_data_processing import DataProcessing
from src.classifier.cst_classifier import Classifier
from src.performance_metrics.binary_classifier_metrics import BinaryClassifierMetrics
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
