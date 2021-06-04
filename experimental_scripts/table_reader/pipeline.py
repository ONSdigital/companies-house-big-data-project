import gcsfs
import pandas as pd
import numpy as np
import statistics as stats

from line_reader import LineReader
from table_identifier import TableIdentifier
from table_fitter import TableFitter
#from pdf_annotator import PDFAnnotator
from doc_ai_parser import DocParser
from table_to_df import Table2Df

fs = gcsfs.GCSFileSystem("ons-companies-house-dev", token="/home/andre_faul/keys/dev_key.json")

# sheets = fs.ls("ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/bs_pdfs")
# names = [((t.split("/")[-1]).split(".")[0])[:-3] for t in sheets]

# print(sheets)
# fails = []
# for i in range(len(sheets)):
#     try:
#         doc_parser = DocParser(fs)
#         doc_parser.parse_document(sheets[i],
#                                   "/home/dylan_purches/keys/data_key.json",
#                                   "ons-companies-house-dev")
#         doc_parser.tokens_to_df()

#         # Implements the line reader module
#         lines_data = LineReader(doc_parser.token_df)
#         lines_data.add_first_vertex()
#         lines_data.get_line_nums()
#         lines_data.group_within_line()
#         lines_data.add_first_vertex()

#         # Implement the table identifier module
#         structs_data = TableIdentifier(lines_data.data)
#         structs_data.detect_table()

#         # Implement the table fitter module
#         table_data = TableFitter(structs_data.data)
#         table_data.clean_values()
#         table_data.get_first_col()
#         table_data.get_header_row()
#         table_data.get_other_columns_v2()
#         table_data.remove_excess_lines()

#         # Create an annotated pdf
#         # annotator = PDFAnnotator(sheets[i], gcp=True)
#         # annotator.pdf_to_png()
#         # annotator.annotate_table("ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/dev_visual_outputs/" + names[i]+"_table.jpg",
#         #                          table_data)

#         to_df = Table2Df(table_data, fs)
#         to_df.get_final_df()
#         df = to_df.df
#         df.to_csv("gs://ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/processed_csv_outputs/new_outputs/"+names[i]+"_final_df.csv",
#         index=False)        
#         fs.setxattrs("gs://ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/processed_csv_outputs/new_outputs/"+names[i]+"_final_df.csv",
#         content_type = "text/csv")

#     except:
#         fails.append(names[i])

# df = pd.read_csv("gs://ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/doc_ai_token_dfs/04391694_active_bs_tokens.csv")
#
doc_parser = DocParser(fs) #02460543_dormant_bs 04391694_active_bs SC564642_bs astra_zeneca_bs 03541703_bs 04391694_active_bs   03875584_bs  (04416996_bs mod 0 error)
doc_parser.parse_document("ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/bs_pdfs/astra_zeneca_bs.pdf",
                          "/home/andre_faul/keys/dev_key.json",
                          "ons-companies-house-dev")
doc_parser.tokens_to_df()
# Implements the line reader module
lines_data = LineReader(doc_parser.token_df)
lines_data.add_first_vertex()
lines_data.get_line_nums()
lines_data.group_within_line()
lines_data.add_first_vertex()

# Implement the table identifier module
structs_data = TableIdentifier(lines_data.data)
structs_data.detect_table()

# Implement the table fitter module
table_data = TableFitter(structs_data.data)
table_data.clean_values()
table_data.get_first_col()
table_data.get_header_row()
table_data.remove_excess_lines()
table_data.get_other_columns()
#table_data.remove_excess_lines()
table_data.group_value_points()

#print(table_data.data)

to_df = Table2Df(table_data, fs)

# print(to_df.data)

to_df.get_final_df()

# print(to_df.data)
# # Create an annotated pdf
# annotator = PDFAnnotator("ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/bs_pdfs/"
#                          + "diageo_bs" + ".pdf", gcp=True)
# annotator.pdf_to_png()
# annotator.annotate_table("ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/visual_outputs/" + "v3_test"+"_table.jpg",
#                          table_data)