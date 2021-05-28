from doc_ai_parser import DocParser
import gcsfs
import pandas as pd

fs = gcsfs.GCSFileSystem("ons-companies-house-dev", token="/home/dylan_purches/keys/data_key.json")

sheets = fs.ls("ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/bs_pdfs")
names = [((t.split("/")[-1]).split(".")[0])[:-3] for t in sheets]

final_df = pd.DataFrame(columns=["line_height", "space_width"])
print(sheets)
fails = []
for i in range(len(sheets)):

    df_dict = {"line_height":[], "space_width":[]}
    doc_parser = DocParser(fs)
    doc_parser.parse_document(sheets[i],
                                "/home/dylan_purches/keys/data_key.json",
                                "ons-companies-house-dev")
    doc_parser.tokens_to_df()

    temp_df = doc_parser.token_df
    for index in temp_df.index:
        if temp_df.loc[i,"detected_break_type"] == "SPACE":
            df_dict["line_height"].append(eval(temp_df.loc[i,"normed_vertices"])[3][1] - eval(temp_df.loc[i,"normed_vertices"])[0][1])
            df_dict["space_width"].append(eval(doc_parser.token_df.loc[i+1, "normed_vertices"])[0][0] - eval(doc_parser.token_df.loc[i, "normed_vertices"])[1][0])
    final_df = final_df.append(pd.DataFrame.from_dict(df_dict))
