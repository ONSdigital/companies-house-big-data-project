import pandas as pd
import numpy as np
import statistics as stats
import regex
import gcsfs

from line_reader import LineReader
from table_identifier import TableIdentifier
from doc_ai_parser import DocParser



class TableFitter(TableIdentifier):
    def __init__(self, df):
        TableIdentifier.__init__(self, df)
        self.columns = []
        self.notes_row = []
        self.assets_row = []
        self.dates_row = []
        self.currency_row = []

        # Header properties
        self.header_indices = []
        self.header_lines = []
        self.header_coords = []
        self.header_groups = []

        # Date/Unit properties
        self.date_headers = []
        self.unit_headers = []

    def __getitem__(self, i):
        return f"Value {i}"
        
    def clean_values(self, chars = ["\n"]):
        """
        Removes all unwanted characters (default is just newline) from
        the value column of the class data. Also sets attributes for the
        indices where we see specific values.
        Arguments:
            chars: List of strings of characters to remove
        Returns:
            None
        Raises:
            None
        """
        for char in chars:
            self.data["value"] = self.data["value"].str\
                        .replace(char, '')
        # if len(self.notes_row) == 0 :#look for currency
        # Set class attributes for the indices where we find specific values
        
        
        self.notes_row = [i for i in self.data.index
                          if self.data.loc[i, "value"].lower()
                          in ["note", "notes"]]           
        self.notes_tf = len(self.notes_row) != 0  
        
        years = range(1999,2050)
        years_two = range(0,50)       
        date_indexes = []         
        date_x = 0
        for i in self.data.index:
            contains_year = any([str(y) == self.data.loc[i, "value"].strip() for y in years or years_two]) 
            if contains_year and date_x <=2: #add check for at most 2 
                date_indexes.append(i)
                date_x+=1
        if date_x == 1:
            date_headers = date_indexes
            self.dates_row = date_headers
        elif date_x > 1: #if there are 2 dates then checks if they are sequential 
            if (int(self.data.loc[date_indexes[0] , "value"])- 1) == int(self.data.loc[date_indexes[1], "value"]): 
                date_headers = date_indexes
                self.dates_row = date_headers
            else:
                date_headers = date_indexes[0]
                self.dates_row = date_headers
        
        currency_indexes = [i for i in self.data.index if
                            len(regex.findall(r"\p{Sc}", self.data.loc[i, "value"]))]
        self.unit_headers = currency_indexes
        

        self.assets_row = [i for i in self.data.index
                           if "asset" in self.data.loc[i, "value"].lower()]
        
    
    def get_first_col(self):
        """
        Gets the indices of the first column of a table and saves
        as a class attribute. First column selected by presence of the
        string "assets".
        
        Arguments:
            None
        Returns:
            None
        Raises:
            None
        """
        # Find the distribution of coordinates in assets_row
        x_dist = self.find_alignment(self.data, self.assets_row)

        # Find all other elements that align with the assets coordinates
        aligned_dict = self.find_aligned_indices(self.data, x_dist)
        for i in aligned_dict["indices"]:
            self.data.loc[i, "column"] = int(0)
        self.columns.append(aligned_dict["indices"])

    @staticmethod
    def find_alignment(df, indices):
        """
        Returns the average alignment of the given indices. Returns a list
        of the average x coordinate of the left edge, right edge and center
        (of the boxes around the elements).
        
        Arguments:
            df:         DataFrame for which indices can be used
            indices:    Indices of elements to be considered
        Returns:
            assets_summary: List of the average left, right and central
                             x coordinate for the elements
        Raises:
            None
        """
        x_dist = [[],[],[]]
        if type(indices) != list:
            indices = [indices]

        # For each of the indices, append the x coordinate of the left, centre
        # and right of the bounding polygon
        for i in indices:
            x_dist[0].append(eval(df.loc[i, "normed_vertices"])[3][0])
            x_dist[1].append(eval(df.loc[i, "normed_vertices"])[2][0])
            x_dist[2].append(
                0.5*(eval(df.loc[i, "normed_vertices"])[3][0]
                     + eval(df.loc[i, "normed_vertices"])[2][0]))


        aligned_medians = [stats.median(x_dist[0]),
                        stats.median(x_dist[1]),
                        stats.median(x_dist[2])]
        aligned_index = None
        aligned_x_point = None
        # Compute the variance for each of these coordinates
        if len(x_dist[0]) >= 2:
            aligned_vars = [stats.variance(x_dist[0]),
                            stats.variance(x_dist[1]),
                            stats.variance(x_dist[2])]
            
            # Find the alignment by taking the index where variance is minimised
            aligned_index = aligned_vars.index(min(aligned_vars))
            aligned_x_point = stats.median(x_dist[aligned_index])
        # Return a dict of the aligned index and the median x point of the alignment
        assets_summary = {"aligned_index": aligned_index,
                            "aligned_x_point": aligned_x_point,
                            "median_points": aligned_medians}
        return assets_summary
            

    @staticmethod
    def find_aligned_indices(df, alignment, d=0.01):
        """
        Finds the indices of elements in the DataFrame aligned with
        the three x coordinates in the alignment object (the result of
        find_alignment). Decides which way to align based on the number
        of elements that correspond to each alignment.

        Arguments:
            df:         DataFrame to be considered
            alignment:  dict of alignment from find_alignment
            d:          Constraint on how close elements need to be to be
                        considered aligned
        Returns (as dict):
            indices:    The indices of the dataframe that align to the given
                        alignment object
            alignment:  Number corresponding to left right or central alignment
        Raises:
            None
        """
        # List of indices 'close' to each of the three coordinates
        indices_close = []

        # Cases for the 3 alignment options
        if alignment["aligned_index"] == 0:
            for i in df.index:
                if TableFitter.is_close(alignment["aligned_x_point"],
                                eval(df.loc[i, "normed_vertices"])[3][0],
                                dist=d):
                    indices_close.append(i)

        elif alignment["aligned_index"] == 1:
            for i in df.index:
                if TableFitter.is_close(alignment["aligned_x_point"],
                                eval(df.loc[i, "normed_vertices"])[2][0],
                                dist=d):
                    indices_close.append(i)

        elif alignment["aligned_index"] == 2:
            for i in df.index:
                if TableFitter.is_close(alignment["aligned_x_point"],
                                0.5*(eval(df.loc[i, "normed_vertices"])[2][0]
                                    + eval(df.loc[i, "normed_vertices"])[3][0]),
                                dist=d):
                    indices_close.append(i)
        elif alignment["aligned_index"] == None:
            indices_close = [[],[],[]]
            for i in df.index:            
                #print(indices_close)
                if TableFitter.is_close(alignment["median_points"][0],
                                eval(df.loc[i, "normed_vertices"])[3][0],
                                dist=d):
                    indices_close[0].append(i)
                if TableFitter.is_close(alignment["median_points"][1],
                                eval(df.loc[i, "normed_vertices"])[2][0],
                                dist=d):
                    indices_close[1].append(i)
                if TableFitter.is_close(alignment["median_points"][2],
                                0.5*(eval(df.loc[i, "normed_vertices"])[2][0]
                                        + eval(df.loc[i, "normed_vertices"])[3][0]),
                                dist=d):
                    indices_close[2].append(i)
            aligned = [j for j, k in enumerate(indices_close)
                   if len(k) == max([len(i) for i in indices_close])]
            alignment["aligned_index"] = aligned[0]
            indices_close = indices_close[aligned[0]]

        return {"indices": indices_close, "alignment":alignment["aligned_index"]}
   
    def get_header_row(self):
        """
        Obtains a list of indices corresponding to the header row. This is
        based on looking for lines near to the line containing the 'notes'
        tag which don't have elements in the first column.

        Arguments:
            None
        Returns:
            None
        Raises:
            None
        """
        # Set the current line we are considering - start with notes line
        #l = self.data.loc[self.notes_row[0], "line_num"] 
        if (len(self.notes_row) != 0):
            l = self.data.loc[self.notes_row[0], "line_num"]            
        elif (len(self.notes_row) == 0):
            l = self.data.loc[self.dates_row[0], "line_num"]   
            self.notes_row = self.dates_row            
        elif (len(self.notes_row) == 0 and len(self.dates_row == 0)):
            l = self.data.loc[self.unit_headers[0], "line_num"]      
        else:
            print("no header")

        # Add the notes line to the relevant variables  
        header_lines = [l]
        header_indices = list(self.data[self.data["line_num"] == l].index)      
        
        # Look for other header rows below the 'notes' row
        while l < self.total_lines:
            l += 1

            # Stop at the first line with an element in the first column        
            if any([(i in self.columns[0]) 
                    for i in self.data[self.data["line_num"] == l].index]):
                break
            else:
                # Add the relevant lines and indices if it is a header row
                header_lines.append(l)
                header_indices += \
                    list(self.data[self.data["line_num"] == l].index)

        # Start from the 'notes' line and look for header rows above 
        #l = self.data.loc[self.notes_row[0], "line_num"]  
        if (len(self.notes_row) != 0):
            l = self.data.loc[self.notes_row[0], "line_num"]            
        elif (len(self.notes_row) == 0):
            l = self.data.loc[self.dates_row[0], "line_num"]   
            self.notes_row = self.dates_row            
        elif (len(self.notes_row) == 0 and len(self.dates_row == 0)):
            l = self.data.loc[self.unit_headers[0], "line_num"]      
        else:
            print("no header")

    
        y0 = stats.median(self.data[self.data["line_num"] == l]["first_y_vertex"])
        y1 = 0
        while l > min(self.data["line_num"]):
            l -= 1
            h = stats.median(self.data[self.data["line_num"] == l]["height"])
            y1 = stats.median(self.data[self.data["line_num"] == l]["first_y_vertex"])
            if any([(i in self.columns[0])
                    for i in self.data[self.data["line_num"] == l].index]):
                break
            elif y0 - y1 > 2*h:#1.5
                break
            else:
                header_lines.append(l)
                header_indices += \
                    list(self.data[self.data["line_num"] == l].index)
                y0 = y1
        self.header_lines = header_lines
        self.header_indices = header_indices
        self.header_groups = self.group_header_points(self.data,
                                                      self.header_indices)
        
        self.header_coords = \
            [self.find_alignment(self.data, i)["median_points"]
             for i in self.header_groups]
        
        print(len(header_lines), " header lines have been detected")

    def get_other_columns(self,thresh=0.95): 
        """
        Function to group the indices of columns other than the first, based
        on elements which are aligned with the elements in the header row.

        Arguments:
            None
        Returns:
            exceptions: a list containing the indices of elements that are not 
        Raises:
            None
        """

        # Don't consider elements in the first column
        #other_cols_df = self.data.drop(self.columns[0])

        # Add a list to the columns attribute for each in the header row
        #for i in range(len(self.header_coords)):
        #    self.columns.append([])
        #exceptions = []

        # For each element not in a column add the index to a column
        # for i in other_cols_df.index:
        #     col_to_fit = \
        #         self.find_closest_col(other_cols_df, self.header_coords, i)
        #     if col_to_fit != None:
        #         self.data.loc[i, "column"] = int(col_to_fit)
        #         self.columns[col_to_fit].append(i)
        #     else:
        #         exceptions.append(i)

        #print('This is what it looked like',self.data)

        # determine if any residual elements are aligned
        exception_aligned = TableFitter.group_header_points(other_cols_df, exceptions)
        print(exception_aligned)
        
        # evaluate residual elements and append a new column if 
        # a critera is met:
        # (more than 2 elements, high confidence or if any value is a digit)
        #check value_index and check for anomolies ####
        for x in exception_aligned:
            c = max(self.data.column)
            print('Original C',c)
            if len(x) > 1:
                c += 1
                self.data.loc[[i for i in x], "column"] = int(c)
            else:
                if self.data.loc[x[0],"confidence"] > thresh:
                    c += 1
                    self.data.loc[x[0], "column"] = int(c)

                elif any(str.isdigit(c) for c in self.data.loc[x[0],'value']):
                    c += 1
                    self.data.loc[x[0], "column"] = int(c)
                else:
                    pass

        #print('Did i do anything?', self.data)

        return()
    """
    def headerless_column(self,exceptions):
        
        Function to add any exception elements in the scraped 
        data, if are headerless columns of data. 
        Uses a condifence threshold and the number of allined elements
        to determine whetehr the data is an additional column 

        Arguments:
            execptions: 
        Returns:
            None
        Raises:
            None
        
    """
    
    def get_other_columns_v2(self):
        """
        Function to group the indices of columns other than the first, without
        using the detected header as a reference.

        Arguments:
            None
        Returns:
            None
        Raises:
            None
        """

        # Don't consider elements in the first column
        other_cols_df = self.data.drop(self.columns[0]).sort_values(["line_num", "first_x_vertex"], ascending=[True, True])

        columns_data = []

        for index, row in other_cols_df.iterrows():
            v1, v2 = eval(row["normed_vertices"])[3][0], eval(row["normed_vertices"])[2][0]
            for col in columns_data:
                if (v2 > col["xs"][0]) & (v1 < col["xs"][1]):
                    col["indices"].append(index)
                    col["xs"] = [min(v1, col["xs"][0]), max(v2, col["xs"][1])]
                    break
            else:
                columns_data.append({"xs":[v1, v2], "indices":[index]})
        if len(columns_data) > len(self.header_groups):
            columns_data = sorted(columns_data, key = lambda k: len(k["indices"]))[(len(columns_data) - len(self.header_groups)):]
        elif len(columns_data) < len(self.header_groups):
            print("Not enough columns have been detected")
            return 0

        columns_data = sorted(columns_data, key=lambda k: k["xs"][0])
        self.columns += [col["indices"] for col in columns_data]

        for i, col in enumerate(self.columns):
            self.data.loc[col, "column"] = i

    
        
    @staticmethod
    def find_closest_col(df, columns, index, const=5):
        """
        Finds which column a given element (index) should be assigned to by
        finding which header element it is closest to.

        Arguments:
            df:         DataFrame to reference from
            columns:    List of the x coordinates of the alignments of each of
                        the columns
            index:      The index of the element to be classified
        Returns:
            fitted_col: The list index of the column to add the index to
        Raises:
            None
        """
        dists = []
        # For each of the columns, append the distance between the centre of
        # the element and the column
        for col in columns:
            dists.append(
                abs(col[2] -
                    (0.5*(eval(df.loc[index, "normed_vertices"])[3][0]
                          + eval(df.loc[index, "normed_vertices"])[2][0]))))

        # Find the index of the one with the smallest distance (+1 since can't
        # be the first column
        fitted_col = dists.index(min(dists))
        
        
        if dists[fitted_col] <= const*stats.median(df.loc[df.index,"height"]):#* stats.median(self.data.loc["height"]): #(const*eval(df.loc[index, "normed_vertices"])[0][1]
                          #- eval(df.loc[index, "normed_vertices"])[0][1]):
                       
            return fitted_col + 1
        else:
            return None
 
    @staticmethod
    def group_header_points(df, header_indices):
        """
        Takes a set of indices and groups them using the find_alignment
        function to put close indices into groups.

        Arguments:
            df:             DataFrame to use the indices to reference from
            header_indices: Indices which we want to group
        Returns:
            header_groups:  List of indices grouped into lists according to
                            which are aligned to each other.
        Raises:
            None
        """

        # Creates a new DataFrame to use and a list to add groups to
        header_df = df.loc[header_indices, :].sort_values(by = "central_x_vertex")
        header_groups = []

        # While we still have ungrouped indices
        while header_df.shape[0] > 0:
            # Find the first elements of the dataframe
            first_ind = header_df.index[0]
            #print(f"This is the group value: {header_df.loc[first_ind, 'value']}")
            # Find the alignment object for that element
            aligner = TableFitter.find_alignment(header_df, first_ind)

            # Find the indices aligned to that object
            grouped_inds = TableFitter.find_aligned_indices(header_df,
                                                            aligner,
                                                            d=0.04)

            # Add the group of indices to the list and remove them from the df
            header_groups.append(grouped_inds["indices"])
            header_df = header_df.drop(grouped_inds["indices"])
            df.dropna()

        return header_groups
      
    def group_value_points(self):
        #print(self.data)
        value_index = [] 
        new_data = self.data.drop(self.columns[0])
        
        #header_values = list(set(self.dates_row + self.header_indices)) 
        header_values = list(set(self.header_indices)) #once it detects dates properly.
        
        value_index = list(new_data.index)
        self.value_index = [i for i in value_index if i not in header_values]
        
        grouped_value_index = self.group_header_points(self.data,
                                                      self.value_index)  #index of values sorted into columns

        self.value_data = self.data.loc[self.value_index] #dataframe that contains only the values and not asset row or headers 
        
        
        for i,x in enumerate(grouped_value_index):
            for v in x:
                self.data.loc[v,"column"] = int(i+1)
        #print(self.data, " with nan added to entire dataframe")
        
        #for i in self.data.index:
        #    self.data.loc[i,"column"] = self.data.loc[i,"column"][np.logical_not(np.isnan(self.data.loc[i,"column"]))]
        
        #check table_data for valid columns , test d threshold 
        #print(grouped_value_index," grouped")
        self.grouped_value_index = grouped_value_index
  
        return grouped_value_index

    def remove_excess_lines(self):
        min_line = min(self.data.loc[self.header_indices, "line_num"])
        new_cols = []
        for col in self.columns:
            new_cols.append([i for i in col if self.data.loc[i,"line_num"]>= min_line])
        self.data = self.data.drop([i for i in self.data.index if self.data.loc[i,"line_num"]< min_line])
        self.columns = new_cols

if __name__ == "__main__":
    fs = gcsfs.GCSFileSystem("ons-companies-house-dev", token="/home/dylan_purches/keys/data_key.json")

    doc_parser = DocParser(fs)
    doc_parser.parse_document("ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/bs_pdfs/04677900_bs.pdf",
                            "/home/dylan_purches/keys/data_key.json",
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
    table_data.group_value_points()
    