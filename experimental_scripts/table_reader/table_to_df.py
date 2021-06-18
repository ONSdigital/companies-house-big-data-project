import gcsfs
import pandas as pd
import regex

class Table2Df:

    def __init__(self, table_fit, fs):
        self.table = table_fit
        self.fs = fs
        self.df = None
        self.data = table_fit.data
        self.table_data = self.table.data.drop(self.table.header_indices)



    def reconstruct_table(self):
        """
        Takes the information stored in the table_fit object and reconstitutes it into
        a DataFrame that mirrors how we see the table on the balance sheet.

        Arguments:
            None
        Returns:
            new_df: pandas DataFrame of table data from the balance sheet
        Raises:
            None
        """
        headers = self.headers_to_strings(self.table.header_groups)

        new_df = pd.DataFrame(columns=headers)
        # For each row from our collected data, add the value in the corresponding position
        for index, row in self.table_data.iterrows():
            new_df.loc[row["line_num"], headers[int(row["column"])]] = row["value"]
        return new_df


    def headers_to_strings(self, headers):
        """
        Converts groups of header indices into a list of strings of all elements
        of each header group combined into a string.

        Arguments:
            headers:        List of grouped header indices (the result of
                            TableFitter.group_header_points())
        Returns:
            str_headers:    List of strings corresponding to the header values
        Raises:
            None
        """
        # Set a value for the first column (which won't have a title)
        str_headers = ["Entities"]

        # Loop over the header groups
        for h in headers:
            k = 0
            new_string = ""
            # Add all strings together
            while k < len(h):
                new_string+= (self.data.loc[h[k], "value"] + "\n")
                k+=1
            str_headers.append(new_string)
        return str_headers




    def get_info_headers_v3(self):
        """
        Creates a DataFrame of information of column info (meta data). For each column in
        our fitted table object, we record the corresponding date and units (currency).
        Arguments:
            self:
        Returns:
            header_data:    pandas DataFrame of column number with their relevant date and units
                            as other variables
        Raises:
            None
        """
        #Converts "column" column into a sorted unique list and removes the unlabeled first column
        sorted_column = list(set(self.table.data["column"]))
        sorted_column = [x for x in sorted_column if str(x)!="nan"]

        #sorted_column = list(set(sorted_column))
        #print('hello', sorted_column)
        sorted_column.sort()
        sorted_column.remove(0)

        column_coords = \
            [self.table.find_alignment(self.table.data, i)["median_points"]
             for i in self.table.grouped_value_index]

        notes_col = self.table.find_closest_col(self.table.data, column_coords,self.table.notes_row[0])
        self.table.data.loc[self.table.notes_row,"column"] = notes_col
        #print(notes_col)
        #assign notes row a colomn and then drop it.
        if self.table.notes_tf:
            #can do if statement that checks if there are any values in the notes row.
            print(notes_col,"notes col")
            sorted_column.remove(notes_col)
        #print('after drop', sorted_column)
        data_cols = sorted_column



        #print(data_cols)
        df = self.table.data
        #print(df)
        column_groups = []
        current_group = []
        dates = []
        dict_column_grouping = {"column":[],"date":[],"unit":[]}
        date_counter = 0
        while len(data_cols) > 0:
            #print(date_counter)
            # date index we want to assoicate to columns
            assignment_date = self.table.dates_row[date_counter]

            # left and right x coordinates of the date we want to associate (x1 and x2 respectivly)
            date_x1 = eval(df.loc[assignment_date, "normed_vertices"])[3][0]
            date_x2 = eval(df.loc[assignment_date, "normed_vertices"])[2][0]

            # first column we want to associate date
            target_column = data_cols[0]

            # update column grouping with current column
            current_group.append(target_column)
            #print(current_group)

            # determine the left and right edges of column
            left_vertex = min([eval(v)[3][0] for v in df.loc[df["column"] == min(current_group),"normed_vertices"]])

            right_vertex = max([eval(v)[2][0] for v in df.loc[df["column"] == max(current_group),"normed_vertices"]])

            #print(left_vertex, date_x1, date_x2, right_vertex) left_vertex <= date_x1 <= right_vertex and
            #see how this works maybe give a different variable increments to allow
            if  left_vertex <= date_x2 <= right_vertex:
                column_groups.append(current_group)

                # update date counter
                date_counter += 1
                # refresh current group
                current_group = []
            #print(date_counter)
            #append dates and columns to the dictionary
            data_cols.pop(0)
            dict_column_grouping["date"].append(df.loc[assignment_date,"value"])
            dict_column_grouping["column"].append(target_column)


            #print('SORTED COLUMNS', data_cols)
        #print(column_groups)

        #find and set currencies
        currencies = [self.data.loc[i, "value"] for i in self.table.data.index if
                            len(regex.findall(r"\p{Sc}", self.data.loc[i, "value"]))]
        currency = max(set(currencies), key=currencies.count)
        dict_column_grouping["unit"] = [currency]*len(dict_column_grouping["column"])
        print(dict_column_grouping,"header dictionary")


        # Create an empty DataFrame to add information to
        header_data = pd.DataFrame.from_dict(dict_column_grouping)
        return header_data


    def get_final_df(self):
        """
        Get a final DataFrame in a similar form as how we scraped xbrl data ("name", "value", "date", "unit"
        as column headers). This is done by merging our TableFitter df with our header info df.

        Arguments:
            None
        Returns:
            df (as attribute):  Final DataFrame of table data in a form similar to our xbrl data.
        Raises:
            None
        """
        # Merge TableFitter df with our info headers data
        self.df = self.table_data.merge(self.get_info_headers_v3(), on="column")
        changed_index = []
        original_index = []
        header_index = []
        
        # For each row of the df, either add a "name" value if you can find one, if not just set to None
        for index, row in self.df.iterrows():
            l = row["line_num"]
            #Adds tag to a data value
                        
            try:
                #print(self.df[(self.df["line_num"]==l)&(self.df["column"]==0)].iloc[0]["value"])
                self.df.loc[index, "name"] = self.data[(self.data["line_num"]==l)&(self.data["column"]==0)].iloc[0]["value"]
                changed_index.append(self.df.iloc[l]["line_num"]) #Not working for for row 11 so it cant be checked for tag pdf 03875584_bs
                
            except:
                self.df.loc[index, "name"] = None
                changed_index.append(self.df.iloc[l]["line_num"])
                
        original_index = self.data["line_num"]
        header_index = self.data.loc[self.table.header_indices,"line_num"]
        missing_index = set(original_index) - set(changed_index) 
        missing_index = missing_index - set(header_index)
    
        print(missing_index,"missing values")
        
        for i in missing_index:
            print(self.data.loc[self.data["line_num"]==i]["value"])
            #self.df.loc[i,"name"] = self.data.loc[self.data["line_num"]==i]["value"]
            #self.df.loc[i,"name"] 
        #compare df with data and add back in the left over TAG that has no value. (once we have the new dataframe order by line number before it is generated.)
        #for i in changed_index:
            
        # for index, row in self.df.iterrows():
        #       l = row["line_num"]
        #       #Adds tag to a data value
        #       if self.df.loc[index, "value"] == 0:
        #           try:
        #           #print(self.df[(self.df["line_num"]==l)&(self.df["column"]==0)].iloc[0]["value"])
        #                  self.df.loc[index, "value"] = None

              

        # Only save the relevant columns
        self.df = self.df[["name", "value", "date", "unit"]]

    @staticmethod
    def get_central_coord(data, i):
        """
        Function to find the bottom central coordinate of a given element.

        Arguments:
            data:   DataFrame in which the element is recorded
            i:      Index (of data) of the element in question
        Returns:
            The central x point between the bottom two vertices of the element
        Raises:
            None
        """
        return 0.5*(eval(data.loc[i, "normed_vertices"])[3][0] + eval(data.loc[i, "normed_vertices"])[2][0])

    @staticmethod
    def get_closest_el(data, x, elements):
        """
        Given a set of indices, and element to query, this function returns the "value" of the
        index which is closest to the central x coordinate of the queried element.

        Arguments:
            data:       DataFrame in which the elements are recorded
            x:          Index of the single element to compare
            elements:   List of indices of the the elements to compare distances to x
        Returns:
            The value of an element (from elements) closest to x
        Raises:
            None
        """
        dists = []
        # Find the distance from x for each of the elements
        for el in elements:
            dists.append(abs(Table2Df.get_central_coord(data, el) - Table2Df.get_central_coord(data, x)))
        return data.loc[elements[dists.index(min(dists))], "value"]


