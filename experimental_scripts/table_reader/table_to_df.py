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

    def get_info_headers(self, years = range(1,20)):
        """
        Creates a DataFrame of information of column info (meta data). For each column in
        our fitted table object, we record the corresponding date and units (currency).

        Arguments:
            years:          List of possible years to search for.
        Returns:
            header_data:    pandas DataFrame of column number with their relevant date and units
                            as other variables
        Raises:
            None
        """
        # Get a list of header indicies where we see a currency symbol
        currency_indexes = [i for i in self.table.header_indices if
                            len(regex.findall(r"\p{Sc}", self.data.loc[i, "value"]))]
        self.unit_headers = currency_indexes
        
        # As above but for where we see a year
        date_indexes = []
        for i in self.table.header_indices:
            contains_year = any([str(y).zfill(2) in self.data.loc[i, "value"] for y in years])
            if contains_year:
                date_indexes.append(i)
        self.date_headers = date_indexes

        # Create an empty DataFrame to add information to
        header_data = pd.DataFrame(columns=["column", "date", "unit"])

        # Add information for each column header to the DataFrame
        for i, g in enumerate(self.table.header_groups):
            unit_col = [j for j in g if j in self.unit_headers]
            date_col = [j for j in g if j in self.date_headers]
            if len(unit_col)==1  or len(date_col)==1:
                header_data.loc[i, "column"] = i+1

                # Work out the unit if we don't have one assigned
                if not unit_col:
                    header_data.loc[i, "date"] = self.data.loc[date_col[0], "value"]
                    header_data.loc[i, "unit"] = self.get_closest_el(self.data, date_col[0], self.unit_headers)
                # As above but for dates
                elif not date_col:
                    header_data.loc[i, "date"] = self.get_closest_el(self.data, unit_col[0], self.date_headers)
                    header_data.loc[i, "unit"] = self.data.loc[unit_col[0], "value"]
                # If we already have both add them to the dataset
                else:
                    header_data.loc[i, "date"] = self.data.loc[date_col[0], "value"]
                    header_data.loc[i, "unit"] = self.data.loc[unit_col[0], "value"]
        return header_data


    def get_info_headers_v2(self, years = range(1,20)):
        """
        Creates a DataFrame of information of column info (meta data). For each column in
        our fitted table object, we record the corresponding date and units (currency).

        Arguments:
            years:          List of possible years to search for.
        Returns:
            header_data:    pandas DataFrame of column number with their relevant date and units
                            as other variables
        Raises:
            None
        """
        self.data_cols = [i+1 for i,g in enumerate(self.table.header_groups) if self.table.notes_row[0] not in g]
        data_cols = [i+1 for i,g in enumerate(self.table.header_groups) if self.table.notes_row[0] not in g]

        currencies = [self.data.loc[i, "value"] for i in self.table.header_indices if
                            len(regex.findall(r"\p{Sc}", self.data.loc[i, "value"]))]
        currency = max(set(currencies), key=currencies.count)
        
        # As above but for where we see a year
        dates = []
        for i in self.table.header_indices:
            contains_year = any([str(y).zfill(2) in self.data.loc[i, "value"] for y in years])
            if contains_year:
                dates.append(self.data.loc[i, "value"])
        self.dates = dates

        if len(data_cols)%len(dates) != 0:
            raise(TypeError("Cannot logically fit dates to columns"))
        else:
            header_dict = {"column": data_cols, "date":[dates[i//(len(data_cols)//len(dates))] for i in range(len(data_cols))], 
                        "unit":[currency]*len(data_cols)}


        # Create an empty DataFrame to add information to
        header_data = pd.DataFrame.from_dict(header_dict)
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
        self.df = self.table_data.merge(self.get_info_headers_v2(), on="column")
        
        # For each row of the df, either add a "name" value if you can find one, if not just set to None
        for index, row in self.df.iterrows():
            l = row["line_num"]
            try:            
                # print(self.df[(self.df["line_num"]==l)&(self.df["column"]==0)].iloc[0]["value"])
                self.df.loc[index, "name"] = self.data[(self.data["line_num"]==l)&(self.data["column"]==0)].iloc[0]["value"]
            except:
                self.df.loc[index, "name"] = None
        
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
        

