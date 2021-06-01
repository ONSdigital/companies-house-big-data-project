from google.cloud import documentai_v1beta3 as documentai
from google.oauth2 import service_account
import gcsfs
import pandas as pd
import math


class DocParser:

    def __init__(self, fs):
        self.document = None
        self.token_df = None
        self.fs = fs

    @staticmethod
    def get_text(el, document):
        """
        Convert text offset indexes into text snippets.

        Arguments:
            el:         Layout element from the document response from Doc AI
            document:   Document element response from Doc AI
        Returns:
            response:   The text corresponding to that layout element
        Raises:
            None
        """
        response = ''
        # If a text segment spans several lines, it will
        # be stored in different text segments.
        for segment in el.text_anchor.text_segments:
            start_index = segment.start_index
            end_index = segment.end_index
            response += document.text[start_index:end_index]
        return response

    @staticmethod
    def get_vertices(layout_el):
        """
        Returns the vertices of the bounding polygon of a layout element as
        a list of pairs of coordinates.

        Arguments:
            layout_el:  Layout element from the document response from Doc AI
        Returns:
            vertices:   List of pairs of vertices that bound the element
        Raises:
            None
        """
        vertices = []
        for vertex in layout_el.bounding_poly.normalized_vertices:
            vertices.append([vertex.x, vertex.y])
        return vertices

    def parse_document(self, input_uri, token_path,
                       project_id, processor_id="cc464c0ab0b35040"):
        """
        Facilitates sending a request to the Doc AI API (via a specified
        processor) and saves the response (a 'document') as a class attribute.

        Arguments:
            input_uri:      The gcs location of a pdf to be processed by Doc AI
            token_path:     Path to the location of json key for authorisation
            project_id:     The gcp project id
            processor_id:   The id of the processor created in the cloud
                            console
        Returns:
            None
        Raises:
            None
        """
        # Instantiates a client
        credentials = service_account.Credentials.from_service_account_file(
            token_path)
        client_options = {"api_endpoint": "eu-documentai.googleapis.com"}
        client = documentai.DocumentProcessorServiceClient(
            credentials=credentials,
            client_options=client_options)

        name = f"projects/{project_id}/locations/eu/processors/{processor_id}"

        # Read the file into memory
        with self.fs.open(input_uri, "rb") as image:
            image_content = image.read()

        document = {"content": image_content, "mime_type": "application/pdf"}

        # Configure the process request
        request = documentai.types.ProcessRequest(name=name,
                                                  document=document,
                                                  skip_human_review=True)

        # Recognizes text entities in the PDF document
        result = client.process_document(request=request)

        self.document = result.document

    def tokens_to_df(self):
        """
        Converts a document response of the API (saved as a class attribute)
        to a DataFrame of individual 'tokens' detected from the document (also
        saved as a class attribute).

        Arguments:
            None
        Returns:
            None
        Raises:
            None
        """
        # Set up a dict to save token data to
        rows = {"value": [],
                "confidence": [],
                "detected_break_type": [],
                "detected_break_count": [],
                "normed_vertices": []}

        # Extract the tokens reference from the document object
        tokens = self.document.pages[0].tokens

        for token in tokens:
            vertices = self.get_vertices(token.layout)

            # Only take tokens which are horizontally aligned
            if abs(self.get_orientation(vertices)) < math.pi/4:
                
                # Get the text for each layout element
                rows["value"].append(
                    self.get_text(token.layout, self.document))
                rows["confidence"].append(token.layout.confidence)
                rows["detected_break_type"].append(token.detected_break.type_.name)
                rows["detected_break_count"].append(
                    token.detected_break.type_.value)
                rows["normed_vertices"].append(vertices)
            else:
                print(self.get_text(token.layout, self.document))

        df = pd.DataFrame.from_dict(rows)

        # Convert the normed vertices to strings - this just means the rest
        # of the pipeline is compatible if we pass the df in as a csv
        df["normed_vertices"] = df["normed_vertices"].astype("str")
        df["value"] = df["value"].replace('"', "")
        self.token_df = df

    @staticmethod
    def get_orientation(vertices):
        """
        For a given set of vertices for the bounding poly of a token, calculates
        the angle from horizontal of the edge below the text. This is an angle
        between -pi and pi. This allows us to determine the orientation of 
        such a token

        Arguments:
            vertices:   List of coordinates of the bounding poly around the token.
                        Note that this should be of type list (other functions in
                        the pipeline take these vertices as strings).
        Returns:
            theta:  Angle between -pi and pi of the orientation of the edge 
                    below the token text.
        Raises:
            None
        """
        v1, v2 = vertices[3], vertices[2]
        delta_x = v2[0] - v1[0]
        delta_y = v1[1] - v2[1]

        theta = math.atan2(delta_y, delta_x)
        return theta
