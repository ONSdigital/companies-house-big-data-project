from google.oauth2 import service_account
import googleapiclient.discovery
import git
import json
import base64
import time

import os
import shutil

class GCPAuthenticator:
    """Class for downloading keys relevant to pipeline modules"""
    def __init__(self, iam_access_sa, project):
        """
        Initialise class attributes to be filled with key data.

        Arguments:
            iam_access_sa:  Filepath of key to be used to create module
                            specific keys.
            project:        String of project ID
        Returns
            None
        Raises:
            None
        """
        self.iam_access_sa = iam_access_sa
        self.project = project

        self.keys_filepath = None

        self.master_key = None
        self.xbrl_scraper_key = None
        self.xbrl_unpacker_key = None
        self.xbrl_validator_key = None
        self.xbrl_parser_key = None
    
    def get_sa_keys(self, keys_filepath):
        """
        Function to create and download service account keys
        for any modules where a key does not already exist.

        WARNING -- Ensure that the keys_filepath is NOT within 
        the repo - this is a security risk.

        Arguments
            keys_filepath (str):  Directory where existing keys are saved/where
                                  new keys should be saved.
        Returns
            None
        Raises
            ValueError:     When either the filepath doesn't exist or it references
                            a location within a git repo
        """
        
        # Check the keys_file path exists and is not in a git repo
        if not os.path.isdir(keys_filepath):
            raise ValueError(
        "Specified path either does not exist or isn't a directory"
        )
        #if self.check_git_repo(keys_filepath):
        #    raise ValueError(
        #"SECURITY WARNING - do not save keys within a git repo! Ensure folder is immediately deleted from repo"
        #)   

        self.keys_filepath = keys_filepath

        # If a master key exists, set all module specific variables to use this key
        if os.path.exists(keys_filepath + "/master_key.json"):
            master_key = self.read_json(keys_filepath + "/master_key.json")
            self.master_key = master_key
            self.xbrl_scraper_key = master_key
            self.xbrl_unpacker_key = master_key
            self.xbrl_validator_key = master_key
            self.xbrl_parser_key = keys_filepath + "/master_key.json"

        # If not, for each module key, check if one already exists and if not create one
        # which is saved (as a dict) to the relevant class attribute
        else:
            # Create a key for the xbrl_scraper if needed
            if os.path.exists(keys_filepath + "/xbrl_scraper_key.json"):
                self.xbrl_scraper_key = self.read_json(keys_filepath + "/xbrl_scraper_key.json")
            else:
                self.xbrl_scraper_key = self.create_key("xbrl-web-scraper@ons-companies-house-dev.iam.gserviceaccount.com")

            # Create a key for the xbrl_unpacker if needed
            if os.path.exists(keys_filepath + "/xbrl_unpacker_key.json"):
                self.xbrl_unpacker_key = self.read_json(keys_filepath + "/xbrl_unpacker_key.json")
            else:
                self.xbrl_unpacker_key = self.create_key("xbrl-unpacker@ons-companies-house-dev.iam.gserviceaccount.com")

            # Create a key for the xbrl_validator if needed
            if os.path.exists(keys_filepath + "/xbrl_validator_key.json"):
                self.xbrl_validator_key = self.read_json(keys_filepath + "/xbrl_validator_key.json")
            else:
                self.xbrl_validator_key = self.create_key("xbrl-validator@ons-companies-house-dev.iam.gserviceaccount.com")

            # Create a key for the xbrl_parser if needed - this needs to be saved locally as
            # code is run through the console in this module
            if os.path.exists(keys_filepath + "/xbrl_parser_key.json"):
                self.xbrl_parser_key = keys_filepath + "/xbrl_parser_key.json"
            else:
                parser_dict = self.create_key("xbrl-parser@ons-companies-house-dev.iam.gserviceaccount.com")
                with open(keys_filepath + "/xbrl_parser_key.json", "w") as f:
                    json.dump(parser_dict, f)
                self.xbrl_parser_key = keys_filepath + "/xbrl_parser_key.json"
        
        # Add a sleep to allow GCP to finish creating keys
        time.sleep(5)


    def create_key(self, service_account_email):
        """
        Creates a key for a service account.
        
        Arguments
            service_account_email (str): Email address of the 
                                        service account to create a key for
        Returns
            key:    python dict of the json key returned from GCP
        Raises
            ValueError:    If too many keys are already associated to a
                           service account
        """

        # Set up credentials to allow creation of keys
        credentials = service_account.Credentials.from_service_account_file(
            filename=self.iam_access_sa,
            scopes=['https://www.googleapis.com/auth/cloud-platform'])
        
        # Create an api client
        service = googleapiclient.discovery.build(
            'iam', 'v1', credentials=credentials)

        # List all the keys currently associated with a sa
        keys_list = service.projects().serviceAccounts().keys().list(
            name='projects/-/serviceAccounts/' + service_account_email).execute()
        
        # If the number of keys exceeds the threshold raise an error
        if len(keys_list["keys"]) >= 10:
            raise ValueError(
        "Too many keys have been generated for this service account. Please remove some and try again"
        )

        # Create a key and save the response
        response_json = service.projects().serviceAccounts().keys().create(
            name='projects/-/serviceAccounts/' + service_account_email, body={}
            ).execute()

        print('Created key: ' + response_json['name'])

        # Decode the key from the response and evaluate as python dict
        key = eval(base64.b64decode(response_json["privateKeyData"]))

        return key
    
    def delete_key(self, key, full_name=False):
        """
        Deletes a service account key.
        
        Arguments
            key (dict): JSON key we want to delete
        Returns
            None
        Raises
            None  
        """
        if full_name:
            full_key_name = key["name"]
        else:
            # Construct the full key name from the json fields
            full_key_name = "projects/" + key["project_id"] + "/serviceAccounts/" + key["client_email"] + "/keys/" + key["private_key_id"]

        # Set up credentials to allow deletion of keys
        credentials = service_account.Credentials.from_service_account_file(
            filename= self.iam_access_sa,
            scopes=['https://www.googleapis.com/auth/cloud-platform'])

        # Create API client
        service = googleapiclient.discovery.build(
            'iam', 'v1', credentials=credentials)

        # Delete key
        service.projects().serviceAccounts().keys().delete(
            name=full_key_name).execute()

        print('Deleted key: ' + full_key_name)

    def clean_up_keys(self, remove_disk_keys=True):
        """
        Function to clean up (delete) keys used once the pipeline
        has run.

        Arguments
            remove_disk_keys (Bool): Switch for whether or not to
                                     remove locally saved keys
        Returns
            None
        Raises
            None
        """
        print("CLEANING UP")

        # List the names of the xbrl keys
        keys_names = [name for name in self.__dict__.keys() if (name.split("_")[-1] == "key" and name.split("_")[0] != "master")]
        
        if self.master_key == None:
            for k in keys_names:

                # If the key has been created and it is saved in memory delete
                if (self.__dict__[k] != None) and type(self.__dict__[k]) == dict:
                    self.delete_key(self.__dict__[k])
                    self.__dict__[k] = None

                # If we should remove disk keys and the key is specified locally, delete key
                # and remove local file
                elif remove_disk_keys and type(self.__dict__[k]) == str:
                    print("Removing disk key")
                    self.delete_key(self.read_json(self.keys_filepath + "/" + k + ".json"))
                    os.remove(self.keys_filepath + "/" + k + ".json")
        else:
            print("Master key specified, nothing to clean up")
                

    def remove_n_keys(self, sa_email, n=10):
        """
        Removes the last n keys created for a given service account

        Arguments
            sa_email (str): Service account email to delete keys from
            n (int):        Number of keys to delete
        Returns
            None
        Raises
            None
        """

        # Set up credentials allowing deletion of keys
        credentials = service_account.Credentials.from_service_account_file(
            filename=self.iam_access_sa,
            scopes=['https://www.googleapis.com/auth/cloud-platform'])

        # Create API client
        service = googleapiclient.discovery.build(
            'iam', 'v1', credentials=credentials)

        # List the keys associated to that sa
        keys_response = service.projects().serviceAccounts().keys().list(
            name='projects/-/serviceAccounts/' + sa_email).execute()
        
        
        keys_list = keys_response["keys"]
        
        # Reverse the list so that we delete the most recently created first
        keys_list.reverse()
        
        # count added incase n is larger than the number of keys
        count = min(n, len(keys_list))
        for i in range(count):
            try:
                self.delete_key(keys_list[i], full_name=True)
                print("DELETED A KEY")
            except:
                print("Couldn't DELETE A KEY")


    @staticmethod
    def check_git_repo(filepath):
        """
        Function to check if a given filepath lies within a git repo.

        Arguments
            filepath (str): Filepath to be checked
        Returns
            is_git (Bool): Whether or not filepath lies within a repo
        Raises
            ValueError: If filepath does not exist
        """

        if os.path.exists(filepath):
            try:
                repo = git.Repo(filepath, search_parent_directories=True)
                is_git = True
            except:
                is_git = False
        else:
            raise ValueError("Specified filepath does not exist")
        return is_git

    @staticmethod
    def read_json(filepath):
        """
        Reads .json file into memory and saves as dict

        Arguments
            filepath (str): Filepath of the .json file we want to
                            read in
        Returns
            json_object (dict): .json file in dict format
        Raises
            ValueError: If the filepath doesn't exist or doesn't point to
                        a .json file
        """
        if not os.path.isfile(filepath):
            raise ValueError(
        "Specified filepath does not exist"
        )
        if filepath.split(".")[-1] != "json":
            raise ValueError(
        "Please specify the path of a JSON file"
        )

        with open(filepath, 'r') as f:
            data = f.read()
        json_object = json.loads(data)

        return json_object

   
    

if __name__ == "__main__":
    import atexit
    import gcsfs
    
    keys_folder = "/home/kirsty_cope/keys"
    authenticator = GCPAuthenticator("/home/kirsty_cope/keys/Feb_key.json", "ons-companies-house-dev")
    
