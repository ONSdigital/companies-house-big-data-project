import configparser
#import sys

#sys.path.append('~/companies-house-big-data-project/src')

from src.authenticator.gcp_authenticator import GCPAuthenticator
import gcsfs

# Reads from the config file
cfg = configparser.ConfigParser()
cfg.read("cha_pipeline.cfg")

# Config imports
auth_setup = cfg["auth_setup"]

class XbrlBatcher:
    """ This is a class for parsing the XBRL data."""

    def __init__(self, auth):
        self.__init__
        self.project = auth.project
        self.key = auth.xbrl_parser_key
        self.fs = gcsfs.GCSFileSystem(
            project=self.project, token=self.key, cache_timeout=0)

    def batch_files(self, directory, authenticator):

        batch_count_hard_limit = 360
        #batch_size_hard_limit = 4e9
        batch_size_hard_limit = 80000

        # Retrieve all files in specified bucket/directory
        files = [filename for filename in self.fs.ls(directory)
                        if ((".htm" in filename.lower())
                            or (".xml" in filename.lower()))
                ]

        # Retrieve a list of all file sizes for all files retrieved
        sizes = []
        num_files = 11
        #num_files = len(files) + 1
        for filename, n in zip(files[0:num_files], range(1, num_files + 1)):
            
            if n % (int)(len(files) / 100) == 0:
                print("Retrieving file size for file " + str(n) + " of " + str(len(files)))
            
            sizes.append(self.fs.size(filename))

        # Create list of files and their sizes and sort in descending order
        data_descending = sorted(zip(files, sizes), key=lambda t: t[1], reverse=True)
        data_ascending = sorted(data_descending, key=lambda t: t[1], reverse=False)

        data_descending = data_descending[:(int)(len(data_descending) / 2)]
        data_ascending = data_ascending[(int)(len(data_ascending) / 2):]

        # If there is an odd number of files, append a dummy file to the end of the
        # descending file list
        if (num_files % 2) == 1:
            data_descending.append(("", 0))

        print(data_descending)
        print(data_ascending)

        # Create batches of files using the Gauss approach of pairing the largest and
        # smallest files
        batch_list = [[]]
        batch_size_list = [[]]
        batch_size = 0
        batch_count = 0
        current_batch = 0
        for file_big, file_small in zip(data_descending, data_ascending):
            size_remaining = batch_size_hard_limit - batch_size
            count_remaining = batch_count_hard_limit - batch_count

            pair_size = file_big[1] + file_small[1]

            print("---")
            print(file_big[1], file_small[1])
            print(pair_size)
            print(size_remaining)

            # Start a new batch if the size or count of the current batch has hit the limit
            if (size_remaining - pair_size <= 0) or (count_remaining <= 0):
                batch_list.append([])
                batch_size_list.append([])
                current_batch += 1
                batch_size = 0
                batch_count = 0

            # Don't append the dummy file if it is present in the current file pair
            if file_big[0] != "":
                batch_list[current_batch] += [file_big[0], file_small[0]]
            else:
                batch_list[current_batch] += [file_small[0]]

            batch_size_list[current_batch].append(pair_size)
            batch_size += pair_size
            batch_count += 1

        print("Final batches:")
        [print(batch) for batch in batch_list]
        
        print("Batch sizes...")
        [print(sum(batch)) for batch in batch_size_list]

        return batch_list

        