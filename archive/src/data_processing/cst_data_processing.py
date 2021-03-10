# import cv2
from os import listdir, mkdir
from os.path import isfile, join, exists, splitext
import random
import zipfile
import shutil
import io


class DataProcessing:
    """ This is a class for processing data. """

    def __init__(self, fs):
        self.__init__
        self.fs = fs

    @staticmethod
    def import_files(directory):
        """
        Returns list of all files present in given directory, complete with
        file extensions.

        Arguments:
            directory: source of files of interest (str)
        Returns:
            List of files in a given directory (list)
        Raises:
            None
        """
        return [f for f in listdir(directory) if isfile(join(directory, f))]

    @staticmethod
    def select_extensions(file_list, acceptable_extensions):
        """
        Returns list of files with specified file extensions from the given
        file list.

        Arguments:
            file_list:             list of files to filter (list)
            acceptable_extensions: list of extensions user wishes to keep
                                   (list)
        Returns:
            List of files with specified extensions (list)
        Raises:
            None
        """
        return [file for file in file_list if '.'.join(file.split('.')[-1:])
                in acceptable_extensions]

    @staticmethod
    def trim_extensions(file_list):
        """
        Returns list of file names with no file extensions.

        Arguments:
            file_list: list of files requiring extension removal (list)
        Returns:
            List of files with no extensions, eg. "example.png" --> "example"
            (list)
        Raises:
            None
        """
        return ['.'.join(file.split('.')[:-1]) for file in file_list]

    @staticmethod
    def resizeMultipleImages(image_list, scale_factor,
                             input_folder, output_folder):
        """
        Resizes a list of images to a given scale factor, outputting to given
        directory.

        Arguments:
            image_list:    list of images requiring resizing (list)
            scale_factor:  factor to scale each image by (float)
            input_folder:  user specified file source (str)
            output_folder: user specified file destination (str)
        Returns:
            None
        Raises:
            None
        """
        for image in image_list:
            original_image = cv2.imread(input_folder + "\\" + image, -1)
            new_width = int(original_image.shape[1] * scale_factor)
            new_height = int(original_image.shape[0] * scale_factor)
            new_dimension = (new_width, new_height)
            resized = cv2.resize(original_image, new_dimension,
                                 interpolation=cv2.INTER_AREA)
            cv2.imwrite(output_folder + "\\RESIZED___" + image + ".png",
                        resized)

    @staticmethod
    def create_random_ROI_from_list(list_of_images, ROI_count, rect_width,
                                    rect_height, input_folder, output_folder):
        """
        Creates a given number (ROI_count) of images of dimension
        (rect_width, rect_height) for each image in a list and outputs to a
        given directory. Anchor of ROI is at each rectangle's North West
        vertex, and is always contained within the page geometry.

        Arguments:
            list_of_images: list of images to extract Regions Of Interests
                            (list)
            ROI_count:      the number of ROIs required per source image (int)
            rect_width:     width of each ROI outputted (float)
            rect_height:    height of each ROI outputted (float)
            input_folder:   user specified file source (str)
            output_folder:  user specified file destination (str)
        Returns:
            None
        Raises:
            None
        """
        for image_file in list_of_images:
            image = cv2.imread(input_folder + "\\" + str(image_file), -1)
            image_height, image_width = image.shape[:-1]
            for i in range(ROI_count):
                left_coord = random.randrange(0, image_width - rect_width + 1,
                                              1)
                top_coord = random.randrange(0, image_height - rect_height + 1,
                                             1)
                right_coord = left_coord + rect_width
                bottom_coord = top_coord + rect_height
                region_of_interest = image[top_coord:bottom_coord,
                                     left_coord:right_coord]
                cv2.imwrite(
                    output_folder
                    + "\\"
                    + str(image_file)
                    + "___("
                    + str(left_coord)
                    + str(",")
                    + str(top_coord)
                    + ").png"
                    + "_w="
                    + str(rect_width)
                    + "_h="
                    + str(rect_height)
                    + "_dtype=COLOUR"
                    + ".png",
                    region_of_interest
                )

    def extract_compressed_files(self, file_source, file_dest):
        """
        Extracts .zip files from a given directory or filename
        to a given file directory.
        Arguments:
            file_source: source of files of interest, either a filename
                         or a directory (str)
            file_dest: destination directory to save extracted files to (str)
        Returns:
            None
        Raises:
            None
        """
        if self.fs.exists(file_dest):
            if self.fs.exists(file_source):

                # Retrieve a list of all zip files that have been extracted
                # If no such list exists, create one
                if self.fs.exists(file_dest + "/extracted_files.txt"):
                    extracted_files_txt = self.fs.open(
                        file_dest + "/extracted_files.txt", "r")
                    list_extracted_files = [f.rstrip("\n") for f in
                                            extracted_files_txt]
                    extracted_files_txt.close()
                else:
                    list_extracted_files = []
                extracted_files_txt = self.fs.open(
                    file_dest + "/extracted_files.txt", "a")

                if self.fs.isfile(file_source):
                    files = [file_source]
                else:
                    # Retrieve list of all files present in the directory
                    # (not folders)
                    files = self.fs.ls(file_source)
                    #[file_source + f for f in listdir(file_source)]
                    files = [f for f in files if self.fs.isfile(f)]

                # Filter out all files from 'files' that have already been
                # extracted
                files = [f for f in files if f not in list_extracted_files]

                print("Extracting files...")
                for file in files:
                    if file.endswith('.zip'):

                        directory = file_dest + "/" + \
                                    file.split('.')[0].split("/")[-1]

                        # If the directory for the zip contents exists already,
                        # delete it just in case a previous attempt at
                        # extraction was incomplete. Otherwise, create a new
                        # directory
                        if self.fs.exists(directory):
                            self.fs.rm(directory, recursive=True)

                        # Perform the extraction
                        with zipfile.ZipFile(self.fs.open(file), 'r') \
                                as zip_ref:
                            for contentfilename in zip_ref.namelist():
                                contentfile = zip_ref.read(contentfilename)
                                try:
                                    with self.fs.open(
                                            directory + "/" + contentfilename,
                                            'wb') as f:
                                        f.write(contentfile)

                                    self.fs.setxattrs(
                                        directory + "/" + contentfilename,
                                        content_type="text/"
                                                     +contentfilename \
                                                         .split(".")[-1])
                                except:
                                    print("Failed to save:", contentfilename)
                        print("Extracted files from " + file)

                        # Add extracted filename to file list
                        extracted_files_txt.write(file + "\n")
                    else:
                        print("File extension " + file.split('.')[
                            1] + " not supported")
                        print("Unable to extract file " + file)

                extracted_files_txt.close()
                print("Extraction complete")
            else:
                print("File or directory not found: " + file_source)
        else:
            print("Destination directory not valid!: " + file_dest)

def get_file_details(files, n_objects=1, x_coord=0, y_coord=0):
    """
    Returns details for a singular file.
    
    Arguments:
        files:     list of files to filter (list)
        n_objects: number of object identified in scene (int)
        x_coord:   horizontal position of object (float)
        y_coord:   vertical position of object (float)
    Returns:
        List of files with specified extensions (list)
    Raises:
        None
    """
    for f in files:
        f_title, f_details = splitext(
            f + " " + str(n_objects) + " " + str(x_coord) + " " + str(y_coord)
            + " " + str(cv2.imread(join(path, f)).shape))
        f_type, f_num, f_xcoord, f_ycoord, f_height, f_width, f_num_channels \
            = f_details.split(" ")
        strip_f_height = f_height.strip(",")[1:]
        strip_f_width = f_width.strip(",")

        return "{}{} {} {} {} {} {}".format(f_title, f_type, f_num, f_xcoord,
                                            f_ycoord, strip_f_width,
                                            strip_f_height)


def rename_files(input_folder, file_name="chp_signature", extension=".png"):
    """
    Renames all files in a directory to systematic structure.
    
    Arguments:
        input_folder: directory to read in files (str)
        file_name:    naming convention to standardise to (str)
        extension:    save to specific file type (str)
    Returns:
        None
    Raises:
        None
    """
    i = 0
    for file in input_folder:
        new_name = "{}{}{}".format(file_name, "_", i)
        rename(path + file, path + new_name + extension)
        i += 1
