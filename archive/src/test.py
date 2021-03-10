import cv2
from os import listdir
from os.path import isfile, join, splitext

path = "E:\\companies_house_project\\files\\data\\chp_stamps_20200319\\"

def import_files(directory):
    """
    Returns list of all files present in given directory, complete with file extensions.
    
    Arguments:
        directory: source of files of interest
    Returns:
        List of files in a given directory
    Raises:
        None
    """
    return [f for f in listdir(directory) if isfile(join(directory, f))]

def select_extensions(file_list, acceptable_extensions):
    """
    Returns list of files with specified file extensions.
    
    Arguments:
        file_list:             list of files to filter
        acceptable_extensions: list of extensions user wishes to keep
    Returns:
        List of files with specified extensions
    Raises:
        None
    """
    return [file for file in file_list if ('.').join(file.split('.')[-1:]) in acceptable_extensions]

def pos_details(files):
    '''
    '''
    for f in files:
        f_title, f_details = splitext(f + " " + "1" + " " + "0" + " " + "0" + " " + str(cv2.imread(join(path, f)).shape))
        
        f_type, f_num, f_xcord, f_ycord, f_height, f_width, f_num_channels = f_details.split(" ")
        strip_f_height = f_height.strip(",")[1:]
        strip_f_width = f_width.strip(",")

        print("{}{} {} {} {} {} {}".format(f_title, f_type, f_num, f_xcord, f_ycord, strip_f_width, strip_f_height))

pos_details(select_extensions(import_files(path), ".png"))