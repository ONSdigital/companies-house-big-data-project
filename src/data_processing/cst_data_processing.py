import cv2
from os import listdir
from os.path import isfile, join
import random

class DataProcessing:
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

	def trim_extensions(file_list):
		"""
		Returns list of file names with no file extensions.
		
		Arguments:
			file_list: list of files requiring extension removal
		Returns:
			List of files with no extensions, eg. "example.png" --> "example"
		Raises:
			None
		"""
		return [('.').join(file.split('.')[:-1]) for file in file_list]

	def resizeMultipleImages(image_list, scale_factor, input_folder, output_folder):
		"""
		Resizes a list of images to a given scale factor, outputting to given directory.
		
		Arguments:
			image_list:    list of images requiring resizing
			scale_factor:  factor to scale each image by
			input_folder:  user specified file source
			output_folder: user specified file destination
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
			resized = cv2.resize(original_image, new_dimension, interpolation = cv2.INTER_AREA)
			cv2.imwrite(output_folder + "\\RESIZED___" + image + ".png", resized)

	def create_random_ROI_from_list(list_of_images, ROI_count, rect_width, rect_height, input_folder, output_folder):
		"""
		Creates a given number (ROI_count) of images of dimension (rect_width, rect_height) for each image in a 
		list and outputs to a given directory. Anchor of ROI is at each rectangle's North West vertex, and is 
		always contained within the page geometry.
		
		Arguments:
			list_of_images: list of images to extract Regions Of Interests
			ROI_count:      the number of ROIs required per source image
			rect_width:     width of each ROI outputted
			rect_height:    height of each ROI outputted
			input_folder:   user specified file source
			output_folder:  user specified file destination
		Returns:
			None
		Raises:
			None
		"""
		for image_file in list_of_images:
			image = cv2.imread(input_folder + "\\" + str(image_file), -1)
			image_height, image_width = image.shape[:-1]
			for i in range(ROI_count):
				left_coord = random.randrange(0, image_width - rect_width + 1, 1)
				top_coord = random.randrange(0, image_height - rect_height + 1, 1)
				right_coord = left_coord + rect_width
				bottom_coord = top_coord + rect_height
				region_of_interest = image[top_coord:bottom_coord, left_coord:right_coord]
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

def get_file_details(files, n_objects = 1, x_coord = 0, y_coord = 0):
    """
    Returns details for a singular file.
    
    Arguments:
        files:     list of files to filter
        n_objects: number of object identified in scene
        x_coord:   horizontal position of object
        y_coord:   vertical position of object
    Returns:
        List of files with specified extensions
    Raises:
        None
    """
    for f in files:
        f_title, f_details = splitext(f + " " + str(n_objects) + " " + str(x_coord) + " " + str(y_coord) + " " + str(cv2.imread(join(path, f)).shape))
        f_type, f_num, f_xcoord, f_ycoord, f_height, f_width, f_num_channels = f_details.split(" ")
        strip_f_height = f_height.strip(",")[1:]
        strip_f_width = f_width.strip(",")

        return "{}{} {} {} {} {} {}".format(f_title, f_type, f_num, f_xcoord, f_ycoord, strip_f_width, strip_f_height)

def rename_files(input_folder, file_name = "chp_signature", extension = ".png"):
    """
    Renames all files in a directory to systematic structure.
    
    Arguments:
        input_folder: directory to read in files
        file_name:    naming convention to standardise to
        extension:    save to specific file type
    Returns:
        None
    Raises:
        None
    """
    i = 0
    for file in input_folder:
        new_name ="{}{}{}".format(file_name, "_", i)
        rename(path + file, path + new_name + extension)
        i += 1