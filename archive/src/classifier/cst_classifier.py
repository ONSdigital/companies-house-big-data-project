# import the necessary packages
from os import listdir
from os.path import isfile, join
import sys
# import cv2
import numpy

class Classifier:
	def imgs_to_grey(path):
		'''
		'''
		#if path is None:
		#	raise ValueError("Specify a path to an image.")
	#   if type(path) not in [""]:
	#       raise TypeError("The value of the path argument must be a of type string")
		return cv2.cvtColor(path, cv2.COLOR_BGR2GRAY)

	def classifier_input(path):
		'''
		'''
		#if path is None:
		#	raise ValueError("Specify a path to an image.")
	#    if type(path) not in [cv2.imread(path)]:
	#        raise TypeError("The value of the path argument must be a of type string")
		return cv2.imread(path)
		
	def ensemble(detector, imgs, scale_factor, min_neighbors, w, h):
	    '''
	    '''
	    return detector.detectMultiScale(imgs, scale_factor, min_neighbors, minSize = (w, h))

	def display_classifier_output(title, imgs):
		'''
		'''
		return cv2.imshow(title, imgs)

	def count_classifier_output(imgs):
		'''
		'''
		return len(imgs)

	def classifier_output(rects, image, path):
		'''
		'''
		for (i, (x, y, w, h)) in enumerate(rects):
			cv2.rectangle(image, (x, y), (x + w, y + h), (0, 0, 255), 2)
			cv2.putText(image, "CH Stamp #{}".format(i + 1), (x, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 255), 2)
			roi_colour = image[y:y + h, x:x + w]
			print("[INFO] Object classified. Saving locally")
			cv2.imwrite(path + str(w) + str(h) + "_ch_stamps.png", roi_colour)

	def classifier_process(image, path):
		status = cv2.imwrite(path, image)
		print("[INFO] Output from the classifier written to filesystem in: " + path, "[STATUS]: " + str(status))