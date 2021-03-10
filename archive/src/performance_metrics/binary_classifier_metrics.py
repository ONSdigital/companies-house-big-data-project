class BinaryClassifierMetrics:
    '''
    '''

    def __init__(self):
        self.__init__

    @staticmethod
    def binary_confusion_matrix(pred: int, files: str):
        '''
        Calculates the number of true positives, true negatives, false positives and
        false negatives from the output of the classifier, where the true values are
        extracted from the corresponding file names

        Arguments:
            pred:  output from classifier
            files: list of file names

        Raises:
            None

        Returns:
            tuple 
        '''

        exp = [1 if i.rfind('positive') else 0 for i in files]
        tp = sum(1 for i, j in zip(pred, exp) if i + j is 2)
        tn = sum(1 for i, j in zip(pred, exp) if i + j is 0)
        fp = sum(1 for i, j in zip(pred, exp) if i is 1 and j is 0)
        fn = sum(1 for i, j in zip(pred, exp) if i is 0 and j is 1)

        return tp, tn, fp, fn

    @staticmethod
    def accuracy(tp: int, tn: int, fp: int, fn: int):
        '''
        Calculate the accuracy as a ratio of the number of correct predictions 
        to the total number of predictions.
        
        Arguments:
            tp: number of true positives
            tn: number of true negatives
            fp: number of false positives
            fn: number of false negatives
        
        Raises:
            Exception if a None value is specified as an argument.
            Exception if the type of a value specified as an argument is not an int.
        
        Returns:
            float

        accuracy = ( tp + tn ) / (tp + tn + fp + fn)
        '''

        args = (tp, tn, fp, fn)

        if any(_ is None for _ in args):
            raise ValueError("Specify integer values")
        if any(type(_) != int for _ in args):
            raise TypeError("Specify integer types")

        if sum(args) != 0:
            return (tp + tn) / sum(args)

    @staticmethod
    def precision(tp: int, fp: int):
        '''
        Calculate the precision as a ratio of the number of true positives 
        to the total number of reported positives.
        
        Arguments:
            tp: number of true positives
            fp: number of false positives  
        
        Raises:
            Exception if a None value is specified as an argument.
            Exception if the type of a value specified as an argument is not an int.
        
        Returns:
            float
        
        precision = tp / (tp + fp)
        '''

        args = (tp, fp)

        if any(_ is None for _ in args):
            raise ValueError("Specify integer values")
        if any(type(_) != int for _ in args):
            raise TypeError("Specify integer types")

        if sum(args) != 0:
            return tp / sum(args)

    @staticmethod
    def recall(tp: int, fn: int):
        '''
        Calculate the recall as a ratio of the number of true positives 
        to the total number of real positives. 
        
        Arguments:
            tp: number of true positives
            fn: number of false negatives  
        
        Raises:
            Exception if a None value is specified as an argument.
            Exception if the type of a value specified as an argument is not an int.
        
        Returns:
            float
        
        recall = tp / (tp + fn)
        '''

        args = (tp, fn)

        if any(_ is None for _ in args):
            raise ValueError("Specify integer values")
        if any(type(_) != int for _ in args):
            raise TypeError("Specify integer types")

        if sum(args) != 0:
            return tp / sum(args)

    @staticmethod
    def specificity(tn: int, fn: int):
        '''
        Calculate the specificity as a ratio of the number of true negatives 
        to the total number of reported negatives.
        
        Arguments:
            tn: number of true negatives
            fn: number of false negatives  
        
        Raises:
            Exception if a None value is specified as an argument.
            Exception if the type of a value specified as an argument is not an int.
        
        Returns:
            float
        
        specificity = tn / (tn + fn)
        '''

        args = (tn, fn)

        if any(_ is None for _ in args):
            raise ValueError("Specify integer values")
        if any(type(_) != int for _ in args):
            raise TypeError("Specify integer types")

        if sum(args) != 0:
            return tn / sum(args)

    @staticmethod
    def metrics_report(accuracy: float, precision: float, recall: float, specificity: float):
        """
        Outputs the performance metrics values of a binary classifier to the terminal.
        The performance metrics are accuracy, precision, recall and specificity.
        Values for each metric are between 0.0 and 1.0.

        Arguments:
            accuracy:       instance of the accuracy object.
            precision:      instance of the precision object.
            recall:         instance of the recall object.
            specificity:    instance of the specificity object.

        Raises:
            ValueError if no argument is given or floats passed are not less than or equal to 1.
            TypeError if arguments passed are not floats.

        Returns:
            None
        """

        args = [accuracy, precision, recall, specificity]

        output = "The performance metrics of the binary classifier presented with image ??? are:\naccuracy = {}" \
                 "\nprecision = {}\nrecall = {}\nspecificity = {}".format(accuracy, precision, recall, specificity)

        if any(arg is None for arg in args):
            raise ValueError("Specify float values")

        if any(type(arg) != type(float) for arg in args):
            raise TypeError("Specify float types")

        if any(arg > 1 for arg in args):
            raise ValueError("Specify float types less than or equal to 1")

        print(output)
