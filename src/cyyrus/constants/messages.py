import difflib


class Messages:  # TODO: link up the support channels
    """Class for holding user-facing informational and error messages"""

    @staticmethod
    def missing_data_file(filename):
        """
        This static method returns a message indicating that a data file could not be located.

        :param filename: The `filename` parameter is a string that represents the name of the data file
        that the method is trying to locate
        :return: The method `missing_data_file` returns a string message indicating that the data file
        specified by the `filename` parameter could not be located. The message advises the user to
        check if the file exists and if the path provided is correct.
        """
        return f"Couldn't locate the data file: '{filename}'. Please check if the file exists and the path is correct."

    @staticmethod
    def dataset_initialization_failed(method_name):
        """
        This function is designed to handle dataset initialization failures and takes the method name as
        a parameter.

        :param method_name: The `method_name` parameter is a variable that stores the name of the method
        or function where the dataset initialization failed
        """
        return (
            f"Direct instantiation is not allowed. Please use the class method '{method_name}' "
            "to create an instance."
        )

    @staticmethod
    def attribute_error(attribute_name, available_attributes):
        """
        This function checks if a given attribute is available in a list of available attributes and
        raises an AttributeError if it is not found.

        :param attribute_name: The `attribute_error` function seems to be designed to handle attribute
        errors in Python. The `attribute_name` parameter likely refers to the name of the attribute that
        caused the error, and the `available_attributes` parameter probably contains a list of available
        attributes that can be accessed
        :param available_attributes: The `available_attributes` parameter should be a list of strings
        representing the attributes that are available. This function seems to be designed to handle
        attribute errors by checking if a given attribute name is in the list of available attributes.
        """
        suggestion = difflib.get_close_matches(attribute_name, available_attributes)
        suggestion_text = (
            f"Did you mean '{suggestion[0]}' instead?"
            if suggestion
            else "Please check if the attribute name is correct and available in this context."
        )
        return f"Couldn't find the requested attribute: '{attribute_name}'. {suggestion_text}"

    @staticmethod
    def value_error(expected_type, received_type):
        """
        The function `value_error` compares the expected and received types of a value and provides an
        error message if they do not match.

        :param expected_type: The `expected_type` parameter in the `value_error` function is the data
        type that is expected for a certain value. It is the type that the value should be in order for
        the function or program to work correctly
        :param received_type: The `received_type` parameter in the `value_error` function refers to the
        type of the value that was actually received as input. It is used to indicate the data type of
        the value that was provided as an argument to a function or method
        :return: a formatted string that indicates the expected type and the received type of a value.
        """
        return f"Expected a value of type '{expected_type}', but received type '{received_type}'. Please check the input value."

    @staticmethod
    def field_not_found(field_name):
        """
        The function `field_not_found` returns a message indicating that the specified field does not
        exist in the dataset.

        :param field_name: The `field_name` parameter in the `field_not_found` function represents the
        name of the field that was not found in the dataset. This function is designed to generate a
        message informing the user that the specified field does not exist in the dataset
        :return: The function `field_not_found(field_name)` returns a message stating that the specified
        field does not exist in the dataset. The message advises the user to check the field name for
        typos or refer to the dataset documentation for available fields.
        """
        return f"The specified field '{field_name}' does not exist in the dataset. Please check the field name for typos or refer to the dataset documentation for available fields."

    @staticmethod
    def invalid_operation(operation, reason):
        """
        The function `invalid_operation` returns a formatted error message with the given operation and
        reason.

        :param operation: The `operation` parameter represents the specific operation that could not be
        completed. It could be a mathematical operation, a function call, or any other action that was
        attempted but failed for some reason
        :param reason: The `reason` parameter in the `invalid_operation` function is a string that
        explains why the operation could not be completed. It provides a specific explanation or
        justification for why the operation failed
        :return: The function `invalid_operation` is returning a string that includes the operation and
        the reason why it could not be completed.
        """
        return f"The operation '{operation}' could not be completed. Reason: {reason}"

    @staticmethod
    def generator_exhausted():
        """
        The function `generator_exhausted` returns a message indicating that there is no more data to
        fetch from the generator.
        :return: "No more data to fetch. The generator is exhausted."
        """
        return "No more data to fetch. The generator is exhausted."

    @staticmethod
    def unexpected_error(error):
        """
        This function is designed to handle unexpected errors by taking in an error parameter.

        :param error: The "error" parameter in the function "unexpected_error" is used to pass an error
        object or message to the function for handling or processing. You can use this parameter to
        capture and work with any unexpected errors that may occur during the execution of the function
        """
        # Placeholder for the support URL or troubleshooting guide
        support_url = "http://docs.cyyrus.com"
        return (
            f"An unexpected error occurred: {error}. Please reach us out at {support_url} "
            "for more information."
        )
