import difflib


class Messages:  # TODO: link up the support channels
    """Class for holding user-facing informational and error messages"""

    @staticmethod
    def missing_data_file(filename):
        return f"Couldn't locate the data file: '{filename}'. Please check if the file exists and the path is correct."

    @staticmethod
    def dataset_initialization_failed(method_name):
        return (
            f"Direct instantiation is not allowed. Please use the class method '{method_name}' "
            "to create an instance."
        )

    @staticmethod
    def attribute_error(attribute_name, available_attributes):
        suggestion = difflib.get_close_matches(attribute_name, available_attributes)
        suggestion_text = (
            f"Did you mean '{suggestion[0]}' instead?"
            if suggestion
            else "Please check if the attribute name is correct and available in this context."
        )
        return f"Couldn't find the requested attribute: '{attribute_name}'. {suggestion_text}"

    @staticmethod
    def value_error(expected_type, received_type):
        return f"Expected a value of type '{expected_type}', but received type '{received_type}'. Please check the input value."

    @staticmethod
    def field_not_found(field_name):
        return f"The specified field '{field_name}' does not exist in the dataset. Please check the field name for typos or refer to the dataset documentation for available fields."

    @staticmethod
    def invalid_operation(operation, reason):
        return f"The operation '{operation}' could not be completed. Reason: {reason}"

    @staticmethod
    def generator_exhausted():
        return "No more data to fetch. The generator is exhausted."

    @staticmethod
    def unexpected_error(error):
        # Placeholder for the support URL or troubleshooting guide
        support_url = "http://docs.cyyrus.com"
        return (
            f"An unexpected error occurred: {error}. Please reach us out at {support_url} "
            "for more information."
        )
