import sys

def get_error_message_detail(error,error_detail:sys):
    _,_,exc_tb = error_detail.exc_info()
    filename = exc_tb.tb_frame.f_code.co_filename
    error_message = "Error found in python script name [{0}], line no [{1}] with message: [{2}]".format(
        filename,exc_tb.tb_lineno, error
    )

    return error_message

class CustomException(Exception):
    def __init__(self,error,error_detail:sys):
        super().__init__(error)
        self.error_message = get_error_message_detail(error,error_detail)

    def __str__(self):
        self.error_message
