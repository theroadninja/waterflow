"""
Code that returns the correct client for the service pointer.
"""
from waterflow.mocks.business_logic_service.biz_client import BusinessLogicClient


def get_client_for(service_pointer: str):
    #
    # TODO - this is just a mock client
    #
    return BusinessLogicClient()