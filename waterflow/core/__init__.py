import uuid

def make_id():
    return str(uuid.uuid4()).replace("-", "")