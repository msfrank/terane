from twisted.application.service import MultiService

class BIERManager(MultiService):
    """
    Manages indexing and event retrieval. 
    """

    def __init__(self):
        MultiService.__init__(self)

    def configure(self, settings):
        pass
