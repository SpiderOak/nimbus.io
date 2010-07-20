class AlreadyInProgress(Exception):
    pass

class ArchiveFailedError(Exception):
    pass

class DestroyFailedError(Exception):
    pass

class HandoffFailedError(ArchiveFailedError):
    pass

class RetrieveFailedError(Exception):
    pass

class ListmatchFailedError(Exception):
    pass

class DataWriterDownError(Exception):
    pass

class DataReaderDownError(Exception):
    pass

class StartHandoff(Exception):
    def __init__(self, to):
        super(StartHandoff, self).__init__()
        self.to = to
