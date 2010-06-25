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
