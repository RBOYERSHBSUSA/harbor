class HarborError(Exception):
    def __init__(self, code: str, message: str, metadata: dict | None = None, status_code: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.metadata = metadata or {}
        self.status_code = status_code

    def to_dict(self):
        result = {"error": self.code, "message": self.message}
        result.update(self.metadata)
        return result
