from backdrop.write.scanned_file import ScannedFile, VirusSignatureError
from backdrop import statsd


class FileUploadException(IOError):
    def __init__(self, message):
        self.message = message


class UploadedFile(object):
    # This is ~ 1mb in octets
    MAX_FILE_SIZE = 1000001

    def __init__(self, file_object):
        self.file_object = file_object
        if file_object.filename is None:
            raise FileUploadException('No file uploaded %s' % self.file_object)

    def file_stream(self):
        return self.file_object.stream

    def _is_size_valid(self):
        return self.file_object.content_length < self.MAX_FILE_SIZE

    def _is_content_type_valid(self):
        return self.file_object.content_type in [
            "text/csv",
            "application/json",
            "application/vnd.ms-excel",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ]

    @statsd.timer('uploaded_file.save')
    def save(self, bucket, parser):
        if not self.valid:
            self.file_stream().close()
            raise FileUploadException('Invalid file upload {0}'
                                      .format(self.file_object.filename))
        self.perform_virus_scan()
        data = parser(self.file_stream())
        bucket.parse_and_store(data)
        self.file_stream().close()

    @statsd.timer('uploaded_file.perform_virus_scan')
    def perform_virus_scan(self):
        if ScannedFile(self.file_object).has_virus_signature:
            self.file_stream().close()
            raise VirusSignatureError(
                'File {0} could not be uploaded as it may contain a virus.'
                .format(self.file_object.filename))

    @property
    def valid(self):
        return self._is_size_valid() and self._is_content_type_valid()
