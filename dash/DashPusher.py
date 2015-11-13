from datetime import datetime
from functools import partial
from urlparse import urljoin

from twisted.internet import reactor

from dash.HttpHelper import postResource
from dash.HttpHelper import deleteResource

import logging
logger = logging.getLogger(__name__)

# DeletionManager
class DeletionManager:
    def __init__(self, delete_after):
        self.deleteAfter = delete_after
        self.waiting_list = []

    def append(self, path):
        from collections import namedtuple
        DeleteItem = namedtuple("DeleteItem", "path expire")
        from datetime import datetime, timedelta
        expire_time = datetime.now() + timedelta(seconds=self.deleteAfter)
        self.waiting_list.append(DeleteItem(path, expire_time))

    def delete_expired_files(self):
        while len(self.waiting_list) > 0 and self.waiting_list[0].expire < datetime.now():
            file_to_delete = self.waiting_list.pop(0)
            if file_to_delete.expire < datetime.now():
                logger.debug("Delete expired files: ", file_to_delete.path)

                def success(result):
                    pass

                def failure(err):
                    pass
                d = deleteResource(file_to_delete.path)
                # adding the callback but the callbacks will do nothing
                # don't care deletion's result.
                d.addCallbacks(success, failure)
            else:
                break

# Data Pusher
class DashPusher:
    def __init__(self, destinations, source, stat, mpd_repeat=1, init_segment_repeat=5, delete_after=60):
        if not destinations:
            raise ValueError("No destination is given")

        self.destinations = destinations
        self.source = source
        self.stat = stat
        self.pollingInterval = 1
        self.init_segment_repeat = init_segment_repeat
        self.mpd_repeat = mpd_repeat
        self.round = 0
        self.on_fly_file_count = 0
        self.deleter = DeletionManager(delete_after)

    def start(self):
        reactor.callLater(self.pollingInterval, self.round_runner)

    """
    Async function polling the source. so as soon as the source is available
    they will be uploaded
    """
    def round_runner(self):
        getMPD = True if self.round % self.mpd_repeat == 0 else False
        getInit = True if self.round % self.init_segment_repeat == 0 else False

        file_list = self.source(getInit, getMPD)
        if len(file_list.media) > 0 and self.on_fly_file_count == 0:
            # Increase round
            self.round += 1
            logger.info("[round %d] Start uploading %d mpd, %d init segments, %d media segments to %d destinations",
                        self.round, len(file_list.mpd), len(file_list.init), len(file_list.media), len(self.destinations))
            def check_completion():
                self.on_fly_file_count -= 1
                if self.on_fly_file_count == 0:
                    logger.info("[round %d] Files are successfully uploaded. total=%dMB", self.round, self.stat.getMB('uploaded_bytes'))

            def on_upload(path, bytes, need_to_delete, result):
                logger.debug("Uploaded: %s ", path)
                self.stat.append('uploaded_bytes', bytes)
                self.stat.increase('uploaded_file_count')
                if need_to_delete:
                    self.deleter.append(path)
                check_completion()

            def on_fail(path, reason):
                logger.debug("Failed to upload: %s %s", path, str(reason))
                self.stat.increase('uploading_failure_count')
                check_completion()

            """
            replicate the uploading for all destinations
            """
            for destination in self.destinations:
                for filename, buffer in file_list.mpd:
                    url = urljoin(destination, filename)
                    d = postResource(url, buffer)
                    d.addCallbacks(partial(on_upload, url, len(buffer), False), partial(on_fail, url))
                    self.on_fly_file_count += 1
                for filename, buffer in file_list.init:
                    url = urljoin(destination, filename)
                    d = postResource(url, buffer)
                    d.addCallbacks(partial(on_upload, url, len(buffer), False), partial(on_fail, url))
                    self.on_fly_file_count += 1
                for filename, buffer in file_list.media:
                    url = urljoin(destination, filename)
                    d = postResource(url, buffer)
                    d.addCallbacks(partial(on_upload, url, len(buffer), True), partial(on_fail, url))
                    self.on_fly_file_count += 1
        else:
            # no file to upload. we can utilise this idle time for deletion!
            self.deleter.delete_expired_files()

        # self repeating
        reactor.callLater(self.pollingInterval, self.round_runner)


