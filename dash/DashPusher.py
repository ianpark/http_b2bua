from datetime import datetime, timedelta
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
        self.deleter = DeletionManager(delete_after)

        # round can start when this variable is 0
        self.on_fly_file_count = 0
        # timestamp of the start of current round
        self.round_started_at = None

    def start(self):
        reactor.callLater(self.pollingInterval, self.round_runner)

    def start_new_round(self, file_list):
        # Increase round
        self.round += 1
        self.round_started_at = datetime.now()
        self.stat.increase('total_uploading_round')
        logger.info("[round %d] Start uploading %d mpd, %d init segments, %d media segments to %d destinations",
                    self.round, len(file_list.mpd), len(file_list.init), len(file_list.media), len(self.destinations))
        def check_completion():
            self.on_fly_file_count -= 1
            if self.on_fly_file_count == 0:
                delta = datetime.now()-self.round_started_at
                logger.info("[round %d] Files are successfully uploaded. Elapsed time: %f",
                            self.round, delta.total_seconds())
                self.stat.append('total_uploading_time', delta.total_seconds())
                self.stat.set('avg_uploading_time', self.stat.get('total_uploading_time')/self.round)
                logger.info(self.stat)

        def on_upload(path, bytes, need_to_delete, result):
            logger.debug("Uploaded: %s ", path)
            self.stat.append('uploaded_bytes', bytes)
            self.stat.increase('uploaded_file_count')
            if need_to_delete:
                self.deleter.append(path)
            check_completion()

        def on_fail(path, reason):
            logger.error("Failed to upload: %s %s", path, str(reason))
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

    """
    Async function polling the source. so as soon as the source is available
    they will be uploaded
    """
    def round_runner(self):
        # Only start a round or deletion when there is no file being uploaded
        if self.on_fly_file_count == 0:
            # Decide which file to download
            getMPD = True if self.round % self.mpd_repeat == 0 else False
            getInit = True if self.round % self.init_segment_repeat == 0 else False
            # get list of the file from the source
            file_list = self.source(getInit, getMPD)
            if len(file_list.media) > 0:
                # start new round.
                self.start_new_round(file_list)
            else:
                # no file to upload. we can utilise this idle time for deletion!
                self.deleter.delete_expired_files()

        # self repeating
        reactor.callLater(self.pollingInterval, self.round_runner)


