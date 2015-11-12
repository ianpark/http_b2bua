from datetime import datetime
from functools import partial
from urlparse import urljoin

from twisted.internet import reactor

from dash.HttpHelper import postResource
from dash.HttpHelper import deleteResource



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
                print "Delete expired files: ", file_to_delete.path

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
    def __init__(self, destinations, source, mpd_repeat=1, init_segment_repeat=5, delete_after=60, ):
        if not destinations:
            raise ValueError("No destination is given")

        self.destinations = destinations
        self.source = source
        self.pollingInterval = 1
        self.init_segment_repeat = init_segment_repeat
        self.mpd_repeat = mpd_repeat
        self.round = 0
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
        if len(file_list.media) > 0:
            print "Files from the source: {} mpd, {} init segments, {} media segments".format(len(file_list.mpd), len(file_list.init), len(file_list.media))

            def on_upload(path, need_to_delete, result):
                print "Uploaded: ", path
                if need_to_delete:
                    self.deleter.append(path)

            def on_fail(path, reason):
                print "Failed to upload: ", path, str(reason)

            """
            replicate the uploading for all destinations
            """
            for destination in self.destinations:
                for filename, buffer in file_list.mpd:
                    url = urljoin(destination, filename)
                    d = postResource(url, buffer)
                    d.addCallbacks(partial(on_upload, url, False), partial(on_fail, url))
                for filename, buffer in file_list.init:
                    url = urljoin(destination, filename)
                    d = postResource(url, buffer)
                    d.addCallbacks(partial(on_upload, url, False), partial(on_fail, url))
                for filename, buffer in file_list.media:
                    url = urljoin(destination, filename)
                    d = postResource(url, buffer)
                    d.addCallbacks(partial(on_upload, url, True), partial(on_fail, url))
            # Increase round
            self.round += 1
        else:
            # no file to upload. we can utilise this idle time for deletion!
            self.deleter.delete_expired_files()

        # self repeating
        reactor.callLater(self.pollingInterval, self.round_runner)


