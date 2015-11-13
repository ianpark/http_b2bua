from datetime import datetime, timedelta
from functools import partial
from urlparse import urljoin

from twisted.internet import reactor

from HttpHelper import getResource
from mpd.parser import MPDParser

import logging
logger = logging.getLogger(__name__)

def str_to_seconds(time_string):
    time_string = time_string.replace(".0S", "S")
    td = datetime.strptime(time_string, 'PT%HH%MM%SS') - datetime(1900, 1, 1)
    return td.total_seconds()

"""
Interprets MPD and extracts the last segment list and other useful information
"""
class MPDInterpreter:
    # Constructor
    def __init__(self, mpd_path, raw_mpd):
        self.mpd_path = mpd_path
        self.segmentDuration = 0
        self.initSegments = []
        self.mediaSegments = []
        self.update_mpd(raw_mpd)

    def update_mpd(self, raw_mpd):
        self.mpd = MPDParser.parse(raw_mpd)
        self.refresh_interval = str_to_seconds(self.mpd.minimum_update_period)
        self.segmentDuration = self.get_segment_duration()

        if len(self.initSegments) == 0:
            self.createInitSegmentList()
        self.createLastSegmentList()

    # segment duration based on "video" content in the first period.
    def get_segment_duration(self):
        # segment duration based on "video" content in the first period.
        for period in self.mpd.periods:
            for adaptationSet in period.adaptation_sets:
                if adaptationSet.content_type == "video":
                    if(adaptationSet.segment_templates):
                        return adaptationSet.segment_templates[0].duration / adaptationSet.segment_templates[0].timescale

    def createInitSegmentList(self):
        self.initSegments = []
        if len(self.mpd.periods) > 0:
            period = self.mpd.periods[-1]
            for adaptationSet in period.adaptation_sets:
                if adaptationSet.segment_templates:
                    initializationFormat = adaptationSet.segment_templates[0].initialization
                    for representation in adaptationSet.representations:
                        self.initSegments.append(initializationFormat.replace("$RepresentationID$", representation.id))

    def createLastSegmentList(self):
        self.mediaSegments = []
        if len(self.mpd.periods) > 0:
            period = self.mpd.periods[-1]
            periodDuration = str_to_seconds(period.duration)
            for adaptationSet in period.adaptation_sets:
                if adaptationSet.segment_templates:
                    segmentDuration = adaptationSet.segment_templates[0].duration / adaptationSet.segment_templates[0].timescale
                    lastSegmentNumber = int(periodDuration / segmentDuration)
                    mediaFormat = adaptationSet.segment_templates[0].media
                    for representation in adaptationSet.representations:
                        self.mediaSegments.append(mediaFormat.replace("$RepresentationID$", representation.id).replace("$Number$", str(lastSegmentNumber)))


"""
Group downloader. created per group and deleted when the mission is completed
"""
class GroupDownloader:
    def __init__(self, path, sink, file_list):
        self.path = path
        self.sink = sink
        self.count = len(file_list)
        self.error_count = 0
        self.downloaded_list = []
        logger.debug("Initiate group downloading: %s files", len(file_list))
        for filename in file_list:
            url = urljoin(self.path, filename)
            logger.debug("Start downloading segments: %s", url)
            d = getResource(url)
            d.addCallbacks(partial(self.on_download, filename), partial(self.on_err, filename))

    def on_download(self, filename, data):
        self.downloaded_list.append([filename, data])
        logger.debug("Successfully downloaded[{}/{}]: {} ({} bytes)".format(len(self.downloaded_list), self.count, filename, len(data)))
        self.check_completion()

    """
    If an error happened to the downloading, leave a log and keep going.
    Probably no one will want the test to stop due to one transfer failure.
    """
    def on_err(self, filename, err):
        logger.debug("Failed to download %s due to:", filename , err)
        logger.debug("Decrase the target count.")
        self.error_count += 1
        self.check_completion()

    """
    Check the completion and deliver the download files
    """
    def check_completion(self):
        if self.count + self.error_count == len(self.downloaded_list):
            logger.debug("Downloading is completed with {} success and {} failure".format(len(self.downloaded_list), self.error_count))
            self.sink(self.downloaded_list)

"""
Drive downloading MPD and segment files in asynchrnous way
Keeps the downloaded files until they are consumed but will discarse oldest one if buffer got too big
"""
class DashPuller:
    def __init__(self, path):
        logger.info("DashPuller created")
        self.mpd_path = path
        self.mpd_name = path.split("/")[-1]
        self.raw_mpd = ""
        self.mpd_int = None
        self.refresh_interval = 0
        self.last_update = None

        # Collect init segments once and then reuse
        self.init_segments = []
        self.media_segments = []

    def start(self):
        logger.info("Start pulling %s", self.mpd_path)
        self.get_mpd()

    def on_mpd_error(self, reason):
        logger.error("Failed downloading mpd: %s ", reason)
        logger.error("Retey after 5 seconds.")
        reactor.callLater(5, self.start)

    def get_mpd(self):
        logger.debug("Start dowlnoading MPD: {}".format(self.mpd_path))
        # Adjust the timestamp. In order to prevent time shift due to the processing time in callbacks,
        # keep last_update time to be exactly aligned to the next segment start
        if self.last_update is None or self.mpd_int is None:
            self.last_update = datetime.now()
        else:
            self.last_update = self.last_update + timedelta(seconds=self.mpd_int.segmentDuration)
        # trigger async download
        d = getResource(self.mpd_path)
        d.addCallbacks(self.on_mpd, self.on_mpd_error)

    def stop(self):
        logger.debug("Stop pulling", self.mpd_path)

    def on_mpd(self, data):
        logger.debug("Downloaded: {} (size={})".format(self.mpd_path, len(data)))
        # Parse and update the MPD
        self.raw_mpd = data
        if self.mpd_int is None:
            self.mpd_int = MPDInterpreter(self.mpd_path, str(data))
        else:
            self.mpd_int.update_mpd(str(data))

        # reserve the next MPD update
        # Here the delay is deducted from the chunk duration in order to
        # aviod the possible timeshift. It is calculated as a second because
        # less than second delay will be not a big deal because in get_mpd, "last_update"
        # is updated based on "segmentDuration", not "now()" so last_update will be always
        # aligned to the segmentDuration slots
        mpdDownloadDelay = (datetime.now() - self.last_update).seconds
        remainingDuration = self.mpd_int.segmentDuration - mpdDownloadDelay
        reactor.callLater(remainingDuration, self.get_mpd)

        # download init segment if not there
        if len(self.mpd_int.initSegments) > 0:
            if len(self.init_segments) == 0:
                GroupDownloader(self.mpd_path, self.init_segment_collector, self.mpd_int.initSegments)

        # download media segments for this round
        if len(self.mpd_int.mediaSegments) > 0:
            GroupDownloader(self.mpd_path, self.media_segment_collector, self.mpd_int.mediaSegments)

    def init_segment_collector(self, init_segment_list):
        self.init_segments = init_segment_list
        logger.info("Init segments are ready.")

    def media_segment_collector(self, media_segment_list):
        if len(self.media_segments) > 3:
            logger.debug("Too many media segment packages are in the queue. Clear all.")
            del media_segment_list[:]
        logger.debug("New media segment package with {} files is delivered.".format(len(media_segment_list)))
        self.media_segments.append(media_segment_list)

    def consume(self, includeIndex = False, includeMPD = False):
        mpd_list = []
        init_list = []
        media_list = []
        if len(self.media_segments) > 0:
            if includeIndex is True:
                init_list = self.init_segments
            if includeMPD is True:
                mpd_list.append([self.mpd_name, self.raw_mpd])
            media_list = self.media_segments.pop(0)

        from collections import namedtuple
        DashFile = namedtuple("DashFile", "mpd init media")
        files = DashFile(mpd_list, init_list, media_list)
        return files