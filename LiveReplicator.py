import datetime
import optparse
import sys
from urlparse import urlparse
from twisted.internet import reactor
from twisted.python import log
from dash.DashPuller import DashPuller
from dash.DashPusher import DashPusher
from common.Statistic import Statistics

import logging

root = logging.getLogger()
root.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

def parse_args():
    parser = optparse.OptionParser(usage=__doc__)
    parser.add_option("-s", "--src", type="string", dest="source", help="Set source HTTP path (master manifest)")
    parser.add_option("-m", "--mpd-repeat", type="int", dest="mpd_repeat", help="Repeat manifest for every x rounds", default = 1)
    parser.add_option("-i", "--init-repeat", type="int", dest="init_repeat", help="Repeat init segment for every x rounds", default = 5)
    parser.add_option("-D", "--delete-after", type="int", dest="delete_after", help="Delete segments after x seconds", default = 60)
    (options, args) = parser.parse_args()

    if not options.source:
        parser.error('Source path must be set')
    if not args:
        parser.error('No destination is given.')

    def check_url(url):
        x = urlparse(url)
        if not x.scheme or not x.netloc:
            parser.error('{} is not proper url.'.format(url))
    # check validity
    check_url(options.source)
    map(check_url, args)
    # copy the arguments to the option tuple
    options.destination = args

    return options

def proxy_main():
    logger = logging.getLogger(__name__)
    """ Parse argument """
    options = parse_args()

    logger.info("Started: %s", datetime.datetime.now())

    stat = Statistics()

    """ Pulling data and buffer them internally """
    if options.source.endswith(".mpd"):
        puller = DashPuller(options.source)

        pusher = DashPusher(options.destination, puller.consume, stat,
                            mpd_repeat=options.mpd_repeat, init_segment_repeat=options.init_repeat, delete_after=options.delete_after)

        reactor.callWhenRunning(puller.start)
        reactor.callWhenRunning(pusher.start)

        reactor.run()
    else:
        # later other protocol could be added.
        logger.error("Not supported input: %s", options.source)

if __name__ == '__main__':
    proxy_main()