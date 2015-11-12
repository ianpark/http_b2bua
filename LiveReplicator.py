import datetime
import optparse
import sys
from urlparse import urlparse
from twisted.internet import reactor
from twisted.python import log
from dash.DashPuller import DashPuller
from dash.DashPusher import DashPusher


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
    """ Parse argument """
    options = parse_args()

    start = datetime.datetime.now()
    log.startLogging(sys.stdout)
    """ Pulling data and buffer them internally """
    if options.source.endswith(".mpd"):
        puller = DashPuller(options.source)

        pusher = DashPusher(options.destination, puller.consume,
                            mpd_repeat=options.mpd_repeat, init_segment_repeat=options.init_repeat, delete_after=options.delete_after)

        reactor.callWhenRunning(puller.start)
        reactor.callWhenRunning(pusher.start)

        reactor.run()
    else:
        # later other protocol could be added.
        log.err("Not supported input: {}".format(options.source))

    elapsed = datetime.datetime.now() - start

if __name__ == '__main__':
    proxy_main()