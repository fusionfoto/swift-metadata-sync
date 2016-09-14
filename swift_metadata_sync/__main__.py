import argparse
import json
import logging
import traceback

from container_crawler import ContainerCrawler
from .metadata_sync import MetadataSync


def setup_logger(console=False, log_file=None, level='INFO'):
    logger = logging.getLogger('swift-metadata-sync')
    logger.setLevel(level)
    formatter = logging.Formatter(
        '[%(asctime)s] %(name)s [%(levelname)s]: %(message)s')
    if console:
        handler = logging.StreamHandler()
    elif log_file:
        handler = logging.handlers.RotatingFileHandler(log_file,
                                                       maxBytes=1024*1024*100,
                                                       backupCount=5)
    else:
        raise RuntimeError('log file must be set')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def load_config(conf_file):
    with open(conf_file, 'r') as f:
        return json.load(f)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Swift metadata synchronization daemon')
    parser.add_argument('--config', metavar='conf', type=str, required=True,
                        help='path to the configuration file')
    parser.add_argument('--once', action='store_true',
                        help='run once')
    parser.add_argument('--log-level', metavar='level', type=str,
                        default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        help='logging level; defaults to info')
    parser.add_argument('--console', action='store_true',
                        help='log messages to console')
    return parser.parse_args()


def main():
    args = parse_args()
    conf = load_config(args.config)
    setup_logger(console=args.console, level=args.log_level.upper(),
                 log_file=conf.get('log_file'))

    logger = logging.getLogger('swift-metadata-sync')
    logger.info('Starting Swift Metadata Sync')
    try:
        conf['bulk_process'] = True
        crawler = ContainerCrawler(conf, MetadataSync, logger)
        if args.once:
            crawler.run_once()
        else:
            crawler.run_always()
    except Exception as e:
        logger.error("Metadata Sync failed: %s" % repr(e))
        logger.error(traceback.format_exc(e))
        exit(1)

if __name__ == '__main__':
    main()
