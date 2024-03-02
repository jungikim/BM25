import sys
import logging
import argparse
from elasticsearch import Elasticsearch

def main():
  logging.basicConfig(stream=sys.stdout)
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)

  logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)

  parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument("--hostname",    required=True, type=str, help="")
  parser.add_argument("--password",    required=True, type=str, help="")
  parser.add_argument("--cacert",      required=True, type=str, help="")
  parser.add_argument("--indexName",   required=True, type=str, help="")
  parser.add_argument("--src",         required=True, type=str, help="")
  parser.add_argument("--tgt",         required=True, type=str, help="")
  parser.add_argument("--srcF",        required=True, type=str, help="")
  parser.add_argument("--tgtF",        required=True, type=str, help="")
  args = parser.parse_args()

  es = Elasticsearch(args.hostname, \
                     basic_auth=('elastic',args.password), \
                     ca_certs=args.cacert)

  es.indices.create(index=args.indexName, 
                    mappings={
                        'properties': {
                            'docno': {'type': 'integer'},
                            args.src: {'type': 'text', 'analyzer': 'standard'},
                            args.tgt: {'type': 'text', 'index': 'false', 'store': 'true'}, 
                        }
                    })

  docno = 0
  with open(args.srcF, 'r') as srcInF, open(args.tgtF, 'r') as tgtInF:
    for srcLine in srcInF:
      docno += 1
      srcLine = srcLine.strip()
      tgtLine = tgtInF.readline().strip()
      if len(srcLine) == 0 or len(tgtLine) == 0:
        continue
      logger.debug(f'{docno = }')
      resp = es.index(index=args.indexName,
                      id=docno,
                      document={'docno': docno,
                                args.src: srcLine,
                                args.tgt: tgtLine})
      logger.debug(resp['result'])

      if docno % 100 == 0:
        logger.info(f'Processed {docno} instances')

    logger.info(f'Processed all {docno} lines')

  es.indices.refresh(index=args.indexName)


if __name__ == '__main__':
  main()