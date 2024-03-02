import sys
import logging
import argparse
from pathlib import Path
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
  parser.add_argument("--topK",        default=5,     type=int, help="")  
  parser.add_argument("--src",         required=True, type=str, help="")
  parser.add_argument("--tgt",         required=True, type=str, help="")
  parser.add_argument("--srcF",        required=True, type=str, help="")
  parser.add_argument("--tgtF",        required=True, type=str, help="")
  parser.add_argument("--request_timeout", default=600, type=int, help="")
  parser.add_argument("--max_num_tokens",  default=256, type=int, help="")
  args = parser.parse_args()

  isTrain = False
  if 'train' in args.srcF:
    isTrain = True

  es = Elasticsearch(args.hostname,
                     basic_auth=('elastic',args.password),
                     ca_certs=args.cacert,
                     max_retries=10, retry_on_timeout=True,
                     request_timeout=args.request_timeout,
                     sniff_on_node_failure=True, min_delay_between_sniffing=600,
                     sniff_timeout=300,)

  if Path(f'{args.srcF}.top{args.topK}').exists():
    sys.exit(f'{args.srcF}.top{args.topK} already exists')

  outputF = open(f'{args.srcF}.top{args.topK}', 'w')
  docno = 0
  with open(args.srcF, 'rt') as srcInF, open(args.tgtF, 'rt') as tgtInF:
    for srcLine in srcInF:
      docno += 1
      srcLine = srcLine.strip()
      tgtLine = tgtInF.readline().strip()
      if len(srcLine) == 0 or len(tgtLine) == 0:
        outputF.write('\n')
        continue

      if len(srcLine.split()) > args.max_num_tokens:
        srcLine = ' '.join(srcLine.split()[:args.max_num_tokens])

      success = False
      while not success:
        try:
          if isTrain:
            resp = es.search(index=args.indexName,
                            query={"simple_query_string": {"fields": [args.src],
                                                  "query": srcLine}
                            },
                            post_filter={"bool": {"must_not": { "match": {"docno": docno}}}},
                            size = (args.topK + 1)
                  )
            success = True
          else:
            resp = es.search(index=args.indexName,
                            query={"simple_query_string": {"fields": [args.src],
                                                  "query": srcLine}
                            },
                            size = (args.topK + 1)
                  )
            success = True
        except Exception as e: # such as elastic_transport.ConnectionTimeout
          logging.error(str(e))
          logging.error("Trying again ...")


      topKList = []
      for d in resp['hits']['hits']:
        # {'_index': 'test', 
        #  '_id': '1', 
        #  '_score': 67.015114, 
        #  '_source': {'docno': 1, 
        #              'en': '▁Spec ta cular ▁Wing s uit ▁J ump ▁Over ▁Bo got a', 
        #              'fr': '▁Spec t ac ulaire ▁sa ut ▁en ▁" wing s uit " ▁au - dessus ▁de ▁Bo got a'}}
        d = d['_source']
        dId = d['docno']
        srcTxt = d[args.src]
        tgtTxt = d[args.tgt]

        logger.debug(dId, srcTxt, tgtTxt, '\n')
        topKList.append(tgtTxt)
        if len(topKList) == args.topK:
          break

      logger.debug(f'"{srcLine}" retrieved {len(topKList)} matches')
      outputF.write('\t'.join(topKList))
      outputF.write('\n')

      if docno % 100 == 0:
        logger.info(f'Processed {docno} instances')

  outputF.close()
  logger.info(f'Processed {docno} instances')


if __name__ == '__main__':
  main()
