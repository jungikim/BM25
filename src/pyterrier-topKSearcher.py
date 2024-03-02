import sys
import logging
import argparse
import pyterrier as pt

def main():
  logging.basicConfig(stream=sys.stdout)
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)

  parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument("--indexDir",    required=True, type=str, help="")
  parser.add_argument("--topK",        default=5,     type=int, help="")  
  parser.add_argument("--src",         required=True, type=str, help="")
  parser.add_argument("--tgt",         required=True, type=str, help="")
  parser.add_argument("--srcF",        required=True, type=str, help="")
  parser.add_argument("--tgtF",        required=True, type=str, help="")
  args = parser.parse_args()


  outputF = open(args.srcF + f'.top{args.topK}', 'w')


  if not pt.started():
    pt.init()
  index = pt.IndexFactory.of(args.indexDir)
  logger.info(index.getCollectionStatistics().toString())
  meta = index.getMetaIndex()
  tokenizer = pt.rewrite.tokenise(tokeniser='whitespace', matchop=True) 
  bm25 = pt.BatchRetrieve(index, wmodel='BM25', num_results = args.topK * 2)
  searchPipeline = tokenizer >> bm25

  cnt = 0
  with pt.io.autoopen(args.srcF, 'rt') as srcInF, pt.io.autoopen(args.tgtF, 'rt') as tgtInF:
    for srcLine in srcInF:
      srcLine = srcLine.strip()
      tgtLine = tgtInF.readline().strip()
      if len(srcLine) == 0 or len(tgtLine) == 0:
        outputF.write('\n')
        continue

      topKList = []
      dupList = []
      res = searchPipeline.search(srcLine)
      for dId in res['docid']:
        srcTxt = meta.getItem('text', dId).strip()
        tgtTxt = meta.getItem(args.tgt, dId).strip().replace('\t',' ')
        if srcTxt == srcLine:
          dupList.append(tgtTxt)
          continue
        logger.debug(dId, srcTxt, tgtTxt, '\n')
        topKList.append(tgtTxt)
        if len(topKList) == args.topK:
          break

      if len(topKList) < args.topK:
        topKList = dupList[0:args.topK-len(topKList)] + topKList

      logger.debug(f'"{srcLine}" retrieved {len(topKList)} matches ({len(dupList) = })')
      outputF.write('\t'.join(topKList))
      outputF.write('\n')
      cnt += 1
      if cnt % 100 == 0:
        logger.info(f'Processed {cnt} instances')

  outputF.close()
  logger.info(f'Processed {cnt} instances')


if __name__ == '__main__':
  main()