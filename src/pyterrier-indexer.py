import sys
import logging
import argparse
import pyterrier as pt

def main():
  logging.basicConfig(stream=sys.stdout)
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)

  parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument("--src",         required=True, type=str, help="")
  parser.add_argument("--tgt",         required=True, type=str, help="")
  parser.add_argument("--srcF",        required=True, type=str, help="")
  parser.add_argument("--tgtF",        required=True, type=str, help="")
  parser.add_argument("--indexDir",    required=True, type=str, help="")
  args = parser.parse_args()

  if not pt.started():
    pt.init()

  indexer = pt.IterDictIndexer(args.indexDir, \
                              meta={'docno': 20, 'text': 4096, args.tgt:4096}, \
                              meta_reverse=['docno'], \
                              threads=1, \
                              type = pt.index.IndexingType.CLASSIC, \
                              stemmer=None, \
                              stopwords=None, \
                              tokeniser='whitespace', \
                              )
  #indexer.setProperties(**{'max.term.length':256, 'indexer.meta.forward.keylens':256})
  def iter_files(srcF, tgtF):
    docno = 0
    with pt.io.autoopen(srcF, 'rt') as srcInF, pt.io.autoopen(tgtF, 'rt') as tgtInF:
      for srcLine in srcInF:
        docno += 1
        srcLine = srcLine.strip()
        tgtLine = tgtInF.readline().strip()
        if len(srcLine) == 0 or len(tgtLine) == 0:
          continue
        print(f'{docno = }')
        yield {'docno' : docno, 'text': srcLine, args.tgt: tgtLine}
      print(f'Processed all {docno} lines')

  indexref = indexer.index(iter_files(args.srcF, args.tgtF), fields=['text'])


if __name__ == '__main__':
  main()
