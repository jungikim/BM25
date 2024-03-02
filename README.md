Implementation of the Okapi BM25-based similar translation unit retrieval approach described in [Hoang et al., Improving Retrieval Augmented Neural Machine Translation by
Controlling Source and Fuzzy-Match Interactions, Findings of EACL 2023](https://aclanthology.org/2023.findings-eacl.22.pdf).

Given a bilingual corpus, indexing and searching is performed using the source langauge to retrieve the TopK target segments.

For the run examples below, we assume we have these files:
```
wmt14_train.de
wmt14_train.en
wmt14_validation.de
wmt14_validation.en
wmt14_test.de
wmt14_test.en
```
and the source language is English, and the target, German.


## 1. Preprocess Data
There are many ways to preprocess, the below example demonstrates a simple sentencepiece tokenization:
```
spm_train --input=wmt14_train.de,wmt14_train.en --model_prefix=wmt14_ende_sp --vocab_size=32000 --character_coverage=1.0 --model_type unigram

cat > spm_wmt14_ende_tok.yml << EOL
type: SentencePieceTokenizer
params:
  model: wmt14_ende_sp.model
EOL

for i in wmt14_{train,validation,test}.{de,en} ; do
  cat $i | onmt-tokenize-text --tokenizer_config spm_wmt14_ende_tok.yml > ${i}.tok &
done
```

## Using Terrier (https://github.com/terrier-org/pyterrier.git)

### 2. Build Index
```
time python3 src/pyterrier-indexer.py \
  --src en \
  --tgt de \
  --srcF wmt14_train.en.tok \
  --tgtF wmt14_train.de.tok \
  --indexDir wmt14_train.terrier_index.en \
> wmt14_train.terrier_index.en.log 2>&1 &
```

### 3. Retrieve TopK segments in the target langauge
```
split -l 500000 -a 4 -x  wmt14_train.en.tok wmt14_train.en.tok.split.
split -l 500000 -a 4 -x  wmt14_train.de.tok wmt14_train.de.tok.split.

for split in {0000..0009} ; do
python3 src/pyterrier-topKSearcher.py \
  --indexDir wmt14_train.terrier_index.en \
  --topK 5 \
  --src en \
  --tgt de \
  --srcF wmt14_train.en.tok.split.${split} \
  --tgtF wmt14_train.de.tok.split.${split} \
> wmt14_train.en.tok.split.${split}.top5.log 2>&1 &
done

cat wmt14_train.en.tok.split.{0000..0009}.top5 > wmt14_train.en.tok.top5
rm wmt14_train.en.tok.split.{0000..0009}.top5


for c in validation test ; do
  python3 src/pyterrier-topKSearcher.py \
    --indexDir wmt14_train.terrier_index.en \
    --topK 5 \
    --src en \
    --tgt de \
    --srcF wmt14_${c}.en.tok \
    --tgtF wmt14_${c}.de.tok \
  > wmt14_${c}.en.tok.top5.log 2>&1 &
done
```

## Using elasticsearch python client (https://elasticsearch-py.readthedocs.io/)

Set HOSTNAME, PASSWORD and CACERT to match your elasticsearch server configuration.
For example:
```
HOSTNAME='https://localhost:9200'
PASSWORD='GVfnCr6TvL0*+jPQ1Z=4'
CACERT='~/http_ca.crt'
```

### 2. Build Index
```
python3 src/elasticsearch-indexer.py \
  --hostname ${HOSTNAME} \
  --password ${PASSWORD} \
  --cacert ${CACERT} \
  --indexName wmt14_ende_topk \
  --src en \
  --tgt de \
  --srcF wmt14_train.en.tok \
  --tgtF wmt14_train.de.tok \
> wmt14_train.elasticsearch_index.en_topk.log 2>&1 &
```

### 3. Retrieve TopK segments in the target langauge
```
split -l 500000 -a 4 -x  wmt14_train.en.tok wmt14_train.en.tok.split.
split -l 500000 -a 4 -x  wmt14_train.de.tok wmt14_train.de.tok.split.

cat > run_topk_wmt14_ende_trainsplit.sh << EOL
split=\$1
python3 src/elasticsearch-topKSearcher.py \
  --topK 5 \
  --hostname ${HOSTNAME} \
  --password ${PASSWORD} \
  --cacert ${CACERT} \
  --indexName wmt14_ende_topk \
  --src en \
  --tgt de \
  --srcF wmt14_train.en.tok.split.\${split} \
  --tgtF wmt14_train.de.tok.split.\${split} \
> wmt14_train.en.tok.split.\${split}.top5.log 2>&1
EOL
chmod +x run_topk_wmt14_ende_trainsplit.sh
parallel -j 4 ./run_topk_wmt14_ende_trainsplit.sh ::: `seq -w 0000 0009`

cat wmt14_train.en.tok.split.{0000..0009}.top5 > wmt14_train.en.tok.top5
rm wmt14_train.en.tok.split.{0000..0009}.top5


for c in validation test ; do
  time python3 src/elasticsearch-topKSearcher.py \
    --hostname ${HOSTNAME} \
    --password ${PASSWORD} \
    --cacert ${CACERT} \
    --indexName wmt14_ende_topk \
    --src en \
    --tgt de \
    --srcF wmt14_${c}.en.tok \
    --tgtF wmt14_${c}.de.tok \
    > wmt14_${c}.en.tok.top5.log 2>&1 &
done
```
