# <Your Pipeline Name>

## Overview
Brief description of what this pipeline collects and why.

## Data statistics

## Technical framework design and implementation 

## Updated latest literature summary 

## Evaluation/Benchmark Plan

## Outline thesis structure


## Setup
```bash
python -m venv venv
source venv/bin/activate
pip install -r pipeline/requirements.txt
cp pipeline/config.example.yaml pipeline/config.yaml
# fill in pipeline/config.yaml with your own API keys / paths (not committed)
```

## Running the pipeline
```bash
python pipeline/main.py
```

## Resuming after interruption
Explain how the pipeline picks up where it left off (e.g. checkpoint file, idempotent writes, last-updated timestamp).

## Output
Where output lands, in what format, and how it maps to `metadata.yaml`.

## Known limitations
Anything the integration team should be aware of.
