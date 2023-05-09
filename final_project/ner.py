import spacy
from thinc.api import set_gpu_allocator, require_gpu

# Use the GPU, with memory allocations directed via PyTorch.
# This prevents out-of-memory errors that would otherwise occur from competing
# memory pools.
set_gpu_allocator("pytorch")
require_gpu(0)

# load the model and specify the device ID of the GPU to use
transformer_nlp = spacy.load('en_core_web_trf')
nlp = spacy.load('en_core_web_sm')

def lower_case(text):
    return text.lower()

def ner_company_from_text(text, transformer=True):
    text = lower_case(text)
    doc = None
    if transformer:
        doc = transformer_nlp(text)
    else:
        doc = nlp(text)

    orgs = {}
    for ent in doc.ents:
        if ent.label_ == 'ORG':
            orgs[ent.text] = 1 if ent.text not in orgs else orgs[ent.text] + 1
    return set(orgs.keys())
