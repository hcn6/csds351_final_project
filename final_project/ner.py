import spacy

nlp = spacy.load("en_core_web_trf")

def lower_case(text):
    return text.lower()

def ner_company_from_text(text):
    text = lower_case(text)
    doc = nlp(text)
    orgs = {}
    for ent in doc.ents:
        if ent.label_ == 'ORG':
            orgs[ent.text] = 1 if ent.text not in orgs else orgs[ent.text] + 1
    return set(orgs.keys())
