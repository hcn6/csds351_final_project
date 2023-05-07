import spacy

nlp = spacy.load("en_core_web_sm")

def ner_company_from_text(text):
    doc = nlp(text)
    orgs = {}
    for ent in doc.ents:
        if ent.label_ == 'ORG':
            orgs[ent.text] = 1 if ent.text not in orgs else orgs[ent.text] + 1
    return set(orgs.keys())
