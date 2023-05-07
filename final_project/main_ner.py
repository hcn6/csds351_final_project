import ner

text = "Hi! I have a project macthing interview with Google and Meta. Does anyone have any ideas on what the best things to talk about will be?"


print(ner.ner_company_from_text(text))