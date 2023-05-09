import spacy
from thinc.api import set_gpu_allocator, require_gpu
from database import db
from tqdm import tqdm
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

    if transformer and len(doc) > nlp.max_length:
        doc = doc[:nlp.max_length]

    orgs = {}
    for ent in doc.ents:
        if ent.label_ == 'ORG':
            orgs[ent.text] = 1 if ent.text not in orgs else orgs[ent.text] + 1
    return set(orgs.keys())

def ner_company_from_many_texts(texts):
    orgs_list = []
    for doc in transformer_nlp.pipe(texts, batch_size=64):
        orgs = {}
        for ent in doc.ents:
            if ent.label_ == 'ORG':
                orgs[ent.text] = 1 if ent.text not in orgs else orgs[ent.text] + 1
        orgs_list.append(list(orgs.keys()))
    return orgs_list
    # for text in texts:
    #     orgs_in_text = ner_company_from_text(text, transformer)
    #     for org in orgs_in_text:
    #         orgs[org] = 1 if org not in orgs else orgs[org] + 1
    # return set(orgs.keys())


if __name__ == "__main__":
    comment_db_url = "mongodb+srv://dxn183:NBq4c7oQaFm7kaOD@cluster1.ylkmwu2.mongodb.net/"
    post_db_url = "mongodb+srv://colab:Hieu1234@hieubase.r9ivh.gcp.mongodb.net/?retryWrites=true&w=majority"
    comment_output_url = "mongodb+srv://dxn183:P4TnUn0wuNZqztQx@cluster0.7tqovhs.mongodb.net/"

    data_collection = db.get_collection_by_url(url=post_db_url, db_name='reddit_data', collection_name='reddit_post')
    output_collection = db.get_collection_by_url(url=comment_output_url, db_name='reddit_data', collection_name='reddit_post_ner')
    # data_collection = db.get_collection_by_url(url=comment_db_url, db_name='reddit_data', collection_name='reddit_comment_praw')
    # output_collection = db.get_collection_by_url(url=comment_output_url, db_name='reddit_data', collection_name='reddit_comment_ner')

    print("Querying data...")
    # Loop over the documents in the source collection and insert them into the destination collection
    all_data = list(data_collection.find({}))

    chunk_size = 10000
    chunks = []
    for i in range(0, len(all_data), chunk_size):
        chunk = all_data[i:i+chunk_size]
        chunks.append(chunk)
    
    pbar = tqdm(total=len(all_data))
    pbar2 = tqdm(total=len(chunks))

    print("Total data: ", len(all_data))

    for chunk in chunks:
        data_for_insert = []

        orgs_list = ner_company_from_many_texts([data['selftext'] for data in chunk])
        for org, data in zip(orgs_list, chunk):
            data_for_insert.append({
                '_id': data['_id'],
                'orgs': org
            })
            pbar.update(1)
        # for data in chunk:
        #     orgs = ner_company_from_many_texts(data['selftext'])
        #     data_for_insert.append({
        #         '_id': data['_id'],
        #         'orgs': list(orgs)
        #     })
        output_collection.insert_many(data_for_insert)
    pbar.close()

    
