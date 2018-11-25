import base64
from uuid import uuid4
from elasticsearch.client.ingest import IngestClient
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import analyzer, DocType, Index
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.field import Attachment, Text


# Establish a connection
host = '127.0.0.1'
port = 9200
es = connections.create_connection(host=host, port=port)

# Some custom analyzers
html_strip = analyzer('html_strip', tokenizer="standard", filter=["standard", "lowercase", "stop", "snowball"],
                      char_filter=["html_strip"])
lower_keyword = analyzer('keyword', tokenizer="keyword", filter=["lowercase"])


class ExampleIndex(DocType):
    class Meta:
        index = 'example'
        doc_type = 'Example'

    id = Text()
    uuid = Text()
    name = Text()
    town = Text(analyzer=lower_keyword)
    my_file = Attachment(analyzer=html_strip)


def save_document(doc):
    """

    :param obj doc: Example object containing values to save
    :return:
    """
    try:
        # Create the Pipeline BEFORE creating the index
        p = IngestClient(es)
        p.put_pipeline(id='myattachment', body={
            'description': "Extract attachment information",
            'processors': [
                {
                    "attachment": {
                        "field": "my_file"
                    }
                }
            ]
        })

        # Create the index. An exception will be raise if it already exists
        i = Index('example')
        i.doc_type(ExampleIndex)
        i.create()
    except Exception:
        # todo - should be restricted to the expected Exception subclasses
        pass

    indices = ExampleIndex()
    try:
        s = indices.search()
        r = s.query('match', uuid=doc.uuid).execute()
        if r.success():
            for h in r:
                indices = ExampleIndex.get(id=h.meta.id)
                break
    except NotFoundError:
        # New record
        pass
    except Exception:
        print("Unexpected error")
        raise

    # Now set the doc properties
    indices.uuid = doc.uuid
    indices.name = doc.name
    indices.town = doc.town
    if doc.my_file:
        with open(doc.my_file, 'rb') as f:
            contents = f.read()
        indices.my_file = base64.b64encode(contents).decode("ascii")

    # Save the index, using the Attachment pipeline if a file was attached
    return indices.save(pipeline="myattachment") if indices.my_file else indices.save()


class MyObj(object):
    uuid = uuid4()
    name = ''
    town = ''
    my_file = ''

    def __init__(self, name, town, file):
        self.name = name
        self.town = town
        self.my_file = file


me = MyObj("Steve", "London", '/home/steve/Documents/test.txt')

res = save_document(me)