import json
import logging
import traceback
from typing import Optional
from elasticsearch_dsl import Search
from utils import heconstants
from config.logconfig import get_logger

logger = get_logger()


class Index:

    def __init__(self):
        pass

    def search(self, search_query, sort_by=None, source_include: Optional = None, index: Optional[str] = None):
        try:
            if index:
                s = Search(using=heconstants.es_client, index=index)
            else:
                s = Search(using=heconstants.es_client, index=heconstants.transcript_index)
            s = s.query(search_query)

            if source_include:
                if isinstance(source_include, str):
                    source_include = [source_include]
                elif not isinstance(source_include, list):
                    raise ValueError("_source_include must be a string or a list of strings")
                s = s.source(includes=source_include)

            if sort_by:
                if not isinstance(sort_by, tuple) or len(sort_by) != 2:
                    raise ValueError("sort_by must be a tuple of (field, order)")
                field, order = sort_by
                s = s.sort({f"{field}": {"order": f"{order}"}})

            s = s.extra(from_=0, size=1000)  # Consider parameterizing these values
            response = s.execute().to_dict()
            return response['hits']['hits']
        except:
            logger.info(
                f"Couldn't find the document in {heconstants.transcript_index}"
            )
            pass

    def update(self, script_body, doc_id, index: Optional[str] = None):
        try:
            # logger.info(f"updating document: {doc_id} :: {script_body}")

            if index:
                update_response = heconstants.es_client.update(index=index,
                                                               id=doc_id,
                                                               body=script_body, refresh=True)
            else:
                update_response = heconstants.es_client.update(index=heconstants.transcript_index,
                                                               id=doc_id,
                                                               body=script_body, refresh=True)
            esresult = update_response["result"]
            # logger.info(f"update document: {esresult}")
            if esresult not in ["created", "updated", "noop"]:
                msg = "Unable to update document with id :: {}".format(
                    doc_id)
                logger.info(msg)
                logger.info(msg)
                raise Exception(msg) from None
            else:
                logger.info(f"Document updated succesfully")

        except Exception as exc:
            msg = "Failed to update data in {} with exception :: {}".format(heconstants.transcript_index, exc)
            trace = traceback.format_exc()
            logger.error(msg, trace)
            pass

    def add(self, data, id: Optional[str] = None, index: Optional[str] = None):
        try:
            # logger.info(f"adding document :: {id}")
            if not index:
                index = heconstants.transcript_index

            if id:
                res = heconstants.es_client.index(index=index,
                                                  doc_type=heconstants.es_type, body=json.dumps(data),
                                                  id=id, refresh=True)
            else:
                res = heconstants.es_client.index(index=index,
                                                  doc_type=heconstants.es_type, body=json.dumps(data), refresh=True)
            # logger.info(f"adding document resp :: {res}")
            esresult = res.get("result")
            # logger.info(f"add document: {esresult}")
            if esresult not in ["created", "updated"]:
                msg = "Unable to add document :: {}".format(json.dumps(data))
                logger.info(msg)
                raise Exception(msg) from None
            else:
                logger.info(f"Document added succesfully")

        except Exception as exc:
            msg = "Failed to add data in {} with exception :: {}".format(heconstants.transcript_index, exc)
            trace = traceback.format_exc()
            logger.error(msg, trace)
            pass
