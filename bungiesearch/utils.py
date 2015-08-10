import logging

from dateutil.parser import parse as parsedt
from django.utils import timezone
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import bulk_index

from . import Bungiesearch


def update_index(model_items, model_name, action='index', bulk_size=100, num_docs=-1, start_date=None, end_date=None, refresh=True):
    '''
    Updates the index for the provided model_items.
    :param model_items: a list of model_items (django Model instances, or proxy instances) which are to be indexed/updated or deleted.
    If action is 'index', the model_items must be serializable objects. If action is 'delete', the model_items must be primary keys
    corresponding to obects in the index.
    :param model_name: doctype, which must also be the model name.
    :param action: the action that you'd like to perform on this group of data. Must be in ('index', 'delete') and defaults to 'index.'
    :param bulk_size: bulk size for indexing. Defaults to 100.
    :param num_docs: maximum number of model_items from the provided list to be indexed.
    :param start_date: start date for indexing. Must be as YYYY-MM-DD.
    :param end_date: end date for indexing. Must be as YYYY-MM-DD.
    :param refresh: a boolean that determines whether to refresh the index, making all operations performed since the last refresh
    immediately available for search, instead of needing to wait for the scheduled Elasticsearch execution. Defaults to True.
    :note: If model_items contain multiple models, then num_docs is applied to *each* model. For example, if bulk_size is set to 5,
    and item contains models Article and Article2, then 5 model_items of Article *and* 5 model_items of Article2 will be indexed.
    '''
    src = Bungiesearch()

    if action == 'delete' and not isinstance(model_items, (list, tuple, set)):
        raise ValueError("If action is 'delete', model_items must be an iterable of primary keys.")

    logging.info('Getting index for model {}.'.format(model_name))
    for index_name in src.get_index(model_name):
        index_instance = src.get_model_index(model_name)
        model = index_instance.get_model()

        if num_docs == -1:
            if isinstance(model_items, (list, tuple, set)):
                num_docs = len(model_items)
            else:
                model_items = filter_model_items(index_instance, model_items, model_name, start_date, end_date)
                num_docs = model_items.count()
        else:
            logging.warning('Limiting the number of model_items to {} to {}.'.format(action, num_docs))

        logging.info('{} {} documents on index {}'.format(action, num_docs, index_name))
        prev_step = 0
        max_docs = num_docs + bulk_size if num_docs > bulk_size else bulk_size + 1
        for next_step in range(bulk_size, max_docs, bulk_size):
            logging.info('{}: documents {} to {} of {} total on index {}.'.format(action.capitalize(), prev_step, next_step, num_docs, index_name))
            data = create_indexed_document(index_instance, model_items[prev_step:next_step], action)
            bulk_index(src.get_es_instance(), data, index=index_name, doc_type=model.__name__, raise_on_error=True)
            prev_step = next_step

        if refresh:
            src.get_es_instance().indices.refresh(index=index_name)

def delete_index_item(item, model_name, refresh=True):
    '''
    Deletes an item from the index.
    :param item: must be a serializable object.
    :param model_name: doctype, which must also be the model name.
    :param refresh: a boolean that determines whether to refresh the index, making all operations performed since the last refresh
    immediately available for search, instead of needing to wait for the scheduled Elasticsearch execution. Defaults to True.
    '''
    src = Bungiesearch()

    logging.info('Getting index for model {}.'.format(model_name))
    for index_name in src.get_index(model_name):
        index_instance = src.get_model_index(model_name)
        item_es_id = index_instance.fields['_id'].value(item)
        try:
            src.get_es_instance().delete(index_name, model_name, item_es_id)
        except NotFoundError as e:
            logging.warning('NotFoundError: could not delete {}.{} from index {}: {}.'.format(model_name, item_es_id, index_name, str(e)))

        if refresh:
            src.get_es_instance().indices.refresh(index=index_name)

def create_indexed_document(index_instance, model_items, action):
    '''
    Creates the document that will be passed into the bulk index function.
    Either a list of serialized objects to index, or a a dictionary specifying the primary keys of items to be delete.
    '''
    data = []
    if action == 'delete':
        for pk in model_items:
            data.append({'_id': pk, '_op_type': action})
    else:
        for doc in model_items:
            if index_instance.matches_indexing_condition(doc):
                data.append(index_instance.serialize_object(doc))
    return data

def filter_model_items(index_instance, model_items, model_name, start_date, end_date):
    ''' Filters the model items queryset based on start and end date.'''
    if index_instance.updated_field is None:
        logging.warning("No updated date field found for {} - not restricting with start and end date".format(model_name))
    else:
        if start_date:
            model_items = model_items.filter(**{'{}__gte'.format(index_instance.updated_field): __str_to_tzdate__(start_date)})
        if end_date:
            model_items = model_items.filter(**{'{}__lte'.format(index_instance.updated_field): __str_to_tzdate__(end_date)})

    return model_items

def __str_to_tzdate__(date_str):
    return timezone.make_aware(parsedt(date_str), timezone.get_current_timezone())
