from _collections import defaultdict
import logging
from optparse import make_option

from django.core.management.base import BaseCommand
from six import iteritems

from ... import Bungiesearch
from ...utils import update_index


class Command(BaseCommand):
    args = ''
    help = 'Manage search index.'

    option_list = BaseCommand.option_list + (
        make_option('--create',
            action='store_const',
            dest='action',
            const='create',
            help='Create the index specified in the settings with the mapping generating from the search indices.'),
        make_option('--update',
            action='store_const',
            dest='action',
            const='update',
            help='Update the index specified in the settings with the mapping generating from the search indices.'),
        make_option('--update-mapping',
            action='store_const',
            dest='action',
            const='update-mapping',
            help='Update the mapping of specified models (or all models) on the index specified in the settings.'),
        make_option('--delete',
            action='store_const',
            dest='action',
            const='delete',
            help='Delete the index specified in the settings. Requires the "--guilty-as-charged" flag.'),
        make_option('--delete-mapping',
            action='store_const',
            dest='action',
            const='delete-mapping',
            help='Delete the mapping of specified models (or all models) on the index specified in the settings. Requires the "--guilty-as-charged" flag.'),
        make_option('--guilty-as-charged',
            action='store_true',
            dest='confirmed',
            default=False,
            help='Flag needed to delete an index.'),
        make_option('--models',
            action='store',
            dest='models',
            default=None,
            help='Models to be updated, separated by commas. If none are specified, then all models defined in the index will be updated.'),
        make_option('--index',
            action='store',
            dest='index',
            default=None,
            help='Specify the index for which to apply the action, as defined in BUNGIESEARCH.INDEXES of settings. Defaults to using all indices.'),
        make_option('--bulk-size',
            action='store',
            dest='bulk_size',
            default=100,
            type='int',
            help='Specify the number of items to be updated together.'),
        make_option('--num-docs',
            action='store',
            dest='num_docs',
            default=-1,
            type='int',
            help='Specify the maximum number of items to be indexed. By default will index the whole model.'),
        make_option('--start',
            action='store',
            dest='start_date',
            default=None,
            type='str',
            help='Specify the start date and time of documents to be indexed.'),
        make_option('--end',
            action='store',
            dest='end_date',
            default=None,
            type='str',
            help='Specify the end date and time of documents to be indexed.'),
        )

    def handle(self, *args, **options):
        del args
        logging.basicConfig(level='INFO')

        src = Bungiesearch()
        es = src.get_es_instance()

        if not options['action']:
            raise ValueError('No action specified. Must be one of "create", "update" or "delete".')

        if options['action'].startswith('delete'):
            if not options['confirmed']:
                raise ValueError('If you know what a delete operation does (on index or mapping), add the --guilty-as-charged flag.')
            if options['action'] == 'delete':
                if options['index']:
                    indices = [options['index']]
                else:
                    indices = src.get_indices()

                for index in indices:
                    logging.warning('Deleting elastic search index {}.'.format(index))
                    es.indices.delete(index=index, ignore=404)

            else:
                index_to_doctypes = defaultdict(list)
                if options['models']:
                    logging.info('Deleting mapping for models {} on index {}.'.format(options['models'], index))
                    for model_name in options['models'].split():
                        for index in src.get_index(model_name):
                            index_to_doctypes[index].append(model_name)
                elif options['index']:
                    index = options['index']
                    logging.info('Deleting mapping for all models on index {}.'.format(index))
                    index_to_doctypes[index] = src.get_models(index)
                else:
                    for index in src.get_indices():
                        index_to_doctypes[index] = src.get_models(index)
                    logging.info('Deleting mapping for all models ({}) on all indices ({}).'.format(index_to_doctypes.values(), index_to_doctypes.keys()))

                for index, doctype_list in iteritems(index_to_doctypes):
                    es.indices.delete_mapping(index, ','.join(doctype_list), params=None)

        elif options['action'] == 'create':
            if options['index']:
                indices = [options['index']]
            else:
                indices = src.get_indices()
            for index in indices:
                mapping = {}
                for mdl_idx in src.get_model_indices(index):
                    mapping[mdl_idx.get_model().__name__] = mdl_idx.get_mapping()

                analysis = mdl_idx.collect_analysis()

                logging.info('Creating index {} with {} doctypes.'.format(index, len(mapping)))
                es.indices.create(index=index, body={'mappings': mapping, 'settings': {'analysis': analysis}})

            es.cluster.health(index=','.join(indices), wait_for_status='green', timeout=30)

        elif options['action'] == 'update-mapping':
            if options['index']:
                indices = [options['index']]
            else:
                indices = src.get_indices()

            if options['models']:
                models = options['models'].split(',')
            else:
                models = []

            for index in indices:
                for model_name in src._idx_name_to_mdl_to_mdlidx[index]:
                    if models and model_name not in models:
                        continue
                    logging.info('Updating mapping of model/doctype {} on index {}.'.format(model_name, index))
                    try:
                        es.indices.put_mapping(model_name, src._idx_name_to_mdl_to_mdlidx[index][model_name].get_mapping(), index=index)
                    except Exception as e:
                        print(e)
                        if raw_input('Something terrible happened! Type "abort" to stop updating the mappings: ') == 'abort':
                            raise e
                        print('Continuing.')

        else:
            if options['index']:
                indices = options['index']
            else:
                indices = src.get_indices()
            if options['models']:
                model_names = options['models'].split(',')
            else:
                model_names = [model for index in indices for model in src.get_models(index)]

            logging.info('Updating models {} on indices {}.'.format(model_names, indices))

            # Update index.
            for model_name in model_names:
                if src.get_model_index(model_name).indexing_query is not None:
                    update_index(src.get_model_index(model_name).indexing_query, model_name, bulk_size=options['bulk_size'], num_docs=options['num_docs'], start_date=options['start_date'], end_date=options['end_date'])
                else:
                    update_index(src.get_model_index(model_name).get_model().objects.all(), model_name, bulk_size=options['bulk_size'], num_docs=options['num_docs'], start_date=options['start_date'], end_date=options['end_date'])
