from datetime import datetime

from bungiesearch import Bungiesearch
from bungiesearch.utils import update_index
from django.core.management import call_command
from django.test import TestCase
import pytz
from six import iteritems

from core.models import Article, User, Unmanaged, NoUpdatedField, ManangedButEmpty
from core.search_indices import ArticleIndex, UserIndex
from core.bungie_signal import BungieTestSignalProcessor


class CoreTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        # Let's start by creating the index and mapping.
        # If we create an object before the index, the index
        # will be created automatically, and we want to test the command.
        call_command('search_index', action='create')

        art_1 = {'title': 'Title one',
                 'description': 'Description of article 1.',
                 'text_field': '',
                 'link': 'http://example.com/article_1',
                 'published': pytz.UTC.localize(datetime(year=2020, month=9, day=15)),
                 'updated': pytz.UTC.localize(datetime(year=2014, month=9, day=10)),
                 'tweet_count': 20,
                 'source_hash': 159159159159,
                 'missing_data': '',
                 'positive_feedback': 50,
                 'negative_feedback': 5,
                 }

        user_1 = {'user_id': 'bungie1',
                  'description': 'Description of user 1',
                  'created': pytz.UTC.localize(datetime(year=2015, month=1, day=1)),
                  'updated': pytz.UTC.localize(datetime(year=2015, month=6, day=1)),
                 }

        Article.objects.create(**art_1)
        User.objects.create(**user_1)

        art_2 = dict((k, v) for k, v in iteritems(art_1))
        art_2['link'] += '/page2'
        art_2['title'] = 'Title two'
        art_2['description'] = 'This is a second article.'
        art_2['text_field'] = None
        art_2['published'] = pytz.UTC.localize(datetime(year=2010, month=9, day=15))

        user_2 = dict((k, v) for k, v in iteritems(user_1))
        user_2['user_id'] = 'bungie2'
        user_2['description'] = 'This is the second user'
        user_2['created'] = pytz.UTC.localize(datetime(year=2010, month=9, day=15))

        Article.objects.create(**art_2)
        User.objects.create(**user_2)
        NoUpdatedField.objects.create(title='My title', description='This is a short description.')

        call_command('rebuild_index', interactive=False, confirmed='guilty-as-charged')

    def test_count_after_clear(self):
        # can flake because elasticsearch create API is asynchronous
        self.assertEqual(Article.objects.search_index('bungiesearch_demo').count(), 2)
        call_command('rebuild_index', interactive=False, confirmed='guilty-as-charged')
        self.assertEqual(Article.objects.search_index('bungiesearch_demo').count(), 2)

    @classmethod
    def tearDownClass(cls):
        call_command('search_index', action='delete', confirmed='guilty-as-charged')

    def test_model_index_generation(self):
        '''
        Check that the mapping is the expected one.
        '''
        expected_article = {'properties': {'updated': {'type': 'date', 'null_value': '2013-07-01'},
                                           'description': {'type': 'string', 'boost': 1.35, 'analyzer': 'snowball'},
                                           'text': {'type': 'string', 'analyzer': 'edge_ngram_analyzer'},
                                           'text_field': {'type': 'string', 'analyzer': 'snowball'},
                                           'created': {'type': 'date'},
                                           'title': {'type': 'string', 'boost': 1.75, 'analyzer': 'snowball'},
                                           'authors': {'type': 'string', 'analyzer': 'snowball'},
                                           'meta_data': {'type': 'string', 'analyzer': 'snowball'},
                                           'link': {'type': 'string', 'analyzer': 'snowball'},
                                           'effective_date': {'type': 'date'},
                                           'tweet_count': {'type': 'integer'},
                                           'id': {'type': 'integer'},
                                           '_id': {'type': 'integer'}, # This is the elastic search index.
                                           'published': {'type': 'date'}}
                           }
        expected_user = {'properties': {'updated': {'type': 'date'},
                                        'description': {'type': 'string', 'analyzer': 'edge_ngram_analyzer'},
                                        'int_description': {'type': 'integer'},
                                        'user_id': {'analyzer': 'snowball', 'type': 'string'},
                                        'effective_date': {'type': 'date'},
                                        'created': {'type': 'date'},
                                        'name': {'analyzer': 'snowball', 'type': 'string'},
                                        '_id': {'analyzer': 'snowball', 'type': 'string'}}
                        }

        self.assertEqual(ArticleIndex().get_mapping(), expected_article)
        self.assertEqual(UserIndex().get_mapping(), expected_user)

    def test_fetch_item(self):
        '''
        Test searching and mapping.
        '''
        self.assertEqual(Article.objects.search.query('match', _all='Description')[0], Article.objects.get(title='Title one'), 'Searching for "Description" did not return just the first Article.')
        self.assertEqual(Article.objects.search.query('match', _all='second article')[0], Article.objects.get(title='Title two'), 'Searching for "second article" did not return the second Article.')

        self.assertEqual(User.objects.search.query('match', _all='Description')[0], User.objects.get(user_id='bungie1'), 'Searching for "Description" did not return the User.')
        self.assertEqual(User.objects.search.query('match', _all='second user')[0], User.objects.get(user_id='bungie2'), 'Searching for "second user" did not return the User.')

    def test_raw_fetch(self):
        '''
        Test searching and mapping.
        '''
        item = Article.objects.search.query('match', _all='Description')[:1:True]
        self.assertTrue(hasattr(item, 'meta'), 'Fetching first raw results did not return an object with a meta attribute.')

        item = User.objects.search.query('match', _all='Description')[:1:True]
        self.assertTrue(hasattr(item, 'meta'), 'Fetching first raw results did not return an object with a meta attribute.')

    def test_iteration(self):
        '''
        Tests iteration on Bungiesearch items.
        '''
        lazy_search_article = Article.objects.search.query('match', title='title')
        db_items = list(Article.objects.all())
        self.assertTrue(all([result in db_items for result in lazy_search_article]), 'Searching for title "title" did not return all articles.')
        self.assertTrue(all([result in db_items for result in lazy_search_article[:]]), 'Searching for title "title" did not return all articles when using empty slice.')
        self.assertEqual(len(lazy_search_article[:1]), 1, 'Get item with start=None and stop=1 did not return one item.')
        self.assertEqual(len(lazy_search_article[:2]), 2, 'Get item with start=None and stop=2 did not return two item.')

        lazy_search_user = User.objects.search.query('match', description='user')
        db_items = list(User.objects.all())
        self.assertTrue(all([result in db_items for result in lazy_search_user]), 'Searching for description "user" did not return all articles.')
        self.assertTrue(all([result in db_items for result in lazy_search_user[:]]), 'Searching for description "user" did not return all articles when using empty slice.')
        self.assertEqual(len(lazy_search_user[:1]), 1, 'Get item with start=None and stop=1 did not return one item.')
        self.assertEqual(len(lazy_search_user[:2]), 2, 'Get item with start=None and stop=2 did not return two item.')

    def test_no_results(self):
        '''
        Test empty results.
        '''
        self.assertEqual(list(Article.objects.search.query('match', _all='nothing')), [], 'Searching for "nothing" did not return an empty list on iterator call.')
        self.assertEqual(Article.objects.search.query('match', _all='nothing')[:10], [], 'Searching for "nothing" did not return an empty list on get item call.')

        self.assertEqual(list(User.objects.search.query('match', _all='nothing')), [], 'Searching for "nothing" did not return an empty list on iterator call.')
        self.assertEqual(list(User.objects.search.query('match', _all='nothing')), [], 'Searching for "nothing" did not return an empty list on iterator call.')

    def test_custom_search(self):
        '''
        Test searching on custom index and doc_type.
        '''
        search = Article.objects.custom_search(index='bungiesearch_demo', doc_type='Article')
        es_art1 = search.query('match', _all='Description')[0]
        db_art1 = Article.objects.get(title='Title one')
        es_art2 = search.query('match', _all='second article')[0]
        db_art2 = Article.objects.get(title='Title two')
        self.assertTrue(all([es_art1.id == db_art1.id, es_art1.title == db_art1.title, es_art1.description == db_art1.description]), 'Searching for "Description" did not return the first Article.')
        self.assertTrue(all([es_art2.id == db_art2.id, es_art2.title == db_art2.title, es_art2.description == db_art2.description]), 'Searching for "second article" did not return the second Article.')

        search = User.objects.custom_search(index='bungiesearch_demo', doc_type='User')
        es_user1 = search.query('match', _all='Description')[0]
        db_user1 = User.objects.get(user_id='bungie1')
        self.assertRaises(AttributeError, getattr, es_user1, 'id')
        self.assertTrue(all([es_user1.user_id == db_user1.user_id, es_user1.description == db_user1.description]), 'Searching for "Description" did not return the first User.')

    def test_get_model(self):
        '''
        Test model mapping.
        '''
        self.assertEqual(ArticleIndex().get_model(), Article, 'Model was not Article.')
        self.assertEqual(UserIndex().get_model(), User, 'Model was not User')

    def test_cloning(self):
        '''
        Tests that Bungiesearch remains lazy with specific function which should return clones.
        '''
        inst = Article.objects.search.query('match', _all='Description')
        self.assertIsInstance(inst.only('_id'), inst.__class__, 'Calling `only` does not return a clone of itself.')

        inst = User.objects.search.query('match', _all='Description')
        self.assertIsInstance(inst.only('_id'), inst.__class__, 'Calling `only` does not return a clone of itself.')

    def test_search_alias_exceptions(self):
        '''
        Tests that invalid aliases raise exceptions.
        '''
        self.assertRaises(AttributeError, getattr, Article.objects, 'bsearch_no_such_alias')
        self.assertRaises(NotImplementedError, Article.objects.bsearch_invalidalias)
        self.assertRaises(ValueError, getattr, Article.objects.search.bsearch_title('title query').bsearch_titlefilter('title filter'), 'bsearch_noupdatedmdlonly')

    def test_search_aliases(self):
        '''
        Tests search alias errors and functionality.
        '''
        title_alias = Article.objects.bsearch_title_search('title')
        db_items = list(Article.objects.all())
        self.assertEqual(title_alias.to_dict(), {'query': {'match': {'title': 'title'}}}, 'Title alias search did not return the expected JSON query.')
        self.assertTrue(all([result in db_items for result in title_alias]), 'Alias searching for title "title" did not return all articles.')
        self.assertTrue(all([result in db_items for result in title_alias[:]]), 'Alias searching for title "title" did not return all articles when using empty slice.')
        self.assertEqual(len(title_alias[:1]), 1, 'Get item on an alias search with start=None and stop=1 did not return one item.')
        self.assertEqual(len(title_alias[:2]), 2, 'Get item on an alias search with start=None and stop=2 did not return two item.')
        self.assertEqual(title_alias.to_dict(), Article.objects.bsearch_title('title').to_dict(), 'Alias applicable to all models does not return the same JSON request body as the model specific one.')
        self.assertEqual(NoUpdatedField.objects.search.filter('term', title='My title').to_dict(), NoUpdatedField.objects.bsearch_noupdatedmdlonly('My title').to_dict(), 'Alias applicable only to NoUpdatedField does not generate the correct filter.')

    def test_bungie_instance_search_aliases(self):
        alias_dictd = Article.objects.search.bsearch_title('title query').bsearch_titlefilter('title filter').to_dict()
        expected = {'query': {'filtered': {'filter': {'term': {'title': 'title filter'}}, 'query': {'match': {'title': 'title query'}}}}}
        self.assertEqual(alias_dictd, expected, 'Alias on Bungiesearch instance did not return the expected dictionary.')

    def test_search_alias_model(self):
        self.assertEqual(Article.objects.bsearch_get_alias_for_test().get_model(), Article, 'Unexpected get_model information on search alias.')
        self.assertEqual(Article.objects.search.bsearch_title('title query').bsearch_get_alias_for_test().get_model(), Article, 'Unexpected get_model information on search alias.')
        self.assertRaises(ValueError, Bungiesearch().bsearch_get_alias_for_test().get_model)

    def test_post_save(self):
        art = {'title': 'Title three',
               'description': 'Postsave',
               'link': 'http://example.com/sparrho',
               'published': pytz.UTC.localize(datetime(year=2020, month=9, day=15)),
               'updated': pytz.UTC.localize(datetime(year=2014, month=9, day=10)),
               'tweet_count': 20,
               'source_hash': 159159159159,
               'missing_data': '',
               'positive_feedback': 50,
               'negative_feedback': 5}
        obj = Article.objects.create(**art)
        find_three = Article.objects.search.query('match', title='three')
        self.assertEqual(len(find_three), 2, 'Searching for "three" in title did not return exactly two items (got {}).'.format(find_three))
        # Let's check that both returned items are from different indices.
        self.assertNotEqual(find_three[0:1:True].meta.index, find_three[1:2:True].meta.index, 'Searching for "three" did not return items from different indices.')
        # Let's now delete this object to test the post delete signal.
        obj.delete()

    def test_bulk_delete(self):
        '''
        This tests that using the update_index function with 'delete' as the action performs a bulk delete operation on the data.
        '''
        bulk_art1 = {'title': 'Title four',
                     'description': 'Bulk delete first',
                     'link': 'http://example.com/bd1',
                     'published': pytz.UTC.localize(datetime(year=2015, month=7, day=13)),
                     'updated': pytz.UTC.localize(datetime(year=2015, month=7, day=20)),
                     'tweet_count': 20,
                     'source_hash': 159159159159,
                     'missing_data': '',
                     'positive_feedback': 50,
                     'negative_feedback': 5}
        bulk_art2 = {'title': 'Title five',
                     'description': 'Bulk delete second',
                     'link': 'http://example.com/bd2',
                     'published': pytz.UTC.localize(datetime(year=2015, month=7, day=13)),
                     'updated': pytz.UTC.localize(datetime(year=2015, month=7, day=20)),
                     'tweet_count': 20,
                     'source_hash': 159159159159,
                     'missing_data': '',
                     'positive_feedback': 50,
                     'negative_feedback': 5}

        bulk_obj1 = Article.objects.create(**bulk_art1)
        bulk_obj2 = Article.objects.create(**bulk_art2)

        find_five = Article.objects.search.query('match', title='five')
        self.assertEqual(len(find_five), 2, 'Searching for "five" in title did not return exactly two results (got {})'.format(find_five))

        model_items = [bulk_obj1.pk, bulk_obj2.pk]
        model_name = Article.__name__
        update_index(model_items, model_name, action='delete', bulk_size=2, num_docs=-1, start_date=None, end_date=None, refresh=True)

        find_four = Article.objects.search.query('match', title='four')
        self.assertEqual(len(find_four), 0, 'Searching for "four" in title did not return exactly zero results (got {})'.format(find_four))
        find_five = Article.objects.search.query('match', title='five')
        self.assertEqual(len(find_five), 0, 'Searching for "five" in title did not return exactly zero results (got {})'.format(find_five))

    def test_manager_interference(self):
        '''
        This tests that saving an object which is not managed by Bungiesearch won't try to update the index for that model.
        '''
        Unmanaged.objects.create(title='test', description='blah')

    def test_time_indexing(self):
        update_index(Article.objects.all(), 'Article', start_date=datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M'))
        update_index(NoUpdatedField.objects.all(), 'NoUpdatedField', end_date=datetime.strftime(datetime.now(), '%Y-%m-%d'))

    def test_optimal_queries(self):
        db_item = NoUpdatedField.objects.get(pk=1)
        src_item = NoUpdatedField.objects.search.query('match', title='My title')[0]
        self.assertEqual(src_item.id, db_item.id, 'Searching for the object did not return the expected object id.')
        self.assertTrue(src_item._meta.proxy, 'Was expecting a proxy model after fetching item.')
        self.assertEqual(src_item._meta.proxy_for_model, NoUpdatedField, 'Proxy for model of search item is not "NoUpdatedField".')

    def test_concat_queries(self):
        items = Article.objects.bsearch_title_search('title')[::False] + NoUpdatedField.objects.search.query('match', title='My title')[::False]
        for item in items:
            model = item._meta.proxy_for_model if item._meta.proxy_for_model else type(item)
            self.assertIn(model, [Article, NoUpdatedField], 'Got an unmapped item ({}), or an item with an unexpected mapping.'.format(type(item)))

    def test_data_templates(self):
        # One article has a title that contains 'one'
        match_one = Article.objects.search.query('match', text='one')
        self.assertEqual(len(match_one), 2, 'Searching for "one" in text did not return exactly one item (got {}).'.format(match_one))
        self.assertEqual(match_one[0].title, 'Title one', 'Searching for "one" in text did not yield the first article (got {})'.format(match_one[0].title))

        # Two articles have a description that contain 'article'
        match_two = Article.objects.search.query('match', text='article')
        self.assertEqual(len(match_two), 4, 'Searching for "article" in text did not return exactly two items (got {})'.format(match_two))

        # Two articles have a link with 'example,' but since link isn't in the template, there should be zero results
        match_zero = Article.objects.search.query('match', text='example')
        self.assertEqual(len(match_zero), 0, 'Searching for "article" in text did not return exactly zero items (got {})'.format(match_zero))

    def test_fields(self):
        '''
        Checking that providing a specific field will correctly fetch these items from elasticsearch.
        '''
        for mdl, id_field in [(Article, 'id'), (User, 'user_id')]:
            raw_items = mdl.objects.search.fields('_id')[:5:True]
            self.assertTrue(all([dir(raw) == ['meta'] for raw in raw_items]), 'Requesting only _id returned more than just meta info from ES for model {}.'.format(mdl))
            items = mdl.objects.search.fields('_id')[:5]
            self.assertTrue(all([dbi in items for dbi in mdl.objects.all()]), 'Mapping after fields _id only search did not return all results for model {}.'.format(mdl))
            items = mdl.objects.search.fields([id_field, '_id', '_source'])[:5]
            self.assertTrue(all([dbi in items for dbi in mdl.objects.all()]), 'Mapping after fields _id, id and _source search did not return all results for model {}.'.format(mdl))

    def test_prepare_field(self):
        '''
        Check that providing a method to calculate the value of a field will yield correct results in the search index.
        '''
        user_int_description = {'user_id': 'bungie3',
                                'description': '123',
                                'created': pytz.UTC.localize(datetime(year=2015, month=1, day=1)),
                                'updated': pytz.UTC.localize(datetime(year=2015, month=6, day=1)),
                                }
        User.objects.create(**user_int_description)

        find_one = User.objects.search.filter('term', int_description=1)
        self.assertEqual(len(find_one), 4, 'Searching for users with default int description did not return exactly 4 items (got {})'.format(find_one))

        find_123 = User.objects.search.filter('term', int_description=123)
        self.assertEqual(len(find_one), 4, 'Searching for users with int description 123 did not return exactly 2 items (got {})'.format(find_123))

        find_zero = User.objects.search.filter('term', int_description=0)
        self.assertEqual(len(find_zero), 0, 'Searching for users with int description zero did not return exactly 0 items (got {})'.format(find_zero))

    def test_fun(self):
        '''
        Test fun queries.
        '''
        lazy = Article.objects.bsearch_title_search('title').only('pk').fields('_id')
        print(len(lazy)) # Returns the total hits computed by elasticsearch.
        assert all([type(item) == Article for item in lazy.filter('range', effective_date={'lte': '2014-09-22'})[5:7]])

    def test_meta(self):
        '''
        Test search meta is set.
        '''
        lazy = Article.objects.bsearch_title_search('title').only('pk').fields('_id')
        assert all([hasattr(item._searchmeta) for item in lazy.filter('range', effective_date={'lte': '2014-09-22'})[5:7]])

    def test_manangedbutempty(self):
        '''
        Tests that the indexing condition controls indexing properly.
        '''
        mbeo = ManangedButEmpty.objects.create(title='Some time', description='This should never be indexed.')
        idxi = len(ManangedButEmpty.objects.search)
        self.assertEquals(idxi, 0, 'ManagedButEmpty has {} indexed items instead of zero.'.format(idxi))
        mbeo.delete()

    def test_specify_index(self):
        self.assertEqual(Article.objects.count(), Article.objects.search_index('bungiesearch_demo').count(), 'Indexed items on bungiesearch_demo for Article does not match number in database.')
        self.assertEqual(Article.objects.count(), Article.objects.search_index('bungiesearch_demo_bis').count(), 'Indexed items on bungiesearch_demo_bis for Article does not match number in database.')
        self.assertEqual(Article.objects.count(), Article.objects.bsearch_bisindex().count(), 'Indexed items on bungiesearch_demo_bis for Article does not match number in database, using alias.')
        self.assertEqual(NoUpdatedField.objects.count(), NoUpdatedField.objects.search_index('bungiesearch_demo').count(), 'Indexed items on bungiesearch_demo for NoUpdatedField does not match number in database.')
        self.assertEqual(NoUpdatedField.objects.search_index('bungiesearch_demo_bis').count(), 0, 'Indexed items on bungiesearch_demo_bis for NoUpdatedField is zero.')

    def test_None_as_missing(self):
        missing = Article.objects.search_index('bungiesearch_demo').filter('missing', field='text_field')
        self.assertEqual(len(missing), 1, 'Filtering by missing text_field does not return exactly one item.')
        self.assertEqual(missing[0].text_field, None, 'The item with missing text_field does not have text_field=None.')

    def test_signal_setup_teardown(self):
        '''
        Tests that setup and tear down can be ran.
        '''
        btsp = BungieTestSignalProcessor()
        btsp.setup(Article)
        self.assertTrue(btsp.setup_ran, 'Calling setup on the signal processor did not set it up.')
        btsp.teardown(Article)
        self.assertTrue(btsp.teardown_ran, 'Calling teardown on the signal processor did not tear it down.')