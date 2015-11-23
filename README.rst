Bungiesearch
============

|Build Status| |Coverage Status|

.. contents:: Table of contents
   :depth: 2

Purpose
=======

Bungiesearch is a Django wrapper for
`elasticsearch-dsl-py <https://github.com/elasticsearch/elasticsearch-dsl-py>`__.
It inherits from elasticsearch-dsl-py's ``Search`` class, so all the
fabulous features developed by the elasticsearch-dsl-py team are also
available in Bungiesearch. In addition, just like ``Search``,
Bungiesearch is a lazy searching class (and iterable), meaning you can
call functions in a row, or do something like the following.

.. code:: python

    lazy = Article.objects.search.query('match', _all='Description')
    print len(lazy) # Prints the number of hits by only fetching the number of items.
    for item in lazy[5:10]:
        print item

Features
========

-  Core Python friendly

   -  Iteration (``[x for x in lazy_search]``)
   -  Get items (``lazy_search[10]``)
   -  Number of hits via ``len`` (``len(lazy_search)``)

-  Index management

   -  Creating and deleting an index.
   -  Creating, updating and deleting doctypes and their mappings.
   -  Update index doctypes.

-  Django Model Mapping

   -  Very easy mapping (no lies).
   -  Automatic model mapping (and supports undefined models by
      returning a ``Result`` instance of ``elasticsearch-dsl-py``).
   -  Efficient database fetching:

      -  One fetch for all items of a given model.
      -  Fetches only desired fields.

-  Django Manager

   -  Easy model integration:
      ``MyModel.search.query("match", _all="something to search")``.
   -  Search aliases (search shortcuts with as many parameters as
      wanted): ``Tweet.object.bungie_title_search("bungie")`` or
      ``Article.object.bungie_title_search("bungie")``, where
      ``bungie_title_search`` is uniquely defined.

-  Django signals

   -  Connect to post save and pre delete signals for the elasticsearch
      index to correctly reflect the database (almost) at all times.

-  Requirements

   -  Django >= 1.7
   -  Python 2.7 (**no Python 3 support yet**)

Feature examples
----------------

See section "Full example" at the bottom of page to see the code needed
to perform these following examples. ### Query a word (or list thereof)
on a managed model.

``Article.objects.search.query('match', _all='Description')``

Use a search alias on a model's manager.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Article.objects.bsearch_title_search('title')``

Use a search alias on a bungiesearch instance.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Article.objects.search.bsearch_title_search('title').bsearch_titlefilter('filter this title')``

Iterate over search results
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    # Will print the Django model instance.
    for result in Article.objects.search.query('match', _all='Description'):
        print result

Fetch a single item
~~~~~~~~~~~~~~~~~~~

.. code:: python

    Article.objects.search.query('match', _all='Description')[0]

Get the number of returned items
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    print len(Article.objects.search.query('match', _all='Description'))

Deferred model instantiation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    # Will print the Django model instance's primary key. Will only fetch the `pk` field from the database.
    for result in Article.objects.search.query('match', _all='Description').only('pk'):
        print result.pk

Elasticsearch limited field fetching
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    # Will print the Django model instance. However, elasticsearch's response only has the `_id` field.
    for result in Article.objects.search.query('match', _all='Description').fields('_id'):
        print result

Get a specific number of items with an offset.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is actually elasticseach-dsl-py functionality, but it's
demonstrated here because we can iterate over the results via
Bungiesearch.

.. code:: python

    for item in Article.objects.bsearch_title_search('title').only('pk').fields('_id')[5:7]:
        print item

Lazy objects
~~~~~~~~~~~~

.. code:: python

    lazy = Article.objects.bsearch_title_search('title')
    print len(lazy)
    for item in lazy.filter('range', effective_date={'lte': '2014-09-22'}):
        print item

Installation
============

Unless noted otherwise, each step is required.

Install the package
-------------------

The easiest way is to install the package from PyPi:

``pip install bungiesearch``

**Note:** Check your version of Django after installing bungiesearch. It
was reported to me directly that installing bungiesearch may upgrade
your version of Django, although I haven't been able to confirm that
myself. Bungiesearch depends on Django 1.7 and above.

In Django
---------

Updating your Django models
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Note:** this part is only needed if you want to be able to use search
aliases, which allow you to define shortcuts to complex queries,
available directly from your Django models. I think it's extremely
practical.

1. Open your ``models.py`` file.
2. Add the bungiesearch manager import:
   ``from bungiesearch.managers import BungiesearchManager``
3. Find the model, or models, you wish to index on Elasticsearch and set
   them to be managed by Bungiesearch by adding the objects field to
   them, as such: ``objects = BungiesearchManager()``. You should now
   have a Django model `similar to
   this <https://github.com/Sparrho/bungiesearch#django-model>`__.

Creating bungiesearch search indexes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The search indexes define how bungiesearch should serialize each of the
model's objects. It effectively defines how your object is serialized
and how the ES index should be structured. These are referred to as
`ModelIndex <https://github.com/Sparrho/bungiesearch#modelindex-1>`__\ es.

A good practice here is to have all the bungiesearch stuff in its own
package. For example, for the section of the Sparrho platform that uses
Django, we have a package called ``search`` where we define the search
indexes, and a subpackage called ``aliases`` which has the many aliases
we use (more on that latter).

1. Create a subclass of ``ModelIndex``, which you can import from from
   ``bungiesearch.indices import ModelIndex``, in a new module
   preferably.
2. In this class, define a class called ``Meta``: it will hold meta
   information of this search index for bungiesearch's internal working.
3. Import the Django model you want to index (from your models file)
   and, in the Meta class, define a field called ``model``, which must
   be set to the model you want indexed.
4. By default, bungiesearch will index every field of your model. This
   may not always be desired, so you can define which fields must be
   excluded in this ``Meta`` class, via the exclude field.
5. There are plenty of options, so definitely have a read through the
   documentation for
   `ModelIndex <https://github.com/Sparrho/bungiesearch#modelindex-1>`__.

Here's `an
example <https://github.com/Sparrho/bungiesearch#modelindex>`__ of a
search index. There can be many such definitions in a file.

Django settings
~~~~~~~~~~~~~~~

This is the final required step. Here's the `full
documentation <https://github.com/Sparrho/bungiesearch#settings>`__ of
this step.

1. Open your settings file and add a ``BUNGIESEARCH`` variable, which
   must be a dictionary.
2. Define ``URLS`` as a list of URLs (which can contain only one) of
   your ES servers.
3. Define the ``INDICES`` key as a dictionary where the key is the name
   of the index on ES that you want, and the value is the full Python
   path to the module which has all the ModelIndex classes for to be
   indexed on that index name.
4. Set ``ALIASES`` to an empty dictionary (until you define any search
   aliases).
5. You can keep other values as their defaults.

In your shell
-------------

Create the ES indexes
~~~~~~~~~~~~~~~~~~~~~

From your shell, in the Django environment, run the following:

``python manage.py search_index --create``

Start populating the index
--------------------------

Run the following which will take each of the objects in your model,
serialize them, and add them to the elasticsearch index.

``python manage.py search_index --update``

**Note:** With additional parameters, you can limit the number of
documents to be indexed, as well as set conditions on whether they
should be indexed based on updated time for example.

In Elasticsearch
----------------

You can now open your elasticsearch dashboard, such as Elastic HQ, and
see that your index is created with the appropriate mapping and has
items that are indexed.

Quick start example
===================

This example is from the ``test`` folder. It may be partially out-dated,
so please refer to the ``test`` folder for the latest version.

Procedure
---------

1. In your models.py file (or your managers.py), import bungiesearch and
   use it as a model manager.
2. Define one or more ModelIndex subclasses which define the mapping
   between your Django model and elasticsearch.
3. (Optional) Define SearchAlias subclasses which make it trivial to
   call complex elasticsearch-dsl-py functions.
4. Add a BUNGIESEARCH variable in your Django settings, which must
   contain the elasticsearch URL(s), the modules for the indices, the
   modules for the search aliases and the signal definitions.

Example
-------

Here's the code which is applicable to the previous examples. ### Django
Model

.. code:: python

    from django.db import models
    from bungiesearch.managers import BungiesearchManager

    class Article(models.Model):
        title = models.TextField(db_index=True)
        authors = models.TextField(blank=True)
        description = models.TextField(blank=True)
        link = models.URLField(max_length=510, unique=True, db_index=True)
        published = models.DateTimeField(null=True)
        created = models.DateTimeField(auto_now_add=True)
        updated = models.DateTimeField(null=True)
        tweet_count = models.IntegerField()
        raw = models.BinaryField(null=True)
        source_hash = models.BigIntegerField(null=True)
        missing_data = models.CharField(blank=True, max_length=255)
        positive_feedback = models.PositiveIntegerField(null=True, blank=True, default=0)
        negative_feedback = models.PositiveIntegerField(null=True, blank=True, default=0)
        popularity_index = models.IntegerField(default=0)

        objects = BungiesearchManager()

        class Meta:
            app_label = 'core'

ModelIndex
~~~~~~~~~~

The following ModelIndex will generate a mapping containing all fields
from ``Article``, minus those defined in ``ArticleIndex.Meta.exclude``.
When the mapping is generated, each field will the most appropriate
`elasticsearch core
type <http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping-core-types.html>`__,
with default attributes (as defined in bungiesearch.fields).

These default attributes can be overwritten with
``ArticleIndex.Meta.hotfixes``: each dictionary key must be field
defined either in the model or in the ModelIndex subclass
(``ArticleIndex`` in this case).

.. code:: python

    from core.models import Article
    from bungiesearch.fields import DateField, StringField
    from bungiesearch.indices import ModelIndex


    class ArticleIndex(ModelIndex):
        effectived_date = DateField(eval_as='obj.created if obj.created and obj.published > obj.created else obj.published')
        meta_data = StringField(eval_as='" ".join([fld for fld in [obj.link, str(obj.tweet_count), obj.raw] if fld])')

        class Meta:
            model = Article
            exclude = ('raw', 'missing_data', 'negative_feedback', 'positive_feedback', 'popularity_index', 'source_hash')
            hotfixes = {'updated': {'null_value': '2013-07-01'},
                        'title': {'boost': 1.75},
                        'description': {'boost': 1.35},
                        'full_text': {'boost': 1.125}}

SearchAlias
~~~~~~~~~~~

Defines a search alias for one or more models (in this case only for
``core.models.Article``).

.. code:: python

    from core.models import Article
    from bungiesearch.aliases import SearchAlias


    class SearchTitle(SearchAlias):
        def alias_for(self, title):
            return self.search_instance.query('match', title=title)

        class Meta:
            models = (Article,)
            alias_name = 'title_search' # This is optional. If none is provided, the name will be the class name in lower case.

    class InvalidAlias(SearchAlias):
        def alias_for_does_not_exist(self, title):
            return title

        class Meta:
            models = (Article,)

Django settings
~~~~~~~~~~~~~~~

.. code:: python

    BUNGIESEARCH = {
                    'URLS': [os.getenv('ELASTIC_SEARCH_URL')],
                    'INDICES': {'bungiesearch_demo': 'core.search_indices'},
                    'ALIASES': {'bsearch': 'myproject.search_aliases'},
                    'SIGNALS': {'BUFFER_SIZE': 1}  # uses BungieSignalProcessor
                    }

Documentation
=============

ModelIndex
----------

A ``ModelIndex`` defines mapping and object extraction for indexing of a
given Django model.

Any Django model to be managed by bungiesearch must have a defined
ModelIndex subclass. This subclass must contain a subclass called
``Meta`` which must have a ``model`` attribute (sets the model which it
represents).

Class attributes
~~~~~~~~~~~~~~~~

As detailed below, the doc type mapping will contain fields from the
model it related to. However, one may often need to index fields which
correspond to either a concatenation of fields of the model or some
logical operation.

Bungiesearch makes this very easy: simply define a class attribute as
whichever core type, and set to the ``eval_as`` constructor parameter to
a one line Python statement. The object is referenced as ``obj`` (not
``self`` nor ``object``, just ``obj``).

Example
^^^^^^^

This is a partial example as the Meta subclass is not defined, yet
mandatory (cf. below).

.. code:: python

    from bungiesearch.fields import DateField, StringField
    from bungiesearch.indices import ModelIndex

    class ArticleIndex(ModelIndex):
        effective_date = DateField(eval_as='obj.created if obj.created and obj.published > obj.created else obj.published')
        meta_data = StringField(eval_as='" ".join([fld for fld in [obj.link, str(obj.tweet_count), obj.raw] if fld])')

Here, both ``effective_date`` and ``meta_data`` will be part of the doc
type mapping, but won't be reversed mapped since those fields do not
exist in the model.

This can also be used to index foreign keys:

.. code:: python

    some_field_name = StringField(eval_as='",".join([item for item in obj.some_foreign_relation.values_list("some_field", flat=True)]) if obj.some_foreign_relation else ""')

Class methods
~~~~~~~~~~~~~

matches\_indexing\_condition
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Override this function to specify whether an item should be indexed or
not. This is useful when defining multiple indices (and ModelIndex
classes) for a given model. This method's signature and super class code
is as follows, and allows indexing of all items.

.. code:: python

    def matches_indexing_condition(self, item):
        return True

For example, if a given elasticsearch index should contain only item
whose title starts with ``"Awesome"``, then this method can be
overridden as follows.

.. code:: python

    def matches_indexing_condition(self, item):
        return item.title.startswith("Awesome")

Meta subclass attributes
~~~~~~~~~~~~~~~~~~~~~~~~

**Note**: in the following, any variable defined a being a ``list``
could also be a ``tuple``. ##### model *Required:* defines the Django
model for which this ModelIndex is applicable.

fields
^^^^^^

*Optional:* list of fields (or columns) which must be fetched when
serializing the object for elasticsearch, or when reverse mapping the
object from elasticsearch back to a Django Model instance. By default,
all fields will be fetched. Setting this *will* restrict which fields
can be fetched and may lead to errors when serializing the object. It is
recommended to use the ``exclude`` attribute instead (cf. below).

exclude
^^^^^^^

*Optional:* list of fields (or columns) which must not be fetched when
serializing or deserializing the object.

hotfixes
^^^^^^^^

*Optional:* a dictionary whose keys are index fields and whose values
are dictionaries which define `core type
attributes <http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping-core-types.html>`__.
By default, there aren't any special settings, apart for String fields,
where the
`analyzer <http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/analysis-analyzers.html>`__
is set to
```snowball`` <http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/analysis-snowball-analyzer.html>`__
(``{'analyzer': 'snowball'}``).

additional\_fields
^^^^^^^^^^^^^^^^^^

*Optional:* additional fields to fetch for mapping, may it be for
``eval_as`` fields or when returning the object from the database.

id\_field
^^^^^^^^^

*Optional:* the model field to use as a unique ID for elasticsearch's
metadata ``_id``. Defaults to ``id`` (also called
```pk`` <https://docs.djangoproject.com/en/dev/topics/db/models/#automatic-primary-key-fields>`__).

updated\_field
^^^^^^^^^^^^^^

*Optional:* set the model's field which can be filtered on dates in
order to find when objects have been updated. Note, this is *mandatory*
to use ``--start`` and/or ``--end`` when updating index (with
``search_index --update``).

optimize\_queries
^^^^^^^^^^^^^^^^^

*Optional:* set to True to make efficient queries when automatically
mapping to database objects. This will *always* restrict fetching to the
fields set in ``fields`` and in ``additional_fields``. *Note:* You can
also perform an optimal database query with ``.only('__model')``, which
will use the same fields as ``optimize_queries``, or
``.only('__fields')``, which will use the fields provided in the
``.fields()`` call.

indexing\_query
^^^^^^^^^^^^^^^

*Optional:* set to a QuerySet instance to specify the query used when
the search\_index command is ran to index. This **does not** affect how
each piece of content is indexed.

default
^^^^^^^

Enables support for a given model to be indexed on several elasticsearch
indices. Set to ``False`` on all but the default index. **Note**: if all
managed models are set with ``default=False`` then Bungiesearch will
fail to find and index that model.

Example
~~~~~~~

Indexes all objects of ``Article``, as long as their ``updated``
datetime is less than `21 October 2015
04:29 <https://en.wikipedia.org/wiki/Back_to_the_Future_Part_II>`__.

.. code:: python

    from core.models import Article
    from bungiesearch.indices import ModelIndex
    from datetime import datetime

    class ArticleIndex(ModelIndex):

        def matches_indexing_condition(self, item):
            return item.updated < datetime.datetime(2015, 10, 21, 4, 29)

        class Meta:
            model = Article
            id_field = 'id' # That's actually the default value, so it's not really needed.
            exclude = ('raw', 'missing_data', 'negative_feedback', 'positive_feedback', 'popularity_index', 'source_hash')
            hotfixes = {'updated': {'null_value': '2013-07-01'},
                        'title': {'boost': 1.75},
                        'description': {'boost': 1.35},
                        'full_text': {'boost': 1.125}}
            optimized_queries = True
            indexing_query = Article.objects.defer(*exclude).select_related().all().prefetch_related('tags')

SearchAlias
-----------

A ``SearchAlias`` define search shortcuts (somewhat similar to `Django
managers <https://docs.djangoproject.com/en/dev/topics/db/managers/>`__).
Often times, a given search will be used in multiple parts of the code.
SearchAliases allow you define those queries, filters, or any
bungiesearch/elasticsearch-dsl-py calls as an alias.

A search alias is either applicable to a ``list`` (or ``tuple``) of
managed models, or to any bungiesearch instance. It's very simple, so
here's an example which is detailed right below.

Example
~~~~~~~

The most simple implementation of a SearchAlias is as follows. This
search alias can be called via ``Article.objects.bungie_title`` (or
``Article.objects.search.bungie_title``), supposing that the namespace
is set to ``None`` in the settings (cf. below).

Definition
^^^^^^^^^^

.. code:: python

    from bungiesearch.aliases import SearchAlias

    class Title(SearchAlias):
        def alias_for(self, title):
            return self.search_instance.query('match', title=title)

Usage
^^^^^

.. code:: python

    Article.objects.bungie_title('title')

Method overwrite
~~~~~~~~~~~~~~~~

Any implementation needs to inherit from
``bungiesearch.aliases.SearchAlias`` and overwrite ``alias_for``. You
can set as many or as little parameters as you want for that function
(since bungiesearch only return the pointer to that function without
actually calling it).

Since each managed model has its own doc type, ``self.search_instance``
is a bungiesearch instance set to search the specific doctype.

Meta subclass attributes
~~~~~~~~~~~~~~~~~~~~~~~~

Although not mandatory, the ``Meta`` subclass enabled custom naming and
model restrictions for a search alias.

models
^^^^^^

*Optional:* ``list`` (or ``tuple``) of Django models which are allowed
to use this search alias. If a model which is not allowed to use this
SearchAlias tries it, a ``ValueError`` will be raised.

alias\_name
^^^^^^^^^^^

*Optional:* A string corresponding the suffix name of this search alias.
Defaults to the lower case class name.

**WARNING**: As explained in the "Settings" section below, all search
aliases in a given module share the prefix (or namespace). This is to
prevent aliases from accidently overwriting Django manager function
(e.g. ``update`` or ``get``). In other words, if you define the
``alias_name`` to ``test``, then it must be called as
``model_obj.objects.$prefix$_test`` where ``$prefix$`` is the prefix
defined in the settings. This prefix is also applicable to search
aliases which are available via bungiesearch instances directly. Hence,
one can define in one module search utilities (e.g. ``regex`` and
``range``) and define model specific aliases (e.g. ``title``) in another
module, and use both in conjunction as such:
``Article.objects.search.bungie_title('search title').utils_range(field='created', gte='2014-05-20', as_query=True)``.
These aliases can be concatenated ad vitam aeternam.

Sophisticated example
~~~~~~~~~~~~~~~~~~~~~

This example shows that we can have some fun with search aliases. In
this case, we define a Range alias which is applicable to any field on
any model.

.. code:: python

    class Range(SearchAlias):
        def alias_for(self, field, gte=None, lte=None, boost=None, as_query=False):
            body = {field: {}}
            if gte:
                body[field]['gte'] = gte
            if lte:
                body[field]['lte'] = lte
            if boost:
                if not as_query:
                    logging.warning('Boost is not applicable to search alias Range when not used as a query.')
                else:
                    body[field]['boost'] = boost
            if as_query:
                return self.search_instance.query({'range': body})
            return self.search_instance.filter({'range': body})

We can use it as such
``Article.objects.bungie_range(field='created', gte='2014-05-20', as_query=True)``.

Settings
--------
Add 'bungiesearch' to INSTALLED_APPS.

You must define ``BUNGIESEARCH`` in your Django settings in order for
bungiesearch to know elasticsearch URL(s) and which index name contains
mappings for each ModelIndex.

.. code:: python

    BUNGIESEARCH = {
                    'URLS': ['localhost'], # No leading http:// or the elasticsearch client will complain.
                    'INDICES': {'main_index': 'myproject.myapp.myindices'} # Must be a module path.
                    'ALIASES': {'bsearch': 'myproject.search_aliases'},
                    'SIGNALS': {'BUFFER_SIZE': 1},
                    'TIMEOUT': 5
                    }

URLS
~~~~

*Required:* must be a list of URLs which host elasticsearch instance(s).
This is directly sent to elasticsearch-dsl-py, so any issue with
multiple URLs should be refered to them.

INDICES
~~~~~~~

*Required:* must be a dictionary where each key is the name of an
elasticsearch index and each value is a path to a Python module
containing classes which inherit from
``bungiesearch.indices.ModelIndex`` (cf. below).

ALIASES
~~~~~~~

*Optional:* a dictionary whose key is the alias namespace and whose
value is the Python module containing classes which inherit from
``bungiesearch.aliases.SearchAlias``. If the namespace is ``None``, then
the alias will be named ``bungie``. If the namespace is an empty string,
there will be no alias namespace. The provided namespace will be
appended by an underscore. In the example above, each search alias
defined in ``myproject.search_aliases`` will be referenced as
``$ModelObj$.objects.bsearch_$alias$``, where ``$ModelObj$`` is a Django
model and ``$alias$`` is the name of the search alias.

The purpose is to not accidently overwrite Django's default manager
functions with search aliases.

SIGNALS
~~~~~~~

*Optional:* if it exists, it must be a dictionary (even empty), and will
connect to the ``post save`` and ``pre delete`` model functions of *all*
models using ``bungiesearch.managers.BungiesearchManager`` as a manager.
One may also define a signal processor class for more custom
functionality by placing the string value of the module path under a key
called ``SIGNAL_CLASS`` in the dictionary value of ``SIGNALS`` and
defining ``setup`` and ``teardown`` methods, which take ``model`` as the
only parameter. These methods connect and disconnect the signal
processing class to django signals (signals are connected to each model
which uses a BungiesearchManager).

If ``SIGNALS`` is not defined in the settings, *none* of the models
managed by BungiesearchManager will automatically update the index when
a new item is created or deleted.

BUFFER\_SIZE
^^^^^^^^^^^^

*Optional:* an integer representing the number of items to buffer before
making a bulk index update, defaults to ``100``.

**WARNING**: if your application is shut down before the buffer is
emptied, then any buffered instance *will not* be indexed on
elasticsearch. Hence, a possibly better implementation is wrapping
``post_save_connector`` and ``pre_delete_connector`` from
``bungiesearch.signals`` in a celery task. It is not implemented as such
here in order to not require ``celery``.

TIMEOUT
~~~~~~~

*Optional:* Elasticsearch connection timeout in seconds. Defaults to
``5``.

Testing
=======

The easiest way to run the tests is to install all dev dependencies using
``./setup.sh`` then run ``./test.sh``

All Bungiesearch tests are in ``tests/core/test_bungiesearch.py``. You
can run the tests by creating a Python virtual environment, installing
the requirements from ``tests/requirements.txt``, installing the package
(``pip install .``) and running ``python tests/manage.py test``. Make
sure to update ``tests/settings.py`` to use your own elasticsearch URLs,
or update the ELASTIC\_SEARCH\_URL environment variable.

.. |Build Status| image:: https://travis-ci.org/Sparrho/bungiesearch.svg?branch=master
   :target: https://travis-ci.org/Sparrho/bungiesearch
.. |Coverage Status| image:: https://coveralls.io/repos/Sparrho/bungiesearch/badge.svg?branch=master&service=github
   :target: https://coveralls.io/github/Sparrho/bungiesearch?branch=master
