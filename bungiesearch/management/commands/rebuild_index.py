from django.core.management import call_command
from django.core.management.base import BaseCommand

from ._utils import add_arguments


class Command(BaseCommand):
    help = "Rebuilds the search index by clearing the search index and then performing an update."
    add_arguments = add_arguments

    def handle(self, **options):
        call_command('clear_index', **options)
        call_command('search_index', action='update-mapping', **options)
        call_command('search_index', action='update', **options)
