from django.core.management import call_command
from django.core.management.base import BaseCommand

from .clear_index import Command as ClearCommand


class Command(BaseCommand):
    help = "Rebuilds the search index by clearing the search index and then performing an update."
    option_list = set(BaseCommand.option_list + ClearCommand.option_list)

    def handle(self, **options):
        call_command('clear_index', **options)
        call_command('search_index', action='update-mapping', **options)
        call_command('search_index', action='update', **options)
