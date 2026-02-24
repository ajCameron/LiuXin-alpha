

from __future__ import unicode_literals

import queue as Queue
import re

from collections import defaultdict, OrderedDict
from copy import deepcopy

import six
from six import string_types

from LiuXin_alpha.metadata.constants import CREATOR_TYPES, EXTERNAL_EBOOK_ID_SCHEMA, INTERNAL_EBOOK_ID_SCHEMA

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.hashes import generate_title_fingerprint

from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError

from LiuXin_alpha.metadata.standardization import standardize_creator_name, make_creator_phash, gen_title_author_phash
from LiuXin_alpha.metadata.standardization import standardize_genre
from LiuXin_alpha.metadata.standardization import standardize_language
from LiuXin_alpha.metadata.standardization import make_tag_search_term
from LiuXin_alpha.metadata.standardization import standardize_tag
from LiuXin_alpha.metadata.standardization import make_title_search_term
from LiuXin_alpha.metadata.standardization import standardize_title
from LiuXin_alpha.metadata.standardization import standardize_identifier
from LiuXin_alpha.metadata.standardization import standardize_publisher
from LiuXin_alpha.metadata.standardization import standardize_series
from LiuXin_alpha.metadata.standardization import make_series_phash

from LiuXin_alpha.metadata.utils import authors_to_sort_string
from LiuXin_alpha.metadata.utils import author_to_author_sort
from LiuXin_alpha.metadata.utils import title_sort as generate_title_sort
from LiuXin_alpha.metadata.utils import check_isbn
from LiuXin_alpha.metadata.utils import check_issn
from LiuXin_alpha.metadata.utils import check_doi
from LiuXin_alpha.metadata.standardize import standardize_id_name

from LiuXin_alpha.utils.date import isoformat_timestamp, utcnow
from LiuXin_alpha.utils.identifiers import get_unique_group_id
from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


from LiuXin_alpha.databases.api import RowAPI

# Todo: Add relevant tables should have flags. All tables should have phash.


class AgentCreatorOrgMixin:
    """
    Responsible for providing add methods for agents, creators and organizations.

    Why this combination of tables?
     - creators is a legacy term for human agents
     - publishers is a legacy term for organizational agents
     - agents (and associated sidcar tables) encompass both
    """

    def agent(self) -> RowAPI:
        """
        Create an agent row.

        :return:
        """

    def creator(
        self,
        creator,
        creator_sort=None,
        creator_short_name=None,
        creator_last_name=None,
        creator_phash=None,
        creator_legal_name=None,
        creator_birth_date=None,
        creator_death_date=None,
        creator_type="authors",
        creator_seminal_work=None,
        creator_one_person=True,
        creator_wikipedia=None,
        creator_imdb=None,
        creator_link=None,
        creator_created_datestamp=None,
        creator_datestamp=None,
        creator_language=None,
        creator_bio=None,
        creator_image=None,
    ):
        """
        Add to the creator table - no collision checking will be preformed.

        :param creator: The name of the creator used in all their works (J. R. R. Tolkien)
        :param creator_sort: Sort of the creators table - e.g. Tolkien, J. R. R.
        :param creator_short_name: A shortened form of the creators name (ideally unique) - e.g. Tolkien
        :param creator_last_name: The creators surname (Tolkien)
        :param creator_phash: Used to fuzzily match the creator
        :param creator_legal_name: The legal name of the creator (probably just the full name)
                                   (i.e. John Ronald Reuel Tolkien)
        :param creator_birth_date: When was the creator born?
        :param creator_death_date: When did the creator die?
        :param creator_type: What role does this creator serve by default? (authors, editors, e.t.c)
        :param creator_seminal_work: What work is the creator most famous for?
        :param creator_one_person:
        :param creator_wikipedia: A link to the wikipedia article on the creator
        :param creator_imdb: A link to the IMDB page for the creator
        :param creator_link: A link to the creators website.

        :param creator_language: What language did the creator work in? Creates an associate between the creator and a
                                 language in the languages table.
        :param creator_bio: A biography for the creator - will be added to the notes table and the id will be entered
                            here.
        :param creator_image: Not implemented at present
        :return new_creator_row: The row once all the information has been added to it and it's been synced to the
                                 database.
        """
        # Make the actual creator row
        creator_row = Row(database=self.db)

        # Set creator name information
        creator_row["creator"] = creator
        creator_row["creator_sort"] = author_to_author_sort(creator) if creator_sort is None else creator_sort
        creator_row["creator_short_name"] = creator if creator_short_name is None else creator_short_name
        creator_row["creator_last_name"] = creator.split(" ")[-1] if creator_last_name is None else creator_last_name
        creator_row["creator_phash"] = make_creator_phash(creator) if creator_phash is None else creator_phash
        creator_row["creator_legal_name"] = creator_legal_name if creator_legal_name is not None else creator

        # Creator dates
        creator_row["creator_birth_date"] = creator_birth_date
        creator_row["creator_death_date"] = creator_death_date

        # Creator works metadata
        creator_type = creator_type.lower().strip()
        if creator_type not in CREATOR_TYPES:
            err_str = "Unable to create_creator - creator type was not recognized."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("creator_type", creator_type),
                ("CREATOR_TYPES", CREATOR_TYPES),
            )
            raise InputIntegrityError(err_str)
        creator_row["creator_type"] = creator_type
        creator_row["creator_seminal_work"] = creator_seminal_work
        creator_row["creator_one_person"] = creator_one_person

        # Creator's online prescnece
        creator_row["creator_wikipedia"] = creator_wikipedia
        creator_row["creator_imdb"] = creator_imdb
        creator_row["creator_link"] = creator_link

        # Row creation dates
        creator_row["creator_created_datestamp"] = utcnow() if creator_created_datestamp is None else creator_datestamp
        creator_row["creator_datestamp"] = creator_datestamp

        creator_row.sync()

        # Set the assets associated with the creator
        if creator_image is not None:
            info_str = "Cannot set creator_image at present - not implemented"
            default_log.info(info_str)

        # Now that the row exists and has been added to the database associate the other rows with it
        if creator_language is None:
            pass
        elif creator_language is not None and isinstance(creator_language, Row):
            self.apply.language(language=creator_language, resource_row=creator_row)
        else:
            err_str = "Unable to parse creator_language - creator_language must be a row"
            err_str = default_log.log_variables(err_str, "ERROR", ("creator_language", creator_language))
            raise NotImplementedError(err_str)

        # Add the bio (if any)
        if creator_bio is None:
            pass
        elif creator_bio is not None and isinstance(creator_bio, Row):
            self.apply.note(note=creator_bio, resource=creator_row)
        elif creator_bio is not None and isinstance(creator_bio, string_types):
            self.apply.note(note=creator_bio, resource=creator_row)
        else:
            err_str = "Unable to parse creator_language - creator_language must be a row or a string"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("creator_language", creator_language),
                ("creator_lagnuage_type", type(creator_language)),
            )
            raise NotImplementedError(err_str)

        return creator_row
