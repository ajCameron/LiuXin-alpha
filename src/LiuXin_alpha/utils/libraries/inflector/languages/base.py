#!/usr/bin/env python

# Copyright (c) 2006 Bermi Ferrer Martinez
# bermi a-t bermilabs - com
# See the end of this file for the free software, open source license (BSD-style).

"""
Provide base utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise base through a consuming regression::

        python -m pytest -q tests/utils/language_tools/test_pluralizers.py
"""
import re
import unicodedata


class Base(object):
    """
    Locale inflectors must inherit from this base class inorder to provide the basic Inflector functionality.

    Example:
        Exercise Base through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py
    """

    def conditional_plural(self, number_of_records: int, word: str) -> str:
        """
        Returns the plural form of a word if first parameter is greater than 1

        Example:
            Exercise Base.conditional plural through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param number_of_records: Value supplied for number of records under the utility
            contract.
        :param word: Value supplied for word under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        if number_of_records > 1:
            return self.pluralize(word)
        else:
            return word

    def titleize(self, word: str, uppercase: str = "") -> str:
        """
        Converts an underscored or CamelCase word into a English sentence.

        Example:
            Exercise Base.titleize through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param word: Value supplied for word under the utility contract.
        :param uppercase: Value supplied for uppercase under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        if uppercase == "first":
            return self.humanize(self.underscore(word)).capitalize()
        else:
            return self.humanize(self.underscore(word)).title()

    def camelize(self, word: str) -> str:
        """
        Returns given word as CamelCased Converts a word like "send_email" to "SendEmail".

        Example:
            Exercise Base.camelize through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param word: Value supplied for word under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "".join(w[0].upper() + w[1:] for w in re.sub("[^A-Z^a-z^0-9^:]+", " ", word).split(" "))

    def underscore(self, word: str) -> str:
        """
        Converts a word "into_it_s_underscored_version"

        Example:
            Exercise Base.underscore through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param word: Value supplied for word under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        return re.sub(
            r"[^A-Z^a-z^0-9^\/]+",
            "_",
            re.sub(
                r"([a-z\d])([A-Z])",
                "\\1_\\2",
                re.sub("([A-Z]+)([A-Z][a-z])", "\\1_\\2", re.sub("::", "/", word)),
            ),
        ).lower()

    def humanize(self, word: str, uppercase: str = "") -> str:
        """
        Returns a human-readable string from word

        Example:
            Exercise Base.humanize through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param word: Value supplied for word under the utility contract.
        :param uppercase: Value supplied for uppercase under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        if uppercase == "first":
            return re.sub("_id$", "", word).replace("_", " ").capitalize()
        else:
            return re.sub("_id$", "", word).replace("_", " ").title()

    def variablize(self, word):
        """
        Same as camelize but first char is lowercased Converts a word like "send_email" to "sendEmail". It will remove non alphanumeric character from the word, so "who's online" will be converted to "whoSOnline"

        Example:
            Exercise Base.variablize through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param word: Value supplied for word under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        word = self.camelize(word)
        return word[0].lower() + word[1:]

    def tableize(self, class_name):
        """
        Converts a class name to its table name according to rails naming conventions. Example. Converts "Person" to "people"

        Example:
            Exercise Base.tableize through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param class_name: Value supplied for class name under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.pluralize(self.underscore(class_name))

    def classify(self, table_name):
        """
        Converts a table name to its class name according to rails naming conventions. Example: Converts "people" to "Person"

        Example:
            Exercise Base.classify through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param table_name: Value supplied for table name under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.camelize(self.singularize(table_name))

    def ordinalize(self, number):
        """
        Converts number to its ordinal English form. This method converts 13 to 13th, 2 to 2nd ...

        Example:
            Exercise Base.ordinalize through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param number: Value supplied for number under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        tail = "th"
        if number % 100 == 11 or number % 100 == 12 or number % 100 == 13:
            tail = "th"
        elif number % 10 == 1:
            tail = "st"
        elif number % 10 == 2:
            tail = "nd"
        elif number % 10 == 3:
            tail = "rd"

        return str(number) + tail

    def unaccent(self, text):
        """
        Transforms a string to its unaccented version. This might be useful for generating "friendly" URLs

        Example:
            Exercise Base.unaccent through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # find = u'\u00C0\u00C1\u00C2\u00C3\u00C4\u00C5\u00C6\u00C7\u00C8\u00C9\u00CA\u00CB\u00CC\u00CD\u00CE\u00CF\u00D0\u00D1\u00D2\u00D3\u00D4\u00D5\u00D6\u00D8\u00D9\u00DA\u00DB\u00DC\u00DD\u00DE\u00DF\u00E0\u00E1\u00E2\u00E3\u00E4\u00E5\u00E6\u00E7\u00E8\u00E9\u00EA\u00EB\u00EC\u00ED\u00EE\u00EF\u00F0\u00F1\u00F2\u00F3\u00F4\u00F5\u00F6\u00F8\u00F9\u00FA\u00FB\u00FC\u00FD\u00FE\u00FF'
        # replace = u'AAAAAAACEEEEIIIIDNOOOOOOUUUUYTsaaaaaaaceeeeiiiienoooooouuuuyty'
        # return self.string_replace(text, find, replace)
        s = unicodedata.normalize("NFD", text)

        return "".join((c for c in s if unicodedata.category(c) != "Mn"))

    def string_replace(self, word, find, replace):
        """
        This function returns a copy of word, translating all occurrences of each character in find to the corresponding character in replace

        Example:
            Exercise Base.string replace through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param word: Value supplied for word under the utility contract.
        :param find: Value supplied for find under the utility contract.
        :param replace: Value supplied for replace under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for k in range(0, len(find)):
            word = re.sub(find[k], replace[k], word)

        return word

    def urlize(self, text):
        """
        Transform a string its unaccented and underscored version ready to be inserted in friendly URLs

        Example:
            Exercise Base.urlize through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return re.sub("^_|_$", "", self.underscore(self.unaccent(text)))

    def demodulize(self, module_name):
        """
        Perform the demodulize utility operation under explicit compatibility rules.

        Example:
            Exercise Base.demodulize through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param module_name: Value supplied for module name under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.humanize(self.underscore(re.sub("^.*::", "", module_name)))

    def modulize(self, module_description):
        """
        Perform the modulize utility operation under explicit compatibility rules.

        Example:
            Exercise Base.modulize through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param module_description: Value supplied for module description under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.camelize(self.singularize(module_description))

    def foreign_key(self, class_name, separate_class_name_and_id_with_underscore=1):
        """
        Returns class_name in underscored form, with "_id" tacked on at the end. This is for use in dealing with the databases.

        Example:
            Exercise Base.foreign key through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param class_name: Value supplied for class name under the utility contract.
        :param separate_class_name_and_id_with_underscore: Value supplied for separate class
            name and id with underscore under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if separate_class_name_and_id_with_underscore:
            tail = "_id"
        else:
            tail = "id"
        return self.underscore(self.demodulize(class_name)) + tail


# Copyright (c) 2006 Bermi Ferrer Martinez
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software to deal in this software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of this software, and to permit
# persons to whom this software is furnished to do so, subject to the following
# condition:
#
# THIS SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THIS SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THIS SOFTWARE.
