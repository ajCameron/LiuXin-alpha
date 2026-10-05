#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Register and implement Calibre-compatible built-in template functions.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise formatter functions through a consuming regression::

        python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
"""
from __future__ import print_function

"""
Created on 13 Jan 2011
"""

__license__ = "GPL v3"
__copyright__ = "2010, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

import inspect
import re
import traceback
from math import trunc

from LiuXin_alpha.constants import VERBOSE_DEBUG as DEBUG

from LiuXin_alpha.metadata.utils import title_sort

# Todo: we have two human_readable functions...
from LiuXin_alpha.utils.text import human_readable
from LiuXin_alpha.preferences import preferences as tweaks
from LiuXin_alpha.utils.date import parse_date, format_date, now, UNDEFINED_DATE
from LiuXin_alpha.utils.text.icu import capitalize, strcmp, sort_key, lower as icu_lower
from LiuXin_alpha.utils.localization import _, calibre_langcode_to_name, canonicalize_lang
from LiuXin_alpha.utils.libraries.titlecase import titlecase

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


class FormatterFunctions(object):

    """
    Provide the FormatterFunctions utility contract with explicit state and cleanup behavior.

    Example:
        Exercise FormatterFunctions through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    error_function_body = (
        "def evaluate(self, formatter, kwargs, mi, locals):\n"
        '\treturn "'
        + _("Duplicate user function name {0}. " "Change the name or ensure that the functions are identical")
        + '"'
    )

    def __init__(self):
        """
        Initialize and validate the FormatterFunctions state.

        Example:
            Exercise FormatterFunctions.  init   through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: None; validated state is stored on the receiving object.
        """
        self._builtins = {}
        self._functions = {}
        self._functions_from_library = {}

    def register_builtin(self, func_class):
        """
        Perform the register builtin utility operation under explicit compatibility rules.

        Example:
            Exercise FormatterFunctions.register builtin through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param func_class: Value supplied for func class under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not isinstance(func_class, FormatterFunction):
            raise ValueError("Class %s is not an instance of FormatterFunction" % (func_class.__class__.__name__))
        name = func_class.name
        if name in self._functions:
            raise ValueError("Name %s already used" % name)
        self._builtins[name] = func_class
        self._functions[name] = func_class
        for a in func_class.aliases:
            self._functions[a] = func_class

    def _register_function(self, func_class, replace=False):
        """
        Perform the register function utility operation under explicit compatibility rules.

        Example:
            Exercise FormatterFunctions. register function through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param func_class: Value supplied for func class under the utility contract.
        :param replace: Value supplied for replace under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not isinstance(func_class, FormatterFunction):
            raise ValueError("Class %s is not an instance of FormatterFunction" % (func_class.__class__.__name__))
        name = func_class.name
        if not replace and name in self._functions:
            raise ValueError("Name %s already used" % name)
        self._functions[name] = func_class

    def register_functions(self, library_uuid, funcs):
        """
        Perform the register functions utility operation under explicit compatibility rules.

        Example:
            Exercise FormatterFunctions.register functions through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param library_uuid: Value supplied for library uuid under the utility contract.
        :param funcs: Value supplied for funcs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._functions_from_library[library_uuid] = funcs
        self._register_functions()

    def _register_functions(self):
        """
        Perform the register functions utility operation under explicit compatibility rules.

        Example:
            Exercise FormatterFunctions. register functions through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for compiled_funcs in self._functions_from_library.itervalues():
            for cls in compiled_funcs:
                f = self._functions.get(cls.name, None)
                replace = False
                if f is not None:
                    existing_body = f.program_text
                    new_body = cls.program_text
                    if new_body != existing_body:
                        # Change the body of the template function to one that will
                        # return an error message. Also change the arg count to
                        # -1 (variable) to avoid template compilation errors
                        replace = True
                        func = [
                            cls.name,
                            "",
                            -1,
                            self.error_function_body.format(cls.name),
                        ]
                        cls = compile_user_function(*func)
                    else:
                        continue
                formatter_functions()._register_function(cls, replace=replace)

    def unregister_functions(self, library_uuid):
        """
        Perform the unregister functions utility operation under explicit compatibility rules.

        Example:
            Exercise FormatterFunctions.unregister functions through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param library_uuid: Value supplied for library uuid under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if library_uuid in self._functions_from_library:
            for cls in self._functions_from_library[library_uuid]:
                self._functions.pop(cls.name, None)
            self._functions_from_library.pop(library_uuid)
            self._register_functions()

    def get_builtins(self):
        """
        Return builtins under the documented compatibility and safety rules.

        Example:
            Exercise FormatterFunctions.get builtins through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._builtins

    def get_builtins_and_aliases(self):
        """
        Return builtins and aliases under the documented compatibility and safety rules.

        Example:
            Exercise FormatterFunctions.get builtins and aliases through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        res = {}
        for f in self._builtins.itervalues():
            res[f.name] = f
            for a in f.aliases:
                res[a] = f
        return res

    def get_functions(self):
        """
        Return functions under the documented compatibility and safety rules.

        Example:
            Exercise FormatterFunctions.get functions through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._functions

    def reset_to_builtins(self):
        """
        Perform the reset to builtins utility operation under explicit compatibility rules.

        Example:
            Exercise FormatterFunctions.reset to builtins through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._functions = {}
        for n, c in self._builtins.items():
            self._functions[n] = c
            for a in c.aliases:
                self._functions[a] = c


_ff = FormatterFunctions()


def formatter_functions():
    """
    Perform the formatter functions utility operation under explicit compatibility rules.

    Example:
        Exercise formatter functions through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    global _ff
    return _ff


class FormatterFunction(object):

    """
    Provide the FormatterFunction utility contract with explicit state and cleanup behavior.

    Example:
        Exercise FormatterFunction through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    doc = _("No documentation provided")
    name = "no name provided"
    category = "Unknown"
    arg_count = 0
    aliases = []

    def evaluate(self, formatter, kwargs, mi, locals, *args):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise FormatterFunction.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError()

    def eval_(self, formatter, kwargs, mi, locals, *args):
        """
        Perform the eval utility operation under explicit compatibility rules.

        Example:
            Exercise FormatterFunction.eval  through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ret = self.evaluate(formatter, kwargs, mi, locals, *args)
        if isinstance(ret, (str, unicode)):
            return ret
        if isinstance(ret, list):
            return ",".join(list)
        if isinstance(ret, (int, float, bool)):
            return six_unicode(ret)


class BuiltinFormatterFunction(FormatterFunction):
    """
    Implement the Calibre template ``FormatterFunction`` function.

    Example:
        Exercise BuiltinFormatterFunction through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    def __init__(self):
        """
        Initialize and validate the BuiltinFormatterFunction state.

        Example:
            Exercise BuiltinFormatterFunction.  init   through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :return: None; validated state is stored on the receiving object.
        """
        formatter_functions().register_builtin(self)
        eval_func = inspect.getmembers(self.__class__, lambda x: inspect.ismethod(x) and x.__name__ == "evaluate")
        try:
            lines = [l[4:] for l in inspect.getsourcelines(eval_func[0][1])[0]]
        except:
            lines = []
        self.program_text = "".join(lines)


class BuiltinStrcmp(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Strcmp`` function.

    Example:
        Exercise BuiltinStrcmp through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "strcmp"
    arg_count = 5
    category = "Relational"
    __doc__ = doc = _(
        "strcmp(x, y, lt, eq, gt) -- does a case-insensitive comparison of x "
        "and y as strings. Returns lt if x < y. Returns eq if x == y. "
        "Otherwise returns gt."
    )

    def evaluate(self, formatter, kwargs, mi, locals, x, y, lt, eq, gt):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinStrcmp.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :param lt: Value supplied for lt under the utility contract.
        :param eq: Value supplied for eq under the utility contract.
        :param gt: Value supplied for gt under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        v = strcmp(x, y)
        if v < 0:
            return lt
        if v == 0:
            return eq
        return gt


class BuiltinCmp(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Cmp`` function.

    Example:
        Exercise BuiltinCmp through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "cmp"
    category = "Relational"
    arg_count = 5
    __doc__ = doc = _(
        "cmp(x, y, lt, eq, gt) -- compares x and y after converting both to "
        "numbers. Returns lt if x < y. Returns eq if x == y. Otherwise returns gt."
    )

    def evaluate(self, formatter, kwargs, mi, locals, x, y, lt, eq, gt):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinCmp.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :param lt: Value supplied for lt under the utility contract.
        :param eq: Value supplied for eq under the utility contract.
        :param gt: Value supplied for gt under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        x = float(x if x and x != "None" else 0)
        y = float(y if y and y != "None" else 0)
        if x < y:
            return lt
        if x == y:
            return eq
        return gt


class BuiltinFirstMatchingCmp(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``FirstMatchingCmp`` function.

    Example:
        Exercise BuiltinFirstMatchingCmp through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "first_matching_cmp"
    category = "Relational"
    arg_count = -1
    __doc__ = doc = _(
        "first_matching_cmp(val, cmp1, result1, cmp2, r2, ..., else_result) -- "
        'compares "val < cmpN" in sequence, returning resultN for '
        "the first comparison that succeeds. Returns else_result "
        "if no comparison succeeds. Example: "
        'first_matching_cmp(10,5,"small",10,"middle",15,"large","giant") '
        'returns "large". The same example with a first value of 16 returns "giant".'
    )

    def evaluate(self, formatter, kwargs, mi, locals, *args):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinFirstMatchingCmp.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if (len(args) % 2) != 0:
            raise ValueError(_("first_matching_cmp requires an even number of arguments"))
        val = float(args[0] if args[0] and args[0] != "None" else 0)
        for i in range(1, len(args) - 1, 2):
            c = float(args[i] if args[i] and args[i] != "None" else 0)
            if val < c:
                return args[i + 1]
        return args[len(args) - 1]


class BuiltinStrcat(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Strcat`` function.

    Example:
        Exercise BuiltinStrcat through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "strcat"
    arg_count = -1
    category = "String manipulation"
    __doc__ = doc = _(
        "strcat(a, b, ...) -- can take any number of arguments. Returns a "
        "string formed by concatenating all the arguments"
    )

    def evaluate(self, formatter, kwargs, mi, locals, *args):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinStrcat.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        i = 0
        res = ""
        for i in range(0, len(args)):
            res += args[i]
        return res


class BuiltinStrlen(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Strlen`` function.

    Example:
        Exercise BuiltinStrlen through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "strlen"
    arg_count = 1
    category = "String manipulation"
    __doc__ = doc = _("strlen(a) -- Returns the length of the string passed as " "the argument")

    def evaluate(self, formatter, kwargs, mi, locals, a):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinStrlen.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param a: Value supplied for a under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return len(a)
        except:
            return -1


class BuiltinAdd(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Add`` function.

    Example:
        Exercise BuiltinAdd through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "add"
    arg_count = 2
    category = "Arithmetic"
    __doc__ = doc = _("add(x, y) -- returns x + y. Throws an exception if either x or y are not numbers.")

    def evaluate(self, formatter, kwargs, mi, locals, x, y):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinAdd.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        x = float(x if x and x != "None" else 0)
        y = float(y if y and y != "None" else 0)
        return six_unicode(x + y)


class BuiltinSubtract(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Subtract`` function.

    Example:
        Exercise BuiltinSubtract through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "subtract"
    arg_count = 2
    category = "Arithmetic"
    __doc__ = doc = _("subtract(x, y) -- returns x - y. Throws an exception if either x or y are not numbers.")

    def evaluate(self, formatter, kwargs, mi, locals, x, y):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinSubtract.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        x = float(x if x and x != "None" else 0)
        y = float(y if y and y != "None" else 0)
        return six_unicode(x - y)


class BuiltinMultiply(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Multiply`` function.

    Example:
        Exercise BuiltinMultiply through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "multiply"
    arg_count = 2
    category = "Arithmetic"
    __doc__ = doc = _("multiply(x, y) -- returns x * y. Throws an exception if either x or y are not numbers.")

    def evaluate(self, formatter, kwargs, mi, locals, x, y):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinMultiply.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        x = float(x if x and x != "None" else 0)
        y = float(y if y and y != "None" else 0)
        return six_unicode(x * y)


class BuiltinDivide(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Divide`` function.

    Example:
        Exercise BuiltinDivide through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "divide"
    arg_count = 2
    category = "Arithmetic"
    __doc__ = doc = _("divide(x, y) -- returns x / y. Throws an exception if either x or y are not numbers.")

    def evaluate(self, formatter, kwargs, mi, locals, x, y):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinDivide.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        x = float(x if x and x != "None" else 0)
        y = float(y if y and y != "None" else 0)
        return six_unicode(x / y)


class BuiltinTemplate(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Template`` function.

    Example:
        Exercise BuiltinTemplate through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "template"
    arg_count = 1
    category = "Recursion"

    __doc__ = doc = _(
        "template(x) -- evaluates x as a template. The evaluation is done "
        "in its own context, meaning that variables are not shared between "
        "the caller and the template evaluation. Because the { and } "
        "characters are special, you must use [[ for the { character and "
        "]] for the } character; they are converted automatically. "
        "For example, template('[[title_sort]]') will evaluate the "
        "template {title_sort} and return its value. Note also that "
        "prefixes and suffixes (the `|prefix|suffix` syntax) cannot be "
        "used in the argument to this function when using template program mode."
    )

    def evaluate(self, formatter, kwargs, mi, locals, template):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinTemplate.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param template: Template expression parsed or evaluated.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        template = template.replace("[[", "{").replace("]]", "}")
        return formatter.__class__().safe_format(template, kwargs, "TEMPLATE", mi)


class BuiltinEval(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Eval`` function.

    Example:
        Exercise BuiltinEval through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "eval"
    arg_count = 1
    category = "Recursion"
    __doc__ = doc = _(
        "eval(template) -- evaluates the template, passing the local "
        "variables (those 'assign'ed to) instead of the book metadata. "
        " This permits using the template processor to construct complex "
        "results from local variables. Because the { and } "
        "characters are special, you must use [[ for the { character and "
        "]] for the } character; they are converted automatically. "
        "Note also that prefixes and suffixes (the `|prefix|suffix` syntax) "
        "cannot be used in the argument to this function when using "
        "template program mode."
    )

    def evaluate(self, formatter, kwargs, mi, locals, template):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinEval.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param template: Template expression parsed or evaluated.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from formatter import EvalFormatter

        template = template.replace("[[", "{").replace("]]", "}")
        return EvalFormatter().safe_format(template, locals, "EVAL", None)


class BuiltinAssign(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Assign`` function.

    Example:
        Exercise BuiltinAssign through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "assign"
    arg_count = 2
    category = "Other"
    __doc__ = doc = _(
        "assign(id, val) -- assigns val to id, then returns val. " "id must be an identifier, not an expression"
    )

    def evaluate(self, formatter, kwargs, mi, locals, target, value):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinAssign.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param target: Value supplied for target under the utility contract.
        :param value: Value normalized, stored, formatted or returned.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        locals[target] = value
        return value


class BuiltinPrint(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Print`` function.

    Example:
        Exercise BuiltinPrint through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "print"
    arg_count = -1
    category = "Other"
    __doc__ = doc = _(
        "print(a, b, ...) -- prints the arguments to standard output. "
        "Unless you start calibre from the command line (calibre-debug -g), "
        "the output will go to a black hole."
    )

    def evaluate(self, formatter, kwargs, mi, locals, *args):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinPrint.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        print(args)
        return None


class BuiltinField(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Field`` function.

    Example:
        Exercise BuiltinField through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "field"
    arg_count = 1
    category = "Get values from metadata"
    __doc__ = doc = _("field(name) -- returns the metadata field named by name")

    def evaluate(self, formatter, kwargs, mi, locals, name):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinField.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return formatter.get_value(name, [], kwargs)


class BuiltinRawField(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``RawField`` function.

    Example:
        Exercise BuiltinRawField through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "raw_field"
    arg_count = 1
    category = "Get values from metadata"
    __doc__ = doc = _("raw_field(name) -- returns the metadata field named by name " "without applying any formatting.")

    def evaluate(self, formatter, kwargs, mi, locals, name):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinRawField.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return six_unicode(getattr(mi, name, None))


class BuiltinRawList(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``RawList`` function.

    Example:
        Exercise BuiltinRawList through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "raw_list"
    arg_count = 2
    category = "Get values from metadata"
    __doc__ = doc = _(
        "raw_list(name, separator) -- returns the metadata list "
        "named by name without applying any formatting or sorting and "
        "with items separated by separator."
    )

    def evaluate(self, formatter, kwargs, mi, locals, name, separator):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinRawList.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param name: Field, file, function or resource name addressed by the operation.
        :param separator: Delimiter used to split or join list values.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        res = getattr(mi, name, None)
        if not isinstance(res, list):
            return "%s is not a list" % name
        return separator.join(res)


class BuiltinSubstr(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Substr`` function.

    Example:
        Exercise BuiltinSubstr through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "substr"
    arg_count = 3
    category = "String manipulation"
    __doc__ = doc = _(
        "substr(str, start, end) -- returns the start'th through the end'th "
        "characters of str. The first character in str is the zero'th "
        "character. If end is negative, then it indicates that many "
        "characters counting from the right. If end is zero, then it "
        "indicates the last character. For example, substr('12345', 1, 0) "
        "returns '2345', and substr('12345', 1, -1) returns '234'."
    )

    def evaluate(self, formatter, kwargs, mi, locals, str_, start_, end_):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinSubstr.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param str_: Value supplied for str under the utility contract.
        :param start_: Value supplied for start under the utility contract.
        :param end_: Value supplied for end under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return str_[int(start_) : len(str_) if int(end_) == 0 else int(end_)]


class BuiltinLookup(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Lookup`` function.

    Example:
        Exercise BuiltinLookup through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "lookup"
    arg_count = -1
    category = "Iterating over values"
    __doc__ = doc = _(
        "lookup(val, pattern, field, pattern, field, ..., else_field) -- "
        "like switch, except the arguments are field (metadata) names, not "
        "text. The value of the appropriate field will be fetched and used. "
        "Note that because composite columns are fields, you can use this "
        "function in one composite field to use the value of some other "
        "composite field. This is extremely useful when constructing "
        "variable save paths"
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, *args):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinLookup.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if len(args) == 2:  # here for backwards compatibility
            if val:
                return formatter.vformat("{" + args[0].strip() + "}", [], kwargs)
            else:
                return formatter.vformat("{" + args[1].strip() + "}", [], kwargs)
        if (len(args) % 2) != 1:
            raise ValueError(_("lookup requires either 2 or an odd number of arguments"))
        i = 0
        while i < len(args):
            if i + 1 >= len(args):
                return formatter.vformat("{" + args[i].strip() + "}", [], kwargs)
            if re.search(args[i], val, flags=re.I):
                return formatter.vformat("{" + args[i + 1].strip() + "}", [], kwargs)
            i += 2


class BuiltinTest(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Test`` function.

    Example:
        Exercise BuiltinTest through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "test"
    arg_count = 3
    category = "If-then-else"
    __doc__ = doc = _(
        "test(val, text if not empty, text if empty) -- return `text if not "
        "empty` if the field is not empty, otherwise return `text if empty`"
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, value_if_set, value_not_set):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinTest.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param value_if_set: Value supplied for value if set under the utility contract.
        :param value_not_set: Value supplied for value not set under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if val:
            return value_if_set
        else:
            return value_not_set


class BuiltinContains(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Contains`` function.

    Example:
        Exercise BuiltinContains through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "contains"
    arg_count = 4
    category = "If-then-else"
    __doc__ = doc = _(
        "contains(val, pattern, text if match, text if not match) -- checks "
        "if field contains matches for the regular expression `pattern`. "
        "Returns `text if match` if matches are found, otherwise it returns "
        "`text if no match`"
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, test, value_if_present, value_if_not):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinContains.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param test: Value supplied for test under the utility contract.
        :param value_if_present: Value supplied for value if present under the utility
            contract.
        :param value_if_not: Value supplied for value if not under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if re.search(test, val, flags=re.I):
            return value_if_present
        else:
            return value_if_not


class BuiltinSwitch(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Switch`` function.

    Example:
        Exercise BuiltinSwitch through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "switch"
    arg_count = -1
    category = "Iterating over values"
    __doc__ = doc = _(
        "switch(val, pattern, value, pattern, value, ..., else_value) -- "
        "for each `pattern, value` pair, checks if the field matches "
        "the regular expression `pattern` and if so, returns that "
        "`value`. If no pattern matches, then else_value is returned. "
        "You can have as many `pattern, value` pairs as you want"
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, *args):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinSwitch.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if (len(args) % 2) != 1:
            raise ValueError(_("switch requires an odd number of arguments"))
        i = 0
        while i < len(args):
            if i + 1 >= len(args):
                return args[i]
            if re.search(args[i], val, flags=re.I):
                return args[i + 1]
            i += 2


class BuiltinStrcatMax(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``StrcatMax`` function.

    Example:
        Exercise BuiltinStrcatMax through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "strcat_max"
    arg_count = -1
    category = "String manipulation"
    __doc__ = doc = _(
        "strcat_max(max, string1, prefix2, string2, ...) -- "
        "Returns a string formed by concatenating the arguments. The "
        "returned value is initialized to string1. `Prefix, string` "
        "pairs are added to the end of the value as long as the "
        "resulting string length is less than `max`. String1 is returned "
        "even if string1 is longer than max. You can pass as many "
        "`prefix, string` pairs as you wish."
    )

    def evaluate(self, formatter, kwargs, mi, locals, *args):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinStrcatMax.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if len(args) < 2:
            raise ValueError(_("strcat_max requires 2 or more arguments"))
        if (len(args) % 2) != 0:
            raise ValueError(_("strcat_max requires an even number of arguments"))
        try:
            max = int(args[0])
        except:
            raise ValueError(_("first argument to strcat_max must be an integer"))

        i = 2
        result = args[1]
        try:
            while i < len(args):
                if (len(result) + len(args[i]) + len(args[i + 1])) > max:
                    break
                result = result + args[i] + args[i + 1]
                i += 2
        except:
            pass
        return result.strip()


class BuiltinInList(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``InList`` function.

    Example:
        Exercise BuiltinInList through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "in_list"
    arg_count = 5
    category = "List lookup"
    __doc__ = doc = _(
        "in_list(val, separator, pattern, found_val, not_found_val) -- "
        "treat val as a list of items separated by separator, "
        "comparing the pattern against each value in the list. If the "
        "pattern matches a value, return found_val, otherwise return "
        "not_found_val."
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, sep, pat, fv, nfv):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinInList.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param sep: Delimiter used to split or join list values.
        :param pat: Value supplied for pat under the utility contract.
        :param fv: Value supplied for fv under the utility contract.
        :param nfv: Value supplied for nfv under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        l = [v.strip() for v in val.split(sep) if v.strip()]
        if l:
            for v in l:
                if re.search(pat, v, flags=re.I):
                    return fv
        return nfv


class BuiltinStrInList(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``StrInList`` function.

    Example:
        Exercise BuiltinStrInList through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "str_in_list"
    arg_count = 5
    category = "List lookup"
    __doc__ = doc = _(
        "str_in_list(val, separator, string, found_val, not_found_val) -- "
        "treat val as a list of items separated by separator, "
        "comparing the string against each value in the list. If the "
        "string matches a value, return found_val, otherwise return "
        "not_found_val. If the string contains separators, then it is "
        "also treated as a list and each value is checked."
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, sep, str, fv, nfv):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinStrInList.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param sep: Delimiter used to split or join list values.
        :param str: Value supplied for str under the utility contract.
        :param fv: Value supplied for fv under the utility contract.
        :param nfv: Value supplied for nfv under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        l = [v.strip() for v in val.split(sep) if v.strip()]
        c = [v.strip() for v in str.split(sep) if v.strip()]
        if l:
            for v in l:
                for t in c:
                    if strcmp(t, v) == 0:
                        return fv
        return nfv


class BuiltinIdentifierInList(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``IdentifierInList`` function.

    Example:
        Exercise BuiltinIdentifierInList through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "identifier_in_list"
    arg_count = 4
    category = "List lookup"
    __doc__ = doc = _(
        "identifier_in_list(val, id, found_val, not_found_val) -- "
        "treat val as a list of identifiers separated by commas, "
        "comparing the string against each value in the list. An identifier "
        'has the format "identifier:value". The id parameter should be '
        'either "id" or "id:regexp". The first case matches if there is any '
        "identifier with that id. The second case matches if the regexp "
        "matches the identifier's value. If there is a match, "
        "return found_val, otherwise return not_found_val."
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, ident, fv, nfv):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinIdentifierInList.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param ident: Value supplied for ident under the utility contract.
        :param fv: Value supplied for fv under the utility contract.
        :param nfv: Value supplied for nfv under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        l = [v.strip() for v in val.split(",") if v.strip()]
        (id, _, regexp) = ident.partition(":")
        if not id:
            return nfv
        id += ":"
        if l:
            for v in l:
                if v.startswith(id):
                    if not regexp or re.search(regexp, v[len(id) :], flags=re.I):
                        return fv
        return nfv


class BuiltinRe(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Re`` function.

    Example:
        Exercise BuiltinRe through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "re"
    arg_count = 3
    category = "String manipulation"
    __doc__ = doc = _(
        "re(val, pattern, replacement) -- return the field after applying "
        "the regular expression. All instances of `pattern` are replaced "
        "with `replacement`. As in all of calibre, these are "
        "python-compatible regular expressions"
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, pattern, replacement):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinRe.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param pattern: Value supplied for pattern under the utility contract.
        :param replacement: Value supplied for replacement under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return re.sub(pattern, replacement, val, flags=re.I)


class BuiltinReGroup(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``ReGroup`` function.

    Example:
        Exercise BuiltinReGroup through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "re_group"
    arg_count = -1
    category = "String manipulation"
    __doc__ = doc = _(
        "re_group(val, pattern, template_for_group_1, for_group_2, ...) -- "
        "return a string made by applying the reqular expression pattern "
        "to the val and replacing each matched instance with the string "
        "computed by replacing each matched group by the value returned "
        "by the corresponding template. The original matched value for the "
        "group is available as $. In template program mode, like for "
        "the template and the eval functions, you use [[ for { and ]] for }."
        " The following example in template program mode looks for series "
        "with more than one word and uppercases the first word: "
        '{series:\'re_group($, "(\\S* )(.*)", "[[$:uppercase()]]", "[[$]]")\'}'
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, pattern, *args):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinReGroup.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param pattern: Value supplied for pattern under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from formatter import EvalFormatter

        def repl(mo):
            """
            Perform the repl utility operation under explicit compatibility rules.

            Example:
                Exercise BuiltinReGroup.evaluate.repl through a consuming regression::

                    python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


            :param mo: Value supplied for mo under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            res = ""
            if mo and mo.lastindex:
                for dex in range(0, mo.lastindex):
                    gv = mo.group(dex + 1)
                    if gv is None:
                        continue
                    if len(args) > dex:
                        template = args[dex].replace("[[", "{").replace("]]", "}")
                        res += EvalFormatter().safe_format(template, {"$": gv}, "EVAL", None, strip_results=False)
                    else:
                        res += gv
            return res

        return re.sub(pattern, repl, val, flags=re.I)


class BuiltinSwapAroundComma(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``SwapAroundComma`` function.

    Example:
        Exercise BuiltinSwapAroundComma through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "swap_around_comma"
    arg_count = 1
    category = "String manipulation"
    __doc__ = doc = _(
        "swap_around_comma(val) -- given a value of the form "
        '"B, A", return "A B". This is most useful for converting names '
        "in LN, FN format to FN LN. If there is no comma, the function "
        "returns val unchanged"
    )

    def evaluate(self, formatter, kwargs, mi, locals, val):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinSwapAroundComma.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return re.sub(r"^(.*?),\s*(.*$)", r"\2 \1", val, flags=re.I).strip()


class BuiltinIfempty(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Ifempty`` function.

    Example:
        Exercise BuiltinIfempty through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "ifempty"
    arg_count = 2
    category = "If-then-else"
    __doc__ = doc = _(
        "ifempty(val, text if empty) -- return val if val is not empty, " "otherwise return `text if empty`"
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, value_if_empty):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinIfempty.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param value_if_empty: Value supplied for value if empty under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if val:
            return val
        else:
            return value_if_empty


class BuiltinShorten(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Shorten`` function.

    Example:
        Exercise BuiltinShorten through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "shorten"
    arg_count = 4
    category = "String manipulation"
    __doc__ = doc = _(
        "shorten(val, left chars, middle text, right chars) -- Return a "
        "shortened version of the field, consisting of `left chars` "
        "characters from the beginning of the field, followed by "
        "`middle text`, followed by `right chars` characters from "
        "the end of the string. `Left chars` and `right chars` must be "
        "integers. For example, assume the title of the book is "
        "`Ancient English Laws in the Times of Ivanhoe`, and you want "
        "it to fit in a space of at most 15 characters. If you use "
        "{title:shorten(9,-,5)}, the result will be `Ancient E-nhoe`. "
        "If the field's length is less than left chars + right chars + "
        "the length of `middle text`, then the field will be used "
        "intact. For example, the title `The Dome` would not be changed."
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, leading, center_string, trailing):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinShorten.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param leading: Value supplied for leading under the utility contract.
        :param center_string: Value supplied for center string under the utility contract.
        :param trailing: Value supplied for trailing under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        l = max(0, int(leading))
        t = max(0, int(trailing))
        if len(val) > l + len(center_string) + t:
            return val[0:l] + center_string + ("" if t == 0 else val[-t:])
        else:
            return val


class BuiltinCount(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Count`` function.

    Example:
        Exercise BuiltinCount through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "count"
    arg_count = 2
    category = "List manipulation"
    __doc__ = doc = _(
        "count(val, separator) -- interprets the value as a list of items "
        "separated by `separator`, returning the number of items in the "
        "list. Most lists use a comma as the separator, but authors "
        "uses an ampersand. Examples: {tags:count(,)}, {authors:count(&)}"
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, sep):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinCount.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param sep: Delimiter used to split or join list values.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return six_unicode(len([v for v in val.split(sep) if v]))


class BuiltinListitem(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Listitem`` function.

    Example:
        Exercise BuiltinListitem through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "list_item"
    arg_count = 3
    category = "List lookup"
    __doc__ = doc = _(
        "list_item(val, index, separator) -- interpret the value as a list of "
        "items separated by `separator`, returning the `index`th item. "
        "The first item is number zero. The last item can be returned "
        "using `list_item(-1,separator)`. If the item is not in the list, "
        "then the empty value is returned. The separator has the same "
        "meaning as in the count function."
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, index, sep):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinListitem.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param index: Value supplied for index under the utility contract.
        :param sep: Delimiter used to split or join list values.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not val:
            return ""
        index = int(index)
        val = val.split(sep)
        try:
            return val[index].strip()
        except:
            return ""


class BuiltinSelect(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Select`` function.

    Example:
        Exercise BuiltinSelect through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "select"
    arg_count = 2
    category = "List lookup"
    __doc__ = doc = _(
        "select(val, key) -- interpret the value as a comma-separated list "
        'of items, with the items being "id:value". Find the pair with the '
        "id equal to key, and return the corresponding value."
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, key):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinSelect.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param key: Metadata, identifier or local-variable key.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not val:
            return ""
        vals = [v.strip() for v in val.split(",")]
        for v in vals:
            if v.startswith(key + ":"):
                return v[len(key) + 1 :]
        return ""


class BuiltinApproximateFormats(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``ApproximateFormats`` function.

    Example:
        Exercise BuiltinApproximateFormats through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "approximate_formats"
    arg_count = 0
    category = "Get values from metadata"
    __doc__ = doc = _(
        "approximate_formats() -- return a comma-separated "
        "list of formats that at one point were associated with the "
        "book. There is no guarantee that this list is correct, "
        "although it probably is. "
        "This function can be called in template program mode using "
        "the template \"{:'approximate_formats()'}\". "
        "Note that format names are always uppercase, as in EPUB. "
        "This function works only in the GUI. If you want to use these values "
        "in save-to-disk or send-to-device templates then you "
        'must make a custom "Column built from other columns", use '
        "the function in that column's template, and use that "
        "column's value in your save/send templates"
    )

    def evaluate(self, formatter, kwargs, mi, locals):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinApproximateFormats.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if hasattr(mi, "_proxy_metadata"):
            fmt_data = mi._proxy_metadata.db_approx_formats
            if not fmt_data:
                return ""
            data = sorted(fmt_data)
            return ",".join(v.upper() for v in data)
        return _("This function can be used only in the GUI")


class BuiltinFormatsModtimes(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``FormatsModtimes`` function.

    Example:
        Exercise BuiltinFormatsModtimes through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "formats_modtimes"
    arg_count = 1
    category = "Get values from metadata"
    __doc__ = doc = _(
        "formats_modtimes(date_format) -- return a comma-separated "
        "list of colon_separated items representing modification times "
        "for the formats of a book. The date_format parameter "
        "specifies how the date is to be formatted. See the "
        "date_format function for details. You can use the select "
        "function to get the mod time for a specific "
        "format. Note that format names are always uppercase, "
        "as in EPUB."
    )

    def evaluate(self, formatter, kwargs, mi, locals, fmt):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinFormatsModtimes.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param fmt: Date, number or template format specification.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        fmt_data = mi.get("format_metadata", {})
        data = sorted(fmt_data.items(), key=lambda x: x[1]["mtime"], reverse=True)
        return ",".join(k.upper() + ":" + format_date(v["mtime"], fmt) for k, v in data)


class BuiltinFormatsSizes(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``FormatsSizes`` function.

    Example:
        Exercise BuiltinFormatsSizes through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "formats_sizes"
    arg_count = 0
    category = "Get values from metadata"
    __doc__ = doc = _(
        "formats_sizes() -- return a comma-separated list of "
        "colon_separated items representing sizes in bytes "
        "of the formats of a book. You can use the select "
        "function to get the size for a specific "
        "format. Note that format names are always uppercase, "
        "as in EPUB."
    )

    def evaluate(self, formatter, kwargs, mi, locals):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinFormatsSizes.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        fmt_data = mi.get("format_metadata", {})
        return ",".join(k.upper() + ":" + str(v["size"]) for k, v in fmt_data.iteritems())


class BuiltinFormatsPaths(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``FormatsPaths`` function.

    Example:
        Exercise BuiltinFormatsPaths through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "formats_paths"
    arg_count = 0
    category = "Get values from metadata"
    __doc__ = doc = _(
        "formats_paths() -- return a comma-separated list of "
        "colon_separated items representing full path to "
        "the formats of a book. You can use the select "
        "function to get the path for a specific "
        "format. Note that format names are always uppercase, "
        "as in EPUB."
    )

    def evaluate(self, formatter, kwargs, mi, locals):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinFormatsPaths.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        fmt_data = mi.get("format_metadata", {})
        return ",".join(k.upper() + ":" + str(v["path"]) for k, v in fmt_data.iteritems())


class BuiltinHumanReadable(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``HumanReadable`` function.

    Example:
        Exercise BuiltinHumanReadable through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "human_readable"
    arg_count = 1
    category = "Formatting values"
    __doc__ = doc = _("human_readable(v) -- return a string " "representing the number v in KB, MB, GB, etc.")

    def evaluate(self, formatter, kwargs, mi, locals, val):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinHumanReadable.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return human_readable(round(float(val)))
        except:
            return ""


class BuiltinFormatNumber(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``FormatNumber`` function.

    Example:
        Exercise BuiltinFormatNumber through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "format_number"
    arg_count = 2
    category = "Formatting values"
    __doc__ = doc = _(
        "format_number(v, template) -- format the number v using "
        'a python formatting template such as "{0:5.2f}" or '
        '"{0:,d}" or "${0:5,.2f}". The field_name part of the '
        'template must be a 0 (zero) (the "{0:" in the above examples). '
        "See the template language and python documentation for more "
        "examples. Returns the empty string if formatting fails."
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, template):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinFormatNumber.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param template: Template expression parsed or evaluated.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if val == "" or val == "None":
            return ""
        try:
            v1 = float(val)
        except:
            return ""
        try:  # Try formatting the value as a float
            return template.format(v1)
        except:
            pass
        try:  # Try formatting the value as an int
            v2 = trunc(v1)
            if v2 == v1:
                return template.format(v2)
        except:
            pass
        return ""


class BuiltinSublist(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Sublist`` function.

    Example:
        Exercise BuiltinSublist through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "sublist"
    arg_count = 4
    category = "List manipulation"
    __doc__ = doc = _(
        "sublist(val, start_index, end_index, separator) -- interpret the "
        "value as a list of items separated by `separator`, returning a "
        "new list made from the `start_index` to the `end_index` item. "
        "The first item is number zero. If an index is negative, then it "
        "counts from the end of the list. As a special case, an end_index "
        "of zero is assumed to be the length of the list. Examples using "
        "basic template mode and assuming that the tags column (which is "
        'comma-separated) contains "A, B, C": '
        "{tags:sublist(0,1,\\,)} returns \"A\". "
        "{tags:sublist(-1,0,\\,)} returns \"C\". "
        "{tags:sublist(0,-1,\\,)} returns \"A, B\"."
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, start_index, end_index, sep):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinSublist.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param start_index: Value supplied for start index under the utility contract.
        :param end_index: Value supplied for end index under the utility contract.
        :param sep: Delimiter used to split or join list values.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not val:
            return ""
        si = int(start_index)
        ei = int(end_index)
        # allow empty list items so counts are what the user expects
        val = [v.strip() for v in val.split(sep)]

        if sep == ",":
            sep = ", "
        try:
            if ei == 0:
                return sep.join(val[si:])
            else:
                return sep.join(val[si:ei])
        except:
            return ""


class BuiltinSubitems(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Subitems`` function.

    Example:
        Exercise BuiltinSubitems through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "subitems"
    arg_count = 3
    category = "List manipulation"
    __doc__ = doc = _(
        "subitems(val, start_index, end_index) -- This function is used to "
        "break apart lists of items such as genres. It interprets the value "
        "as a comma-separated list of items, where each item is a period-"
        "separated list. Returns a new list made by first finding all the "
        "period-separated items, then for each such item extracting the "
        "`start_index` to the `end_index` components, then combining "
        "the results back together. The first component in a period-"
        "separated list has an index of zero. If an index is negative, "
        "then it counts from the end of the list. As a special case, an "
        "end_index of zero is assumed to be the length of the list. "
        "Example using basic template mode and assuming a #genre value of "
        '"A.B.C": {#genre:subitems(0,1)} returns "A". {#genre:subitems(0,2)} '
        'returns "A.B". {#genre:subitems(1,0)} returns "B.C". Assuming a #genre '
        'value of "A.B.C, D.E.F", {#genre:subitems(0,1)} returns "A, D". '
        '{#genre:subitems(0,2)} returns "A.B, D.E"'
    )

    period_pattern = re.compile(r"(?<=[^\.\s])\.(?=[^\.\s])", re.U)

    def evaluate(self, formatter, kwargs, mi, locals, val, start_index, end_index):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinSubitems.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param start_index: Value supplied for start index under the utility contract.
        :param end_index: Value supplied for end index under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not val:
            return ""
        si = int(start_index)
        ei = int(end_index)
        has_periods = "." in val
        items = [v.strip() for v in val.split(",")]
        rv = set()
        for item in items:
            if has_periods and "." in item:
                components = self.period_pattern.split(item)
            else:
                components = [item]
            try:
                if ei == 0:
                    rv.add(".".join(components[si:]))
                else:
                    rv.add(".".join(components[si:ei]))
            except:
                pass
        return ", ".join(sorted(rv, key=sort_key))


class BuiltinFormatDate(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``FormatDate`` function.

    Example:
        Exercise BuiltinFormatDate through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "format_date"
    arg_count = 2
    category = "Formatting values"
    __doc__ = doc = _(
        "format_date(val, format_string) -- format the value, "
        "which must be a date, using the format_string, returning a string. "
        "The formatting codes are: "
        "d    : the day as number without a leading zero (1 to 31) "
        "dd   : the day as number with a leading zero (01 to 31) "
        'ddd  : the abbreviated localized day name (e.g. "Mon" to "Sun"). '
        'dddd : the long localized day name (e.g. "Monday" to "Sunday"). '
        "M    : the month as number without a leading zero (1 to 12). "
        "MM   : the month as number with a leading zero (01 to 12) "
        'MMM  : the abbreviated localized month name (e.g. "Jan" to "Dec"). '
        'MMMM : the long localized month name (e.g. "January" to "December"). '
        "yy   : the year as two digit number (00 to 99). "
        "yyyy : the year as four digit number. "
        "h    : the hours without a leading 0 (0 to 11 or 0 to 23, depending on am/pm) "
        "hh   : the hours with a leading 0 (00 to 11 or 00 to 23, depending on am/pm) "
        "m    : the minutes without a leading 0 (0 to 59) "
        "mm   : the minutes with a leading 0 (00 to 59) "
        "s    : the seconds without a leading 0 (0 to 59) "
        "ss   : the seconds with a leading 0 (00 to 59) "
        'ap   : use a 12-hour clock instead of a 24-hour clock, with "ap" replaced by the localized string for am or pm '
        'AP   : use a 12-hour clock instead of a 24-hour clock, with "AP" replaced by the localized string for AM or PM '
        "iso  : the date with time and timezone. Must be the only format present"
    )

    def evaluate(self, formatter, kwargs, mi, locals, val, format_string):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinFormatDate.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param format_string: Value supplied for format string under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not val or val == "None":
            return ""
        try:
            dt = parse_date(val)
            s = format_date(dt, format_string)
        except:
            s = "BAD DATE"
        return s


class BuiltinUppercase(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Uppercase`` function.

    Example:
        Exercise BuiltinUppercase through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "uppercase"
    arg_count = 1
    category = "String case changes"
    __doc__ = doc = _("uppercase(val) -- return value of the field in upper case")

    def evaluate(self, formatter, kwargs, mi, locals, val):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinUppercase.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return val.upper()


class BuiltinLowercase(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Lowercase`` function.

    Example:
        Exercise BuiltinLowercase through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "lowercase"
    arg_count = 1
    category = "String case changes"
    __doc__ = doc = _("lowercase(val) -- return value of the field in lower case")

    def evaluate(self, formatter, kwargs, mi, locals, val):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinLowercase.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return val.lower()


class BuiltinTitlecase(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Titlecase`` function.

    Example:
        Exercise BuiltinTitlecase through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "titlecase"
    arg_count = 1
    category = "String case changes"
    __doc__ = doc = _("titlecase(val) -- return value of the field in title case")

    def evaluate(self, formatter, kwargs, mi, locals, val):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinTitlecase.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return titlecase(val)


class BuiltinCapitalize(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Capitalize`` function.

    Example:
        Exercise BuiltinCapitalize through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "capitalize"
    arg_count = 1
    category = "String case changes"
    __doc__ = doc = _("capitalize(val) -- return value of the field capitalized")

    def evaluate(self, formatter, kwargs, mi, locals, val):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinCapitalize.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return capitalize(val)


class BuiltinBooksize(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Booksize`` function.

    Example:
        Exercise BuiltinBooksize through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "booksize"
    arg_count = 0
    category = "Get values from metadata"
    __doc__ = doc = _(
        "booksize() -- return value of the size field. "
        "This function works only in the GUI. If you want to use this value "
        "in save-to-disk or send-to-device templates then you "
        'must make a custom "Column built from other columns", use '
        "the function in that column's template, and use that "
        "column's value in your save/send templates"
    )

    def evaluate(self, formatter, kwargs, mi, locals):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinBooksize.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if hasattr(mi, "_proxy_metadata"):
            try:
                v = mi._proxy_metadata.book_size
                if v is not None:
                    return str(mi._proxy_metadata.book_size)
                return ""
            except:
                pass
            return ""
        return _("This function can be used only in the GUI")


class BuiltinOndevice(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Ondevice`` function.

    Example:
        Exercise BuiltinOndevice through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "ondevice"
    arg_count = 0
    category = "Get values from metadata"
    __doc__ = doc = _(
        "ondevice() -- return Yes if ondevice is set, otherwise return "
        "the empty string. This function works only in the GUI. If you want to "
        "use this value in save-to-disk or send-to-device templates then you "
        'must make a custom "Column built from other columns", use '
        "the function in that column's template, and use that "
        "column's value in your save/send templates"
    )

    def evaluate(self, formatter, kwargs, mi, locals):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinOndevice.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if hasattr(mi, "_proxy_metadata"):
            if mi._proxy_metadata.ondevice_col:
                return _("Yes")
            return ""
        return _("This function can be used only in the GUI")


class BuiltinSeriesSort(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``SeriesSort`` function.

    Example:
        Exercise BuiltinSeriesSort through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "series_sort"
    arg_count = 0
    category = "Get values from metadata"
    __doc__ = doc = _("series_sort() -- return the series sort value")

    def evaluate(self, formatter, kwargs, mi, locals):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinSeriesSort.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if mi.series:
            return title_sort(mi.series)
        return ""


class BuiltinHasCover(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``HasCover`` function.

    Example:
        Exercise BuiltinHasCover through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "has_cover"
    arg_count = 0
    category = "Get values from metadata"
    __doc__ = doc = _("has_cover() -- return Yes if the book has a cover, " "otherwise return the empty string")

    def evaluate(self, formatter, kwargs, mi, locals):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinHasCover.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if mi.has_cover:
            return _("Yes")
        return ""


class BuiltinFirstNonEmpty(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``FirstNonEmpty`` function.

    Example:
        Exercise BuiltinFirstNonEmpty through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "first_non_empty"
    arg_count = -1
    category = "Iterating over values"
    __doc__ = doc = _(
        "first_non_empty(value, value, ...) -- "
        "returns the first value that is not empty. If all values are "
        "empty, then the empty value is returned. "
        "You can have as many values as you want."
    )

    def evaluate(self, formatter, kwargs, mi, locals, *args):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinFirstNonEmpty.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        i = 0
        while i < len(args):
            if args[i]:
                return args[i]
            i += 1
        return ""


class BuiltinAnd(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``And`` function.

    Example:
        Exercise BuiltinAnd through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "and"
    arg_count = -1
    category = "Boolean"
    __doc__ = doc = _(
        "and(value, value, ...) -- "
        'returns the string "1" if all values are not empty, otherwise '
        "returns the empty string. This function works well with test or "
        "first_non_empty. You can have as many values as you want."
    )

    def evaluate(self, formatter, kwargs, mi, locals, *args):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinAnd.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        i = 0
        while i < len(args):
            if not args[i]:
                return ""
            i += 1
        return "1"


class BuiltinOr(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Or`` function.

    Example:
        Exercise BuiltinOr through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "or"
    arg_count = -1
    category = "Boolean"
    __doc__ = doc = _(
        "or(value, value, ...) -- "
        'returns the string "1" if any value is not empty, otherwise '
        "returns the empty string. This function works well with test or "
        "first_non_empty. You can have as many values as you want."
    )

    def evaluate(self, formatter, kwargs, mi, locals, *args):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinOr.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        i = 0
        while i < len(args):
            if args[i]:
                return "1"
            i += 1
        return ""


class BuiltinNot(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Not`` function.

    Example:
        Exercise BuiltinNot through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "not"
    arg_count = 1
    category = "Boolean"
    __doc__ = doc = _(
        "not(value) -- "
        'returns the string "1" if the value is empty, otherwise '
        "returns the empty string. This function works well with test or "
        "first_non_empty. You can have as many values as you want."
    )

    def evaluate(self, formatter, kwargs, mi, locals, val):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinNot.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "" if val else "1"


class BuiltinListUnion(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``ListUnion`` function.

    Example:
        Exercise BuiltinListUnion through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "list_union"
    arg_count = 3
    category = "List manipulation"
    __doc__ = doc = _(
        "list_union(list1, list2, separator) -- "
        "return a list made by merging the items in list1 and list2, "
        "removing duplicate items using a case-insensitive compare. If "
        "items differ in case, the one in list1 is used. "
        "The items in list1 and list2 are separated by separator, as are "
        "the items in the returned list."
    )
    aliases = ["merge_lists"]

    def evaluate(self, formatter, kwargs, mi, locals, list1, list2, separator):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinListUnion.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param list1: Value supplied for list1 under the utility contract.
        :param list2: Value supplied for list2 under the utility contract.
        :param separator: Delimiter used to split or join list values.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        res = [l.strip() for l in list1.split(separator) if l.strip()]
        l2 = [l.strip() for l in list2.split(separator) if l.strip()]
        lcl1 = set([icu_lower(l) for l in res])

        for i in l2:
            if icu_lower(i) not in lcl1 and i not in res:
                res.append(i)
        if separator == ",":
            return ", ".join(res)
        return separator.join(res)


class BuiltinListDifference(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``ListDifference`` function.

    Example:
        Exercise BuiltinListDifference through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "list_difference"
    arg_count = 3
    category = "List manipulation"
    __doc__ = doc = _(
        "list_difference(list1, list2, separator) -- "
        "return a list made by removing from list1 any item found in list2, "
        "using a case-insensitive compare. The items in list1 and list2 "
        "are separated by separator, as are the items in the returned list."
    )

    def evaluate(self, formatter, kwargs, mi, locals, list1, list2, separator):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinListDifference.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param list1: Value supplied for list1 under the utility contract.
        :param list2: Value supplied for list2 under the utility contract.
        :param separator: Delimiter used to split or join list values.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        l1 = [l.strip() for l in list1.split(separator) if l.strip()]
        l2 = set([icu_lower(l.strip()) for l in list2.split(separator) if l.strip()])

        res = []
        for i in l1:
            if icu_lower(i) not in l2 and i not in res:
                res.append(i)
        if separator == ",":
            return ", ".join(res)
        return separator.join(res)


class BuiltinListIntersection(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``ListIntersection`` function.

    Example:
        Exercise BuiltinListIntersection through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "list_intersection"
    arg_count = 3
    category = "List manipulation"
    __doc__ = doc = _(
        "list_intersection(list1, list2, separator) -- "
        "return a list made by removing from list1 any item not found in list2, "
        "using a case-insensitive compare. The items in list1 and list2 "
        "are separated by separator, as are the items in the returned list."
    )

    def evaluate(self, formatter, kwargs, mi, locals, list1, list2, separator):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinListIntersection.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param list1: Value supplied for list1 under the utility contract.
        :param list2: Value supplied for list2 under the utility contract.
        :param separator: Delimiter used to split or join list values.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        l1 = [l.strip() for l in list1.split(separator) if l.strip()]
        l2 = set([icu_lower(l.strip()) for l in list2.split(separator) if l.strip()])

        res = []
        for i in l1:
            if icu_lower(i) in l2 and i not in res:
                res.append(i)
        if separator == ",":
            return ", ".join(res)
        return separator.join(res)


class BuiltinListSort(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``ListSort`` function.

    Example:
        Exercise BuiltinListSort through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "list_sort"
    arg_count = 3
    category = "List manipulation"
    __doc__ = doc = _(
        "list_sort(list, direction, separator) -- "
        "return list sorted using a case-insensitive sort. If direction is "
        "zero, the list is sorted ascending, otherwise descending. The list items "
        "are separated by separator, as are the items in the returned list."
    )

    def evaluate(self, formatter, kwargs, mi, locals, list1, direction, separator):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinListSort.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param list1: Value supplied for list1 under the utility contract.
        :param direction: Value supplied for direction under the utility contract.
        :param separator: Delimiter used to split or join list values.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        res = [l.strip() for l in list1.split(separator) if l.strip()]
        if separator == ",":
            return ", ".join(sorted(res, key=sort_key, reverse=direction != "0"))
        return separator.join(sorted(res, key=sort_key, reverse=direction != "0"))


class BuiltinListEquals(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``ListEquals`` function.

    Example:
        Exercise BuiltinListEquals through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "list_equals"
    arg_count = 6
    category = "List manipulation"
    __doc__ = doc = _(
        "list_equals(list1, sep1, list2, sep2, yes_val, no_val) -- "
        "return yes_val if list1 and list2 contain the same items, "
        "otherwise return no_val. The items are determined by splitting "
        "each list using the appropriate separator character (sep1 or "
        "sep2). The order of items in the lists is not relevant. "
        "The compare is case insensitive."
    )

    def evaluate(self, formatter, kwargs, mi, locals, list1, sep1, list2, sep2, yes_val, no_val):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinListEquals.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param list1: Value supplied for list1 under the utility contract.
        :param sep1: Value supplied for sep1 under the utility contract.
        :param list2: Value supplied for list2 under the utility contract.
        :param sep2: Value supplied for sep2 under the utility contract.
        :param yes_val: Value supplied for yes val under the utility contract.
        :param no_val: Value supplied for no val under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        s1 = set([icu_lower(l.strip()) for l in list1.split(sep1) if l.strip()])
        s2 = set([icu_lower(l.strip()) for l in list2.split(sep2) if l.strip()])
        if s1 == s2:
            return yes_val
        return no_val


class BuiltinListRe(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``ListRe`` function.

    Example:
        Exercise BuiltinListRe through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "list_re"
    arg_count = 4
    category = "List manipulation"
    __doc__ = doc = _(
        "list_re(src_list, separator, include_re, opt_replace) -- "
        "Construct a list by first separating src_list into items using "
        "the separator character. For each item in the list, check if it "
        "matches include_re. If it does, then add it to the list to be "
        "returned. If opt_replace is not the empty string, then apply the "
        "replacement before adding the item to the returned list."
    )

    def evaluate(
        self,
        formatter,
        kwargs,
        mi,
        locals,
        src_list,
        separator,
        include_re,
        opt_replace,
    ):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinListRe.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param src_list: Value supplied for src list under the utility contract.
        :param separator: Delimiter used to split or join list values.
        :param include_re: Value supplied for include re under the utility contract.
        :param opt_replace: Value supplied for opt replace under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        l = [l.strip() for l in src_list.split(separator) if l.strip()]
        res = []
        for item in l:
            if re.search(include_re, item, flags=re.I) is not None:
                if opt_replace:
                    item = re.sub(include_re, opt_replace, item)
                for i in [t.strip() for t in item.split(separator) if t.strip()]:
                    if i not in res:
                        res.append(i)
        if separator == ",":
            return ", ".join(res)
        return separator.join(res)


class BuiltinListReGroup(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``ListReGroup`` function.

    Example:
        Exercise BuiltinListReGroup through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "list_re_group"
    arg_count = -1
    category = "List manipulation"
    __doc__ = doc = _(
        "list_re_group(src_list, separator, include_re, search_re, group_1_template, ...) -- "
        "Like list_re except replacements are not optional. It "
        "uses re_group(list_item, search_re, group_1_template, ...) when "
        "doing the replacements on the resulting list."
    )

    def evaluate(self, formatter, kwargs, mi, locals, src_list, separator, include_re, search_re, *args):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinListReGroup.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param src_list: Value supplied for src list under the utility contract.
        :param separator: Delimiter used to split or join list values.
        :param include_re: Value supplied for include re under the utility contract.
        :param search_re: Value supplied for search re under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from formatter import EvalFormatter

        l = [l.strip() for l in src_list.split(separator) if l.strip()]
        res = []
        for item in l:

            def repl(mo):
                """
                Perform the repl utility operation under explicit compatibility rules.

                Example:
                    Exercise BuiltinListReGroup.evaluate.repl through a consuming regression::

                        python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


                :param mo: Value supplied for mo under the utility contract.
                :return: The normalized value, metadata record, path, stream result or collection
                    described above.
                """
                newval = ""
                if mo and mo.lastindex:
                    for dex in range(0, mo.lastindex):
                        gv = mo.group(dex + 1)
                        if gv is None:
                            continue
                        if len(args) > dex:
                            template = args[dex].replace("[[", "{").replace("]]", "}")
                            newval += EvalFormatter().safe_format(
                                template, {"$": gv}, "EVAL", None, strip_results=False
                            )
                        else:
                            newval += gv
                return newval

            if re.search(include_re, item, flags=re.I) is not None:
                item = re.sub(search_re, repl, item, flags=re.I)
                for i in [t.strip() for t in item.split(separator) if t.strip()]:
                    if i not in res:
                        res.append(i)
        if separator == ",":
            return ", ".join(res)
        return separator.join(res)


class BuiltinToday(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Today`` function.

    Example:
        Exercise BuiltinToday through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "today"
    arg_count = 0
    category = "Date functions"
    __doc__ = doc = _(
        "today() -- "
        "return a date string for today. This value is designed for use in "
        "format_date or days_between, but can be manipulated like any "
        "other string. The date is in ISO format."
    )

    def evaluate(self, formatter, kwargs, mi, locals):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinToday.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return format_date(now(), "iso")


class BuiltinDaysBetween(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``DaysBetween`` function.

    Example:
        Exercise BuiltinDaysBetween through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "days_between"
    arg_count = 2
    category = "Date functions"
    __doc__ = doc = _(
        "days_between(date1, date2) -- "
        "return the number of days between date1 and date2. The number is "
        "positive if date1 is greater than date2, otherwise negative. If "
        "either date1 or date2 are not dates, the function returns the "
        "empty string."
    )

    def evaluate(self, formatter, kwargs, mi, locals, date1, date2):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinDaysBetween.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param date1: Value supplied for date1 under the utility contract.
        :param date2: Value supplied for date2 under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            d1 = parse_date(date1)
            if d1 == UNDEFINED_DATE:
                return ""
            d2 = parse_date(date2)
            if d2 == UNDEFINED_DATE:
                return ""
        except:
            return ""
        i = d1 - d2
        return "%.1f" % (i.days + (i.seconds / (24.0 * 60.0 * 60.0)))


class BuiltinLanguageStrings(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``LanguageStrings`` function.

    Example:
        Exercise BuiltinLanguageStrings through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "language_strings"
    arg_count = 2
    category = "Get values from metadata"
    __doc__ = doc = _(
        "language_strings(lang_codes, localize) -- "
        "return the strings for the language codes passed in lang_codes. "
        "If localize is zero, return the strings in English. If "
        "localize is not zero, return the strings in the language of "
        "the current locale. Lang_codes is a comma-separated list."
    )

    def evaluate(self, formatter, kwargs, mi, locals, lang_codes, localize):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinLanguageStrings.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param lang_codes: Value supplied for lang codes under the utility contract.
        :param localize: Value supplied for localize under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        retval = []
        for c in [c.strip() for c in lang_codes.split(",") if c.strip()]:
            try:
                n = calibre_langcode_to_name(c, localize != "0")
                if n:
                    retval.append(n)
            except:
                pass
        return ", ".join(retval)


class BuiltinLanguageCodes(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``LanguageCodes`` function.

    Example:
        Exercise BuiltinLanguageCodes through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "language_codes"
    arg_count = 1
    category = "Get values from metadata"
    __doc__ = doc = _(
        "language_codes(lang_strings) -- "
        "return the language codes for the strings passed in lang_strings. "
        "The strings must be in the language of the current locale. "
        "Lang_strings is a comma-separated list."
    )

    def evaluate(self, formatter, kwargs, mi, locals, lang_strings):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinLanguageCodes.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param lang_strings: Value supplied for lang strings under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        retval = []
        for c in [c.strip() for c in lang_strings.split(",") if c.strip()]:
            try:
                cv = canonicalize_lang(c)
                if cv:
                    retval.append(canonicalize_lang(cv))
            except:
                pass
        return ", ".join(retval)


class BuiltinCurrentLibraryName(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``CurrentLibraryName`` function.

    Example:
        Exercise BuiltinCurrentLibraryName through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "current_library_name"
    arg_count = 0
    category = "Get values from metadata"
    __doc__ = doc = _(
        "current_library_name() -- "
        "return the last name on the path to the current calibre library. "
        "This function can be called in template program mode using the "
        "template \"{:'current_library_name()'}\"."
    )

    def evaluate(self, formatter, kwargs, mi, locals):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinCurrentLibraryName.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.utils.calibre.library import current_library_name

        return current_library_name()


class BuiltinCurrentLibraryPath(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``CurrentLibraryPath`` function.

    Example:
        Exercise BuiltinCurrentLibraryPath through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "current_library_path"
    arg_count = 0
    category = "Get values from metadata"
    __doc__ = doc = _(
        "current_library_path() -- "
        "return the path to the current calibre library. This function can "
        "be called in template program mode using the template "
        "\"{:'current_library_path()'}\"."
    )

    def evaluate(self, formatter, kwargs, mi, locals):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinCurrentLibraryPath.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.utils.calibre.library import current_library_path

        return current_library_path()


class BuiltinFinishFormatting(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``FinishFormatting`` function.

    Example:
        Exercise BuiltinFinishFormatting through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "finish_formatting"
    arg_count = 4
    category = "Formatting values"
    __doc__ = doc = _(
        "finish_formatting(val, fmt, prefix, suffix) -- apply the "
        "format, prefix, and suffix to a value in the same way as "
        "done in a template like `{series_index:05.2f| - |- }`. For "
        "example, the following program produces the same output "
        "as the above template: "
        'program: finish_formatting(field("series_index"), "05.2f", " - ", " - ")'
    )

    def evaluate(self, formatter, kwargs, mi, locals_, val, fmt, prefix, suffix):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinFinishFormatting.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals_: Local template variables available during evaluation.
        :param val: Template or metadata value evaluated by the operation.
        :param fmt: Date, number or template format specification.
        :param prefix: Text prepended to the formatted or selected result.
        :param suffix: Text appended to the formatted or selected result.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not val:
            return val
        return prefix + formatter._do_format(val, fmt) + suffix


class BuiltinVirtualLibraries(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``VirtualLibraries`` function.

    Example:
        Exercise BuiltinVirtualLibraries through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "virtual_libraries"
    arg_count = 0
    category = "Get values from metadata"
    __doc__ = doc = _(
        "virtual_libraries() -- return a comma-separated list of "
        "virtual libraries that contain this book. This function "
        "works only in the GUI. If you want to use these values "
        "in save-to-disk or send-to-device templates then you "
        'must make a custom "Column built from other columns", use '
        "the function in that column's template, and use that "
        "column's value in your save/send templates"
    )

    def evaluate(self, formatter, kwargs, mi, locals_):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinVirtualLibraries.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals_: Local template variables available during evaluation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if hasattr(mi, "_proxy_metadata"):
            return mi._proxy_metadata.virtual_libraries
        return _("This function can be used only in the GUI")


class BuiltinUserCategories(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``UserCategories`` function.

    Example:
        Exercise BuiltinUserCategories through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "user_categories"
    arg_count = 0
    category = "Get values from metadata"
    __doc__ = doc = _(
        "user_categories() -- return a comma-separated list of "
        "the user categories that contain this book. This function "
        "works only in the GUI. If you want to use these values "
        "in save-to-disk or send-to-device templates then you "
        'must make a custom "Column built from other columns", use '
        "the function in that column's template, and use that "
        "column's value in your save/send templates"
    )

    def evaluate(self, formatter, kwargs, mi, locals_):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinUserCategories.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals_: Local template variables available during evaluation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if hasattr(mi, "_proxy_metadata"):
            cats = set(k for k, v in mi._proxy_metadata.user_categories.iteritems() if v)
            cats = sorted(cats, key=sort_key)
            return ", ".join(cats)
        return _("This function can be used only in the GUI")


class BuiltinTransliterate(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``Transliterate`` function.

    Example:
        Exercise BuiltinTransliterate through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "transliterate"
    arg_count = 1
    category = "String manipulation"
    __doc__ = doc = _(
        "transliterate(a) -- Returns a string in a latin alphabet "
        "formed by approximating the sound of the words in the "
        'source string. For example, if the source is "{0}"'
        ' the function returns "{1}".'
    ).format("Фёдор Миха́йлович Достоевский", "Fiodor Mikhailovich Dostoievskii")

    def evaluate(self, formatter, kwargs, mi, locals, source):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinTransliterate.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param source: Value supplied for source under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.utils.calibre.utils.filenames import ascii_text

        return ascii_text(source)


class BuiltinAuthorLinks(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``AuthorLinks`` function.

    Example:
        Exercise BuiltinAuthorLinks through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "author_links"
    arg_count = 2
    category = "Get values from metadata"
    __doc__ = doc = _(
        "author_links(val_separator, pair_separator) -- returns "
        "a string containing a list of authors and that author's "
        "link values in the "
        "form author1 val_separator author1link pair_separator "
        "author2 val_separator author2link etc. An author is "
        "separated from its link value by the val_separator string "
        "with no added spaces. author:linkvalue pairs are separated "
        "by the pair_separator string argument with no added spaces. "
        "It is up to you to choose separator strings that do "
        "not occur in author names or links. An author is "
        "included even if the author link is empty."
    )

    def evaluate(self, formatter, kwargs, mi, locals, val_sep, pair_sep):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinAuthorLinks.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val_sep: Value supplied for val sep under the utility contract.
        :param pair_sep: Value supplied for pair sep under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if hasattr(mi, "_proxy_metadata"):
            link_data = mi._proxy_metadata.author_link_map
            if not link_data:
                return ""
            names = sorted(link_data.keys(), key=sort_key)
            return pair_sep.join(n + val_sep + link_data[n] for n in names)
        return _("This function can be used only in the GUI")


class BuiltinAuthorSorts(BuiltinFormatterFunction):
    """
    Implement the Calibre template ``AuthorSorts`` function.

    Example:
        Exercise BuiltinAuthorSorts through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    name = "author_sorts"
    arg_count = 1
    category = "Get values from metadata"
    __doc__ = doc = _(
        "author_sorts(val_separator) -- returns a string "
        "containing a list of author's sort values for the "
        "authors of the book. The sort is the one in the author "
        "metadata (different from the author_sort in books). The "
        "returned list has the form author sort 1 val_separator "
        "author sort 2 etc. The author sort values in this list "
        "are in the same order as the authors of the book. If "
        "you want spaces around val_separator then include them "
        "in the separator string"
    )

    def evaluate(self, formatter, kwargs, mi, locals, val_sep):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise BuiltinAuthorSorts.evaluate through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param formatter: Template formatter supplying evaluation services and context.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :param mi: Metadata object exposed to the template function.
        :param locals: Local template variables available during evaluation.
        :param val_sep: Value supplied for val sep under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        sort_data = mi.author_sort_map
        if not sort_data:
            return ""
        names = [sort_data.get(n) for n in mi.authors if n.strip()]
        return val_sep.join(n for n in names)


_formatter_builtins = [
    BuiltinAdd(),
    BuiltinAnd(),
    BuiltinApproximateFormats(),
    BuiltinAssign(),
    BuiltinAuthorLinks(),
    BuiltinAuthorSorts(),
    BuiltinBooksize(),
    BuiltinCapitalize(),
    BuiltinCmp(),
    BuiltinContains(),
    BuiltinCount(),
    BuiltinCurrentLibraryName(),
    BuiltinCurrentLibraryPath(),
    BuiltinDaysBetween(),
    BuiltinDivide(),
    BuiltinEval(),
    BuiltinFirstNonEmpty(),
    BuiltinField(),
    BuiltinFinishFormatting(),
    BuiltinFirstMatchingCmp(),
    BuiltinFormatDate(),
    BuiltinFormatNumber(),
    BuiltinFormatsModtimes(),
    BuiltinFormatsPaths(),
    BuiltinFormatsSizes(),
    BuiltinHasCover(),
    BuiltinHumanReadable(),
    BuiltinIdentifierInList(),
    BuiltinIfempty(),
    BuiltinLanguageCodes(),
    BuiltinLanguageStrings(),
    BuiltinInList(),
    BuiltinListDifference(),
    BuiltinListEquals(),
    BuiltinListIntersection(),
    BuiltinListitem(),
    BuiltinListRe(),
    BuiltinListReGroup(),
    BuiltinListSort(),
    BuiltinListUnion(),
    BuiltinLookup(),
    BuiltinLowercase(),
    BuiltinMultiply(),
    BuiltinNot(),
    BuiltinOndevice(),
    BuiltinOr(),
    BuiltinPrint(),
    BuiltinRawField(),
    BuiltinRawList(),
    BuiltinRe(),
    BuiltinReGroup(),
    BuiltinSelect(),
    BuiltinSeriesSort(),
    BuiltinShorten(),
    BuiltinStrcat(),
    BuiltinStrcatMax(),
    BuiltinStrcmp(),
    BuiltinStrInList(),
    BuiltinStrlen(),
    BuiltinSubitems(),
    BuiltinSublist(),
    BuiltinSubstr(),
    BuiltinSubtract(),
    BuiltinSwapAroundComma(),
    BuiltinSwitch(),
    BuiltinTemplate(),
    BuiltinTest(),
    BuiltinTitlecase(),
    BuiltinToday(),
    BuiltinTransliterate(),
    BuiltinUppercase(),
    BuiltinUserCategories(),
    BuiltinVirtualLibraries(),
]


class FormatterUserFunction(FormatterFunction):
    """
    Provide the FormatterUserFunction utility contract with explicit state and cleanup behavior.

    Example:
        Exercise FormatterUserFunction through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
    """
    def __init__(self, name, doc, arg_count, program_text):
        """
        Initialize and validate the FormatterUserFunction state.

        Example:
            Exercise FormatterUserFunction.  init   through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param doc: Value supplied for doc under the utility contract.
        :param arg_count: Value supplied for arg count under the utility contract.
        :param program_text: Value supplied for program text under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = name
        self.doc = doc
        self.arg_count = arg_count
        self.program_text = program_text


tabs = re.compile(r"^\t*")


def compile_user_function(name, doc, arg_count, eval_func):
    """
    Perform the compile user function utility operation under explicit compatibility rules.

    Example:
        Exercise compile user function through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param doc: Value supplied for doc under the utility contract.
    :param arg_count: Value supplied for arg count under the utility contract.
    :param eval_func: Value supplied for eval func under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def replace_func(mo):
        """
        Perform the replace func utility operation under explicit compatibility rules.

        Example:
            Exercise compile user function.replace func through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


        :param mo: Value supplied for mo under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return mo.group().replace("\t", "    ")

    func = "    " + "\n    ".join([tabs.sub(replace_func, line) for line in eval_func.splitlines()])
    prog = (
        """
from LiuXin_alpha.utils.calibre.utils.formatter_functions import FormatterUserFunction
from LiuXin_alpha.utils.localization import _
from LiuXin_alpha.utils.calibre.utils.formatter_functions import formatter_functions
class UserFunction(FormatterUserFunction):
"""
        + func
    )
    locals_ = {}
    if DEBUG and tweaks.get("enable_template_debug_printing", False):
        print(prog)
    exec(prog, locals_)
    cls = locals_["UserFunction"](name, doc, arg_count, eval_func)
    return cls


def load_user_template_functions(library_uuid, funcs):
    """
    Perform the load user template functions utility operation under explicit compatibility rules.

    Example:
        Exercise load user template functions through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


    :param library_uuid: Value supplied for library uuid under the utility contract.
    :param funcs: Value supplied for funcs under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    unload_user_template_functions(library_uuid)

    compiled_funcs = []
    for func in funcs:
        try:
            # Force a name conflict to test the logic
            # if func[0] == 'myFunc2':
            #     func[0] = 'myFunc3'

            # Compile the function so that the tab processing is done on the
            # source. This helps ensure that if the function already is defined
            # then white space differences don't cause them to compare differently

            compiled_funcs.append(compile_user_function(*func))
        except:
            traceback.print_exc()
    formatter_functions().register_functions(library_uuid, compiled_funcs)


def unload_user_template_functions(library_uuid):
    """
    Perform the unload user template functions utility operation under explicit compatibility rules.

    Example:
        Exercise unload user template functions through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


    :param library_uuid: Value supplied for library uuid under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    formatter_functions().unregister_functions(library_uuid)
