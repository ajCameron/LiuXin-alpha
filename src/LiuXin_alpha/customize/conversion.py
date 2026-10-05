# -*- coding: utf-8 -*-

"""
Define customization hooks for conversion input and output plugins.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise conversion through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
"""

import re
import os
import shutil

from LiuXin_alpha.customize import Plugin

from LiuXin_alpha.utils.storage.local import CurrentDir

from LiuXin_alpha.utils.resources import calibreI as I
from LiuXin_alpha.utils.localization import trans as _

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode, six_unicode as unicode


class ConversionOption:
    """
    Class representing a conversion option.

    Example:
        Exercise ConversionOption through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(
        self, name: str = None, option_help: str = None, long_switch=None, short_switch=None, choices=None
    ) -> None:
        """
        Set parameters for the conversion option.

        Example:
            Exercise ConversionOption.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param option_help: Value supplied for option help under the utility contract.
        :param long_switch: Value supplied for long switch under the utility contract.
        :param short_switch: Value supplied for short switch under the utility contract.
        :param choices: Value supplied for choices under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = name
        self.option_help = option_help
        self.long_switch = long_switch
        self.short_switch = short_switch
        self.choices = choices

        if self.long_switch is None:
            self.long_switch = self.name.replace("_", "-")

        self.validate_parameters()

    def validate_parameters(self):
        """
        Validate the parameters passed to :meth:`__init__`.

        Example:
            Exercise ConversionOption.validate parameters through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if re.match(r"[a-zA-Z_]([a-zA-Z0-9_])*", self.name) is None:
            raise ValueError(self.name + " is not a valid Python identifier")
        if not self.option_help:
            raise ValueError("You must set the help text")

    def __hash__(self):
        """
        hash of the name of the conversion option.

        Example:
            Exercise ConversionOption.  hash   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return hash(self.name)

    def __eq__(self, other):
        """
        Hash check that the other option is the same as this one.

        Example:
            Exercise ConversionOption.  eq   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return hash(self) == hash(other)

    def clone(self):
        """
        Returns a clone of this option.

        Example:
            Exercise ConversionOption.clone through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ConversionOption(
            name=self.name,
            option_help=self.option_help,
            long_switch=self.long_switch,
            short_switch=self.short_switch,
            choices=self.choices,
        )


class OptionRecommendation:
    """
    Provide a recommended value for an option.

    Example:
        Exercise OptionRecommendation through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    LOW = 1
    MED = 2
    HIGH = 3

    def __init__(self, recommended_value=None, level=LOW, **kwargs):
        """
        Includes the recommended value of the options and the strength of the recommendation (low, medium, high).

        Example:
            Exercise OptionRecommendation.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param recommended_value: Value supplied for recommended value under the utility
            contract.
        :param level: Value supplied for level under the utility contract.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        self.level = level
        self.recommended_value = recommended_value
        self.option = kwargs.pop("option", None)
        if self.option is None:
            self.option = ConversionOption(**kwargs)

        self.validate_parameters()

    @property
    def option_help(self):
        """
        Returns help for the option this is a recommendation for.

        Example:
            Exercise OptionRecommendation.option help through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.option.option_help

    def clone(self):
        """
        Returns a duplicate of this recommendation.

        Example:
            Exercise OptionRecommendation.clone through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return OptionRecommendation(
            recommended_value=self.recommended_value,
            level=self.level,
            option=self.option.clone(),
        )

    def validate_parameters(self):
        """
        Check the parameters provided to this class are semantically correct.

        Example:
            Exercise OptionRecommendation.validate parameters through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.option.choices and self.recommended_value not in self.option.choices:
            raise ValueError("OpRec: %s: Recommended value not in choices" % self.option.name)
        if not (isinstance(self.recommended_value, (int, float, str, unicode)) or self.recommended_value is None):
            raise ValueError(
                "OpRec: %s:" % self.option.name + repr(self.recommended_value) + " is not a string or a number"
            )


class DummyReporter(object):
    """
    When we don't want to define a reporter.

    Example:
        Exercise DummyReporter through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    def __init__(self):
        """
        Initialize and validate the dummyreporter state.

        Example:
            Exercise DummyReporter.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; validated state is stored on the receiving object.
        """
        self.cancel_requested = False

    def __call__(self, percent, msg=""):
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise DummyReporter.  call   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param percent: Value supplied for percent under the utility contract.
        :param msg: Value supplied for msg under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


# gui_configuration_widget moved to LiuXin.interfaces.gui_common.customize


class InputFormatPlugin(Plugin):
    """
    InputFormatPlugins are responsible for converting a document into HTML+OPF+CSS+etc. T he results of the conversion *must* be encoded in UTF-8. The main action happens in :meth:`convert`.

    Example:
        Exercise InputFormatPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    type = _("Conversion Input")
    can_be_disabled = False
    supported_platforms = ["windows", "osx", "linux"]

    #: Set of file types for which this plugin should be run
    #: For example: ``set(['azw', 'mobi', 'prc'])``
    file_types = set([])

    #: If True, this input plugin generates a collection of images,
    #: one per HTML file. This can be set dynamically, in the convert method
    #: if the input files can be both image collections and non-image collections.
    #: If you set this to True, you must implement the get_images() method that returns
    #: a list of images.
    is_image_collection = False

    #: Number of CPU cores used by this plugin
    #: A value of -1 means that it uses all available cores
    core_usage = 1

    #: If set to True, the input plugin will perform special processing
    #: to make its output suitable for viewing
    for_viewer = False

    #: The encoding that this input plugin creates files in. A value of
    #: None means that the encoding is undefined and must be
    #: detected individually
    output_encoding = "utf-8"

    #: Options shared by all Input format plugins. Do not override
    #: in sub-classes. Use :attr:`options` instead. Every option must be an
    #: instance of :class:`OptionRecommendation`.
    common_options = {
        OptionRecommendation(
            name="input_encoding",
            recommended_value=None,
            level=OptionRecommendation.LOW,
            option_help=_(
                "Specify the character encoding of the input document. If "
                "set this option will override any encoding declared by the "
                "document itself. Particularly useful for documents that "
                "do not declare an encoding or that have erroneous "
                "encoding declarations."
            ),
        ),
    }

    #: Options to customize the behavior of this plugin. Every option must be an
    #: instance of :class:`OptionRecommendation`.
    options = set([])

    #: A set of 3-tuples of the form
    #: (option_name, recommended_value, recommendation_level)
    recommendations = set([])

    def __init__(self, *args):
        """
        Initialize and validate the inputformatplugin state.

        Example:
            Exercise InputFormatPlugin.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        Plugin.__init__(self, *args)
        self.report_progress = DummyReporter()

    def get_images(self):
        """
        Return a list of absolute paths to the images, if this input plugin represents an image collection.

        Example:
            Exercise InputFormatPlugin.get images through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError()

    def convert(self, stream, options, file_ext, log, accelerators):
        """
        This method must be implemented in sub-classes - returning a path to a created OPF file or an :class:`OEBBook`.

        Example:
            Exercise InputFormatPlugin.convert through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param options: Value supplied for options under the utility contract.
        :param file_ext: Value supplied for file ext under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param accelerators: Value supplied for accelerators under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def __call__(self, stream, options, file_ext, log, accelerators, output_dir):
        """
        Calls convert with the stream after changing the current working dir to the output_dir.

        Example:
            Exercise InputFormatPlugin.  call   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param options: Value supplied for options under the utility contract.
        :param file_ext: Value supplied for file ext under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param accelerators: Value supplied for accelerators under the utility contract.
        :param output_dir: Value supplied for output dir under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            log("InputFormatPlugin: %s running" % self.name)
            if hasattr(stream, "name"):
                log("on", stream.name)
        except:
            # In case stdout is broken
            pass

        with CurrentDir(output_dir, workaround_temp_folder_permissions=True):
            for x in os.listdir("."):
                shutil.rmtree(x) if os.path.isdir(x) else os.remove(x)

            ret = self.convert(stream, options, file_ext, log, accelerators)

        return ret

    def postprocess_book(self, oeb, opts, log):
        """
        Called to allow the input plugin to perform postprocessing after the book has been parsed.

        Example:
            Exercise InputFormatPlugin.postprocess book through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def specialize(self, oeb, opts, log, output_fmt):
        """
        Called to allow the input plugin to specialize the parsed book for a particular output format.

        Example:
            Exercise InputFormatPlugin.specialize through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param output_fmt: Value supplied for output fmt under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def gui_configuration_widget(self, parent, get_option_by_name, get_option_help, db, book_id=None):
        """
        Called to create the widget used for configuring this plugin in the calibre GUI.

        Example:
            Exercise InputFormatPlugin.gui configuration widget through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param parent: Value supplied for parent under the utility contract.
        :param get_option_by_name: Value supplied for get option by name under the utility
            contract.
        :param get_option_help: Value supplied for get option help under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError(
            "This is a interface problem. " "The code for this has been moved to LiuXin.interfaces.gui_common.customize"
        )


class OutputFormatPlugin(Plugin):
    """
    OutputFormatPlugins are responsible for converting an OEB document (OPF+HTML) into an output ebook.

    Example:
        Exercise OutputFormatPlugin through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    type = _("Conversion Output")
    can_be_disabled = False
    supported_platforms = ["windows", "osx", "linux"]

    #: The file type (extension without leading period) that this
    #: plugin outputs
    file_type = None

    #: Options shared by all Input format plugins. Do not override
    #: in sub-classes. Use :attr:`options` instead. Every option must be an
    #: instance of :class:`OptionRecommendation`.
    common_options = {
        OptionRecommendation(
            name="pretty_print",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_(
                "If specified, the output plugin will try to create output "
                "that is as human readable as possible. May not have any effect "
                "for some output plugins."
            ),
        ),
    }

    #: Options to customize the behavior of this plugin. Every option must be an
    #: instance of :class:`OptionRecommendation`.
    options = set([])

    #: A set of 3-tuples of the form
    #: (option_name, recommended_value, recommendation_level)
    recommendations = set([])

    @property
    def description(self):
        """
        Description for the plugin.

        Example:
            Exercise OutputFormatPlugin.description through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return _("Convert ebooks to the %s format") % self.file_type

    def __init__(self, *args):
        """
        Initialize and validate the outputformatplugin state.

        Example:
            Exercise OutputFormatPlugin.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        Plugin.__init__(self, *args)
        self.report_progress = DummyReporter()

    def convert(self, oeb_book, output, input_plugin, opts, log):
        """
        Render the contents of `oeb_book` (an instance of :class:`LiuXin.file_formats.oeb.OEBBook`) to the output.

        Example:
            Exercise OutputFormatPlugin.convert through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param output: Value supplied for output under the utility contract.
        :param input_plugin: Value supplied for input plugin under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @property
    def is_periodical(self):
        """
        Is the file registered as a periodical?

        Example:
            Exercise OutputFormatPlugin.is periodical through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: True when the documented condition holds; otherwise False.
        """
        return self.oeb.metadata.publication_type and six_unicode(self.oeb.metadata.publication_type[0]).startswith(
            "periodical:"
        )

    def specialize_css_for_output(self, log, opts, item, stylizer):
        """
        Can be used to make changes to the css during the CSS flattening process.

        Example:
            Exercise OutputFormatPlugin.specialize css for output through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param log: Value supplied for log under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param item: Value supplied for item under the utility contract.
        :param stylizer: Value supplied for stylizer under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def gui_configuration_widget(self, parent, get_option_by_name, get_option_help, db, book_id=None):
        """
        Called to create the widget used for configuring this plugin in the calibre GUI.

        Example:
            Exercise OutputFormatPlugin.gui configuration widget through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param parent: Value supplied for parent under the utility contract.
        :param get_option_by_name: Value supplied for get option by name under the utility
            contract.
        :param get_option_help: Value supplied for get option help under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param book_id: Value supplied for book id under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Method logic has been moved to LiuXin.interface.gui_common.customize")


# ----------------------------------------------------------------------------------------------------------------------
#
# - LIUXIN PLUGINS START HERE
#
# ----------------------------------------------------------------------------------------------------------------------
