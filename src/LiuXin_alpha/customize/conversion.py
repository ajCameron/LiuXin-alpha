# -*- coding: utf-8 -*-

"""
Defines the plugin system for conversions.

These plugins transform one ebook file format into another.
"""

import re
import os
import shutil

from LiuXin.customize import Plugin

from LiuXin.utils.calibre import CurrentDir
from LiuXin.utils.calibre_utils.calibre_resources import I
from LiuXin.utils.localization import trans as _

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

from past.builtins import unicode


class ConversionOption:
    """
    Class representing a conversion option.

    Most conversion processes will have some kind of options to control them.
    All of the representatives of them should descend from this base class.
    """

    def __init__(
        self, name: str = None, option_help: str = None, long_switch=None, short_switch=None, choices=None
    ) -> None:
        """
        Set parameters for the conversion option.

        :param name: Name of the conversion option.
        :param option_help: A string which provides help as to what the option does and how to use it.
        :param long_switch:
        :param short_switch:
        :param choices:
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
        """
        if re.match(r"[a-zA-Z_]([a-zA-Z0-9_])*", self.name) is None:
            raise ValueError(self.name + " is not a valid Python identifier")
        if not self.option_help:
            raise ValueError("You must set the help text")

    def __hash__(self):
        """
        hash of the name of the conversion option.

        :return:
        """
        return hash(self.name)

    def __eq__(self, other):
        """
        Hash check that the other option is the same as this one.

        :param other:
        :return:
        """
        return hash(self) == hash(other)

    def clone(self):
        """
        Returns a clone of this option.

        Clone is a new class entirely and not a reference.
        :return:
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
    """

    LOW = 1
    MED = 2
    HIGH = 3

    def __init__(self, recommended_value=None, level=LOW, **kwargs):
        """
        Includes the recommended value of the options and the strength of the recommendation (low, medium, high).

        :param recommended_value:
        :param level:
        :param kwargs:
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

        :return:
        """
        return self.option.option_help

    def clone(self):
        """
        Returns a duplicate of this recommendation.

        :return:
        """
        return OptionRecommendation(
            recommended_value=self.recommended_value,
            level=self.level,
            option=self.option.clone(),
        )

    def validate_parameters(self):
        """
        Check the parameters provided to this class are semantically correct.

        :return:
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
    """

    def __init__(self):
        self.cancel_requested = False

    def __call__(self, percent, msg=""):
        pass


# gui_configuration_widget moved to LiuXin.interfaces.gui_common.customize


class InputFormatPlugin(Plugin):
    """
    InputFormatPlugins are responsible for converting a document into HTML+OPF+CSS+etc.
    T
    he results of the conversion *must* be encoded in UTF-8.
    The main action happens in :meth:`convert`.
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
        Plugin.__init__(self, *args)
        self.report_progress = DummyReporter()

    def get_images(self):
        """
        Return a list of absolute paths to the images, if this input plugin represents an image collection.

        The list of images is in the same order as the spine and the TOC.
        """
        raise NotImplementedError()

    def convert(self, stream, options, file_ext, log, accelerators):
        """
        This method must be implemented in sub-classes - returning a path to a created OPF file or an :class:`OEBBook`.

        All output should be contained in the current directory.
        If this plugin creates files outside the current
        directory they must be deleted/marked for deletion before this method
        returns.

        :param stream:   A file like object that contains the input file.
        :param options:  Options to customize the conversion process.
                         Guaranteed to have attributes corresponding
                         to all the options declared by this plugin. In
                         addition, it will have a verbose attribute that
                         takes integral values from zero upwards. Higher numbers
                         mean be more verbose. Another useful attribute is
                         ``input_profile`` that is an instance of
                         :class:`calibre.customize.profiles.InputProfile`.
        :param file_ext: The extension (without the .) of the input file. It
                         is guaranteed to be one of the `file_types` supported
                         by this plugin.
        :param log: A :class:`calibre.utils.logging.Log` object. All output
                    should use this object.
        :param accelerators: A dictionary of various information that the input
                             plugin can get easily that would speed up the
                             subsequent stages of the conversion.

        """
        raise NotImplementedError

    def __call__(self, stream, options, file_ext, log, accelerators, output_dir):
        """
        Calls convert with the stream after changing the current working dir to the output_dir.

        :param stream:
        :param options:
        :param file_ext:
        :param log:
        :param accelerators:
        :param output_dir:
        :return:
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

        :param oeb:
        :param opts:
        :param log:
        :return:
        """
        pass

    def specialize(self, oeb, opts, log, output_fmt):
        """
        Called to allow the input plugin to specialize the parsed book for a particular output format.

        Called after postprocess_book and before any transforms are performed on the parsed book.
        :param oeb: The OEBBook for manipulation
        :param opts: Input options
        :param log:
        :param output_fmt: The output format the specification is occurring for
        :return:
        """
        pass

    def gui_configuration_widget(self, parent, get_option_by_name, get_option_help, db, book_id=None):
        """
        Called to create the widget used for configuring this plugin in the calibre GUI.

        The widget must be an instance of the PluginWidget class.
        See the builting input plugins for examples.
        :param parent:
        :param get_option_by_name:
        :param get_option_help:
        :param db:
        :param book_id:
        :return:
        """
        raise NotImplementedError(
            "This is a interface problem. " "The code for this has been moved to LiuXin.interfaces.gui_common.customize"
        )


class OutputFormatPlugin(Plugin):
    """
    OutputFormatPlugins are responsible for converting an OEB document (OPF+HTML) into an output ebook.

    The OEB document can be assumed to be encoded in UTF-8.
    The main action happens in :meth:`convert`.
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

        :return:
        """
        return _("Convert ebooks to the %s format") % self.file_type

    def __init__(self, *args):
        Plugin.__init__(self, *args)
        self.report_progress = DummyReporter()

    def convert(self, oeb_book, output, input_plugin, opts, log):
        """
        Render the contents of `oeb_book` (an instance of :class:`LiuXin.file_formats.oeb.OEBBook`) to the output.

        :param oeb_book:
        :param output: Either a file like object or a string. If it is a string
                       it is the path to a directory that may or may not exist. The output
                       plugin should write its output into that directory. If it is a file like
                       object, the output plugin should write its output into the file.
        :param input_plugin: The input plugin that was used at the beginning of
                             the conversion pipeline.
        :param opts: Conversion options. Guaranteed to have attributes
                     corresponding to the OptionRecommendations of this plugin.
        :param log: The logger. Print debug/info messages etc. using this.

        """
        raise NotImplementedError

    @property
    def is_periodical(self):
        """
        Is the file registered as a periodical?

        :return:
        """
        return self.oeb.metadata.publication_type and six_unicode(self.oeb.metadata.publication_type[0]).startswith(
            "periodical:"
        )

    def specialize_css_for_output(self, log, opts, item, stylizer):
        """
        Can be used to make changes to the css during the CSS flattening process.

        :param item: The item (HTML file) being processed
        :param stylizer: A Stylizer object containing the flattened styles for item.
                         You can get the style for any element by stylizer.style(element).

        """
        pass

    def gui_configuration_widget(self, parent, get_option_by_name, get_option_help, db, book_id=None):
        """
        Called to create the widget used for configuring this plugin in the calibre GUI.

        The widget must be an instance of the PluginWidget class.
        See the builtin output plugins for examples.
        :param parent:
        :param get_option_by_name:
        :param get_option_help:
        :param db:
        :param book_id:
        :return:
        """
        raise NotImplementedError("Method logic has been moved to LiuXin.interface.gui_common.customize")


# ----------------------------------------------------------------------------------------------------------------------
#
# - LIUXIN PLUGINS START HERE
#
# ----------------------------------------------------------------------------------------------------------------------
