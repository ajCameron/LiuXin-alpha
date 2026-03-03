
"""
The API for the customize class - which serves as the basic API for the plugin classes.
"""

import abc
import pathlib
from typing import Union, Any, Iterable, Tuple, BinaryIO, Optional
from types import ModuleType
from collections import namedtuple

from typing import NamedTuple

class CatalogCLIOption(NamedTuple):
    option: str
    default: str
    dest: str
    help: str


class PluginAPI(abc.ABC):
    """
    API for the basic plugins class.
    """

    supported_platforms: list[str]

    name: str

    version: tuple[int, int, int]

    description: str

    author: str

    priority: int

    minimum_calibre_version: tuple[int, int, int]

    can_be_disabled: bool

    plugin_type: str

    plugin_path: Union[str, pathlib.Path]

    def __init__(self, plugin_path: Union[str, pathlib.Path]) -> None:
        """
        Startup the plugin.

        :param plugin_path:
        """
        self.plugin_path = plugin_path

    @abc.abstractmethod
    def initialize(self) -> None:
        """
        Called once when calibre plugins are initialized. Plugins are re-initialized
        every time a new plugin is added.

        Perform any plugin specific initialization here, such as extracting
        resources from the plugin zip file. The path to the zip file is
        available as ``self.plugin_path``.

        Note that ``self.site_customization`` is **not** available at this point.
        """

    def config_widget(self):
        """
        Implement this method and :meth:`save_settings` in your plugin to use a custom configuration dialog, rather then
        relying on the simple string based default customization.

        This method, if implemented, must return a QWidget. The widget can have an optional method validate() that takes
        no arguments and is called immediately after the user clicks OK. Changes are applied if and only if the method
        returns True.

        If for some reason you cannot perform the configuration at this time, return a tuple of two strings (message,
        details), these will be displayed as a warning dialog to the user and the process will be aborted.
        """
        raise NotImplementedError()

    def save_settings(self, config_widget):
        """
        Save the settings specified by the user with config_widget.

        :param config_widget: The widget returned by :meth:`config_widget`.

        """
        raise NotImplementedError()

    def do_user_config(self, parent=None):
        """
        This method shows a configuration dialog for this plugin.

        It returns True if the user clicks OK, False otherwise.
        The changes are automatically applied - if you use the default logic.
        If you don't, it's up to you.

        To preserve separation between core and interface logic, the code for this has been moved to
        `LiuXin.interfaces.gui_common.plugin_user_config`.
        Call the method `do_calibre_plugin_config` there with this plugin as the first argument and the parent as the
        second.
        This will invoke the defaut configuration logic for calibre.

        :param parent: The parent window for the widget

        """
        raise NotImplementedError

    def pyqt5_do_user_config(self, parent=None):
        """
        Allows the plugin to be configured in a PyQt5 environment.

        There should be one of these methods for each of the generic environments which the interfaces support.

        If you just want a method that returns True if the user clicks OK, False otherwise, use the defalt calibre logic
        by calling the method as specified in `do_user_config`

        The changes should be automatically applied.
        Probably.
        But it is really up to you!

        :param parent:
        :return:
        """
        raise NotImplementedError

    def command_line_do_user_config(self, parent=None):
        """
        Allows the plugin to be configured in a command line environment.

        The changes should be automatically applied.
        But it really is up to you!

        :param parent:
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def load_resources(self, names: list[str]) -> dict[str, bytes]:
        """
        If this plugin comes in a ZIP file (user added plugin), this method will allow you to load resources from the
        ZIP file.

        For example to load an image::

            pixmap = QPixmap()
            pixmap.loadFromData(self.load_resources(['images/icon.png']).itervalues().next())
            icon = QIcon(pixmap)

        Resource will be returned as bytes.

        :param names: List of paths to resources in the zip file using / as separator

        :return: A dictionary of the form ``{name: file_contents}``. Any names
                 that were not found in the zip file will not be present in the
                 dictionary.

        """

    def customization_help(self, gui: bool = False) -> str:
        """
        Return a string giving help on how to customize this plugin.

        By default, raise a :class:`NotImplementedError`, which indicates that the plugin does not require
        customization.

        If you re-implement this method in your subclass, the user will be asked to enter a string as customization for
        this plugin. The customization string will be available as ``self.site_customization``.
        Site customization could be anything, for example, the path to a needed binary on the user's computer.

        :param gui: If True return HTML help, otherwise return plain text help.
        """
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def temporary_file(suffix: str):
        """
        Return a file-like object that is a temporary file on the file system.

        This file will remain available even after being closed and will only
        be removed on interpreter shutdown. Use the ``name`` member of the
        returned object to access the full path to the created temporary file.

        :param suffix: The suffix that the temporary file will have.

        """

    @abc.abstractmethod
    def is_customizable(self) -> bool:
        """
        Can the plugin be customized?

        :return:
        """

    @abc.abstractmethod
    def __enter__(self, *args: Any, **kwargs: Any) -> None:
        """
        Add this plugin to the python path so that it's contents become directly importable.

        Useful when bundling large python libraries into the plugin. Use it like this::
            with plugin:
                import something
        Included for legacy compatibility reasons - ideally should never be used.
        """

    @abc.abstractmethod
    def __exit__(self, *args: Any) -> None:
        """
        Remove the previously added paths.

        :param args:
        :return:
        """

    @abc.abstractmethod
    def cli_main(self, args: Iterable[str]) -> None:
        """
        This method is the main entry point for your plugins command line interface.

        It is called when the user does: calibre-debug -r "Plugin Name".
        Any arguments passed are present in the args variable.
        """


class FileTypePluginAPI(PluginAPI):
    """
    A plugin transforms a particular set of file types.
    """
    #: Set of file types for which this plugin should be run. For example: ``{'lit', 'mobi', 'prc'}``
    file_types: set[str]

    #: If True, this plugin is run when books are added to the database
    on_import: bool

    #: If True, this plugin is run after books are added to the database
    on_postimport: bool

    #: If True, this plugin is run just before a conversion
    on_preprocess: bool

    #: If True, this plugin is run after conversion on the final file produced by the conversion output plugin.
    on_postprocess: bool

    plugins_type: str

    @abc.abstractmethod
    def run(self, path_to_ebook: str) -> str:
        """
        Run the plugin. Must be implemented in subclasses.

        It should perform whatever modifications are required on the ebook and return the absolute path to the modified
        ebook. If no modifications are needed, it should return the path to the original ebook. If an error is
        encountered it should raise an Exception. The default implementation simply return the path to the original
        ebook.

        The modified ebook file should be created with the :meth:`temporary_file` method.
        :param path_to_ebook: Absolute path to the ebook.
        :return: Absolute path to the modified ebook.
        """

    @abc.abstractmethod
    def postimport(self, book_id: int, book_format: str, db) -> None:
        """
        Called post import, i.e., after the book file has been added to the database.

        :param book_id: DatabasePing id of the added book.
        :param book_format: The file plugin_type of the book that was added.
        :param db: Library database.
        """


class MetadataReaderPluginAPI(PluginAPI):
    """
    A plugin which impelements reading metadata from a set of file types.
    """
    #: Set of file types for which this plugin should be run. For example: ``set(['lit', 'mobi', 'prc'])``
    file_types: frozenset[str] = frozenset([])

    # Basic measure of run cost
    inplace_run_cost: str = "high"

    # What platforms does this plugin work on?
    supported_platforms: list[str]

    version: tuple[int, int, int]

    author: str

    plugin_type: str

    # Used when determining if to run or not
    quick: bool

    @abc.abstractmethod
    def get_metadata(self, stream: BinaryIO, ftype: str):
        """
        Return metadata for the file represented by stream (a file like object that supports reading).

        Raise an exception when there is an error with the input data.

        :param ftype: The plugin_type of file. Guaranteed to be one of the entries in :attr:`file_types`.
        :return: A :class:`LiuXin.metadata.metadata.MetaData` object
        """

    @abc.abstractmethod
    def get_metadata_inplace(self, file_path: Union[pathlib.Path, str], ftype: str):
        """
        Returns metadata for the file represented by the file path.

        Must be a valid path with read access.
        Raises an exception when there is an error with the input data.
        Sometimes avoids having to copy the entire file into memory.
        :param file_path:
        :param ftype: Guaranteed to be one of the entries in :attr:`file_types`.
        :return: A :class:`LiuXin.metadata.metadata.MetaData` object
        """


class MetadataWriterPluginAPI(PluginAPI):
    """
    A plugin that implements writing metadata to files in a certain set of file types.
    """
    file_types: set[str]

    supported_platforms: list[str]

    version: tuple[int, int, int]

    author: str

    plugin_type: str

    apply_null: bool

    @abc.abstractmethod
    def set_metadata(self, stream: BinaryIO, mi, type: str) -> None:
        """
        Set metadata for the file represented by stream (a file like object that supports reading).

        Raise an exception when there is an error with the input data.
        :param stream: The file to be modified
        :param type: The plugin_type of file. Guaranteed to be one of the entries in :attr:`file_types`.
        :param mi: A :class:`calibre.ebooks.metadata.book.Metadata` object
        """

    @abc.abstractmethod
    def set_metadata_inplace(self, file_path: Union[str, pathlib.Path], mi, type: str) -> None:
        """
        Set metadata for the file pointed to by the file path.

        Means that the file doesn't have to be copied into memory for updating.
        Raise an exception when there is an error with the input data.

        WARNING: THE ACTUAL FILE ON DISK WILL BE MODIFIED.
        PLEASE CONSIDER TAKING A BACKUP FIRST.

        :param file_path: The file to be modified
        :param type: The plugin_type of file. Guaranteed to be one of the entries in :attr:`file_types`.
        :param mi: A :class:`calibre.ebooks.metadata.book.Metadata` object
        """


class CatalogPluginAPI(PluginAPI):
    """
    A plugin that implements a catalog generator.

    Catalogs are files containing catalog entries from the database.
    The default plugin only writes out calibre metadata.
    You want a LiuXinCatalogPlugin if you want the full LiuXin metadata.
    """
    resources_path: Optional[Union[str, pathlib.Path]]

    #: Output file plugin_type this generator can produce
    #: For example: 'epub' or 'xml'
    file_types: set[str]

    plugin_type: str

    cli_options: list[CatalogCLIOption]

    @abc.abstractmethod
    def _field_sorter(self, key: str) -> str:
        """
        Custom fields sort after standard fields.
        """

    @abc.abstractmethod
    def search_sort_db(self, db, opts):
        """
        Generate a catalog off a db search.

        Returns the data as a dict for writing out.
        :param db:
        :param opts:
        :return:
        """

    @abc.abstractmethod
    def get_output_fields(self, db, opts) -> list[str]:
        """
        Returns a list of the requested fields.

        :param db:
        :param opts:
        :return:
        """

    @abc.abstractmethod
    def initialize(self) -> None:
        """
        If plugin is not a built-in, copy the plugin's .ui and .py files from the zip file to $TMPDIR.

        Tab will be dynamically generated and added to the Catalog Options dialog in
        calibre.gui2.dialogs.catalog.py:Catalog
        """

    @abc.abstractmethod
    def run(self,
            path_to_output: Union[str, pathlib.Path],
            opts,
            db,
            ids: Iterable[str],
            notification = None) -> None:
        """
        Run the plugin. Must be implemented in subclasses.
        It should generate the catalog in the format specified in file_types, returning the absolute path to the
        generated catalog file. If an error is encountered it should raise an Exception.

        The generated catalog file should be created with the :meth:`temporary_file` method.

        :param path_to_output: Absolute path to the generated catalog file.
        :param opts: A dictionary of keyword arguments
        :param db: A LibraryDatabase2 object
        :param ids:
        :param notification: Callback to indicate progress.
        """


class StoreBaseAPI(PluginAPI):
    """
    Interface to an ebook store to allow buying books from within calibre.
    """
    # Plugins this store will run for
    supported_platforms: list[str]

    author: str

    plugin_type: str

    # Information about the store. Should be in the primary language
    # of the store. This should not be translatable when set by
    # a subclass.
    description: str

    # Minimum calibre version for the plugin to run
    minimum_calibre_version: tuple[int, int, int]

    # Plugin version
    version: tuple[int, int, int]

    actual_plugin: Optional[ModuleType]

    # Does the store only distribute ebooks without DRM.
    drm_free_only: bool

    # This is the 2-letter country code for the corporate headquarters of the store.
    headquarters: str

    # All formats the store distributes ebooks in.
    formats: list[str]

    # Is this store on an affiliate program?
    affiliate: bool

    @abc.abstractmethod
    def load_actual_plugin(self, gui: Any) -> ModuleType:
        """
        This method must return the actual interface action plugin object.

        :param gui:
        :return:
        """

    @abc.abstractmethod
    def customization_help(self, gui: bool = False) -> None:
        """
        Help with customizing the store.

        :param gui:
        :return:
        """

    @abc.abstractmethod
    def config_widget(self) -> Any:
        """
        Provides a config widget to config the store plugin.

        :return:
        """

    @abc.abstractmethod
    def save_settings(self, config_widget: Any) -> None:
        """
        Save setting changes made with the config_widget.

        :param config_widget:
        :return:
        """


class ViewerPluginAPI(PluginAPI):
    """
    These plugins are used to add functionality to the calibre viewer.
    """

    plugin_type: str

    @abc.abstractmethod
    def load_fonts(self) -> None:
        """
        This method is called once at viewer startup. It should load any fonts it wants to make available. For example::

            def load_fonts():
                from PyQt5.Qt import QFontDatabase
                font_data = get_resources(['myfont1.ttf', 'myfont2.ttf'])
                for raw in font_data.itervalues():
                    QFontDatabase.addApplicationFontFromData(raw)
        """

    @abc.abstractmethod
    def load_javascript(self, evaljs):
        """
        This method is called every time a new HTML document is loaded in the viewer. Use it to load javascript
        libraries into the viewer. For example::

            def load_javascript(self, evaljs):
                js = get_resources('myjavascript.js')
                evaljs(js)
        :param evaljs:
        :return:
        """

    @abc.abstractmethod
    def run_javascript(self, evaljs):
        """
        This method is called every time a document has finished loading. Use it in the same way as load_javascript().

        :param evaljs:
        :return:
        """

    @abc.abstractmethod
    def customize_ui(self, ui):
        """
        This method is called once when the viewer is created. Use it to make any customizations you want to the
        viewer's user interface. For example, you can modify the toolbars via ui.tool_bar and ui.tool_bar2.

        :param ui:
        :return:
        """

    @abc.abstractmethod
    def customize_context_menu(self, menu, event, hit_test_result):
        """
        This method is called every time the context (right-click) menu is shown. You can use it to customize the
        context menu. ``event`` is the context menu event and hit_test_result is the QWebHitTestResult for this event
        in the currently loaded document.
        :param menu:
        :param event:
        :param hit_test_result:
        :return:
        """


class LibraryClosedPluginAPI(PluginAPI):
    """
    LibraryClosedPlugins are run when a library is closed, either at shutdown, when the library is changed, or when a
    library is used in some other way.
    At the moment these plugins won't be called by the CLI functions.
    """
    plugin_type: str

    minimum_calibre_version: tuple[int, int, int]

    @abc.abstractmethod
    def run(self, db) -> None:
        """
        The db will be a reference to the new_api (db.cache.py).

        The plugin must run to completion.
        It must not use the GUI, threads, or any signals.
        """


class EditBookToolPluginAPI(PluginAPI):
    """
    Tools to edit a book.
    """
    plugin_type: str

    minimum_calibre_version: tuple[int, int, int]


