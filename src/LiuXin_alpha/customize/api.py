
"""
Discover, initialize and expose customization plugins.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise api through a consuming regression::

        python -m pytest -q tests/customize/test_customize_base.py
"""

import abc
import pathlib
from typing import Union, Any, Iterable, Tuple, BinaryIO, Optional, Literal, runtime_checkable, overload, Protocol, runtime_checkable
from types import ModuleType
from collections import namedtuple

from typing import NamedTuple

class CatalogCLIOption(NamedTuple):
    """
    Provide the catalogclioption contract for validated ebook processing.

    Example:
        Exercise CatalogCLIOption through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    option: str
    default: str
    dest: str
    help: str



from typing import TypeVar

class Base:
    """
    Provide the base contract for validated ebook processing.

    Example:
        Exercise Base through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    ...

T = TypeVar("T", bound=Base)


class PluginAPI(abc.ABC):
    """
    API for the basic plugins class.

    Example:
        Exercise PluginAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise PluginAPI.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param plugin_path: Value supplied for plugin path under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.plugin_path = plugin_path

    @abc.abstractmethod
    def initialize(self) -> None:
        """
        Called once when calibre plugins are initialized. Plugins are re-initialized every time a new plugin is added.

        Example:
            Exercise PluginAPI.initialize through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    def config_widget(self):
        """
        Implement this method and :meth:`save_settings` in your plugin to use a custom configuration dialog, rather then relying on the simple string based default customization.

        Example:
            Exercise PluginAPI.config widget through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError()

    def save_settings(self, config_widget):
        """
        Save the settings specified by the user with config_widget.

        Example:
            Exercise PluginAPI.save settings through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param config_widget: Value supplied for config widget under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError()

    def do_user_config(self, parent=None):
        """
        This method shows a configuration dialog for this plugin.

        Example:
            Exercise PluginAPI.do user config through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def pyqt5_do_user_config(self, parent=None):
        """
        Allows the plugin to be configured in a PyQt5 environment.

        Example:
            Exercise PluginAPI.pyqt5 do user config through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    def command_line_do_user_config(self, parent=None):
        """
        Allows the plugin to be configured in a command line environment.

        Example:
            Exercise PluginAPI.command line do user config through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def load_resources(self, names: list[str]) -> dict[str, bytes]:
        """
        If this plugin comes in a ZIP file (user added plugin), this method will allow you to load resources from the ZIP file.

        Example:
            Exercise PluginAPI.load resources through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param names: Value supplied for names under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    def customization_help(self, gui: bool = False) -> str:
        """
        Return a string giving help on how to customize this plugin.

        Example:
            Exercise PluginAPI.customization help through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param gui: Value supplied for gui under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def temporary_file(suffix: str):
        """
        Return a file-like object that is a temporary file on the file system.

        Example:
            Exercise PluginAPI.temporary file through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param suffix: Text appended to the formatted or selected result.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def is_customizable(self) -> bool:
        """
        Can the plugin be customized?

        Example:
            Exercise PluginAPI.is customizable through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: True when the documented condition holds; otherwise False.
        """

    @abc.abstractmethod
    def __enter__(self, *args: Any, **kwargs: Any) -> None:
        """
        Add this plugin to the python path so that it's contents become directly importable.

        Example:
            Exercise PluginAPI.  enter   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def __exit__(self, *args: Any) -> None:
        """
        Remove the previously added paths.

        Example:
            Exercise PluginAPI.  exit   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def cli_main(self, args: Iterable[str]) -> None:
        """
        This method is the main entry point for your plugins command line interface.

        Example:
            Exercise PluginAPI.cli main through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """


class FileTypePluginAPI(PluginAPI):
    """
    A plugin transforms a particular set of file types.

    Example:
        Exercise FileTypePluginAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise FileTypePluginAPI.run through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param path_to_ebook: Value supplied for path to ebook under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def postimport(self, book_id: int, book_format: str, db) -> None:
        """
        Called post import, i.e., after the book file has been added to the database.

        Example:
            Exercise FileTypePluginAPI.postimport through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param book_id: Value supplied for book id under the utility contract.
        :param book_format: Value supplied for book format under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """


class _MetadataReaderPluginAPI:
    """
    We want to implement a very similar interface without cross-pollinating the class hierarchy.

    Example:
        Exercise  MetadataReaderPluginAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise  MetadataReaderPluginAPI.get metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param ftype: Value supplied for ftype under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def get_metadata_inplace(self, file_path: Union[pathlib.Path, str], ftype: str):
        """
        Returns metadata for the file represented by the file path.

        Example:
            Exercise  MetadataReaderPluginAPI.get metadata inplace through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param file_path: Value supplied for file path under the utility contract.
        :param ftype: Value supplied for ftype under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """


class MetadataReaderPluginAPI(PluginAPI, _MetadataReaderPluginAPI):
    """
    A plugin which implements reading metadata from a set of file types.

    Example:
        Exercise MetadataReaderPluginAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """


class MetadataWriterPluginAPI(PluginAPI):
    """
    A plugin that implements writing metadata to files in a certain set of file types.

    Example:
        Exercise MetadataWriterPluginAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise MetadataWriterPluginAPI.set metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param mi: Metadata object exposed to the template function.
        :param type: Value supplied for type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def set_metadata_inplace(self, file_path: Union[str, pathlib.Path], mi, type: str) -> None:
        """
        Set metadata for the file pointed to by the file path.

        Example:
            Exercise MetadataWriterPluginAPI.set metadata inplace through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param file_path: Value supplied for file path under the utility contract.
        :param mi: Metadata object exposed to the template function.
        :param type: Value supplied for type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """


class CatalogPluginAPI(PluginAPI):
    """
    A plugin that implements a catalog generator.

    Example:
        Exercise CatalogPluginAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise CatalogPluginAPI. field sorter through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param key: Metadata, identifier or local-variable key.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def search_sort_db(self, db, opts):
        """
        Generate a catalog off a db search.

        Example:
            Exercise CatalogPluginAPI.search sort db through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def get_output_fields(self, db, opts) -> list[str]:
        """
        Returns a list of the requested fields.

        Example:
            Exercise CatalogPluginAPI.get output fields through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def initialize(self) -> None:
        """
        If plugin is not a built-in, copy the plugin's .ui and .py files from the zip file to $TMPDIR.

        Example:
            Exercise CatalogPluginAPI.initialize through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def run(self,
            path_to_output: Union[str, pathlib.Path],
            opts,
            db,
            ids: Iterable[str],
            notification = None) -> None:
        """
        Run the plugin. Must be implemented in subclasses. It should generate the catalog in the format specified in file_types, returning the absolute path to the generated catalog file. If an error is encountered it should raise an Exception.

        Example:
            Exercise CatalogPluginAPI.run through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param path_to_output: Value supplied for path to output under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :param ids: Value supplied for ids under the utility contract.
        :param notification: Value supplied for notification under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """


class StoreBaseAPI(PluginAPI):
    """
    Interface to an ebook store to allow buying books from within calibre.

    Example:
        Exercise StoreBaseAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
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

        Example:
            Exercise StoreBaseAPI.load actual plugin through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param gui: Value supplied for gui under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def customization_help(self, gui: bool = False) -> None:
        """
        Help with customizing the store.

        Example:
            Exercise StoreBaseAPI.customization help through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param gui: Value supplied for gui under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def config_widget(self) -> Any:
        """
        Provides a config widget to config the store plugin.

        Example:
            Exercise StoreBaseAPI.config widget through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def save_settings(self, config_widget: Any) -> None:
        """
        Save setting changes made with the config_widget.

        Example:
            Exercise StoreBaseAPI.save settings through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param config_widget: Value supplied for config widget under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """


class ViewerPluginAPI(PluginAPI):
    """
    These plugins are used to add functionality to the calibre viewer.

    Example:
        Exercise ViewerPluginAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """

    plugin_type: str

    @abc.abstractmethod
    def load_fonts(self) -> None:
        """
        This method is called once at viewer startup. It should load any fonts it wants to make available. For example::

        Example:
            Exercise ViewerPluginAPI.load fonts through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def load_javascript(self, evaljs):
        """
        This method is called every time a new HTML document is loaded in the viewer. Use it to load javascript libraries into the viewer. For example::

        Example:
            Exercise ViewerPluginAPI.load javascript through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param evaljs: Value supplied for evaljs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def run_javascript(self, evaljs):
        """
        This method is called every time a document has finished loading. Use it in the same way as load_javascript().

        Example:
            Exercise ViewerPluginAPI.run javascript through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param evaljs: Value supplied for evaljs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def customize_ui(self, ui):
        """
        This method is called once when the viewer is created. Use it to make any customizations you want to the viewer's user interface. For example, you can modify the toolbars via ui.tool_bar and ui.tool_bar2.

        Example:
            Exercise ViewerPluginAPI.customize ui through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param ui: Value supplied for ui under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def customize_context_menu(self, menu, event, hit_test_result):
        """
        This method is called every time the context (right-click) menu is shown. You can use it to customize the context menu. ``event`` is the context menu event and hit_test_result is the QWebHitTestResult for this event in the currently loaded document.

        Example:
            Exercise ViewerPluginAPI.customize context menu through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param menu: Value supplied for menu under the utility contract.
        :param event: Value supplied for event under the utility contract.
        :param hit_test_result: Value supplied for hit test result under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """


class LibraryClosedPluginAPI(PluginAPI):
    """
    LibraryClosedPlugins are run when a library is closed, either at shutdown, when the library is changed, or when a library is used in some other way. At the moment these plugins won't be called by the CLI functions.

    Example:
        Exercise LibraryClosedPluginAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    plugin_type: str

    minimum_calibre_version: tuple[int, int, int]

    @abc.abstractmethod
    def run(self, db) -> None:
        """
        The db will be a reference to the new_api (db.cache.py).

        Example:
            Exercise LibraryClosedPluginAPI.run through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """


class EditBookToolPluginAPI(PluginAPI):
    """
    Tools to edit a book.

    Example:
        Exercise EditBookToolPluginAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    plugin_type: str

    minimum_calibre_version: tuple[int, int, int]


# ------------------------------------
#
# - LIUXIN SPECIFIC PLUGINS START HERE


class LiuXinPluginAPI(PluginAPI):
    """
    Base class for all LiuXin specific plugins.

    Example:
        Exercise LiuXinPluginAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """


class MDInputTransformAPI(LiuXinPluginAPI):
    """
    Base class for the MetaData Input Transformation plugins.

    Example:
        Exercise MDInputTransformAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    target_classes: list[Any]

    @abc.abstractmethod
    def transform_metadata(self, first: T, /, *rest: T) -> T:
        """
        Takes a collection of MetaData objects. Uses them to preform a transform. Returns the transformed MetaData.

        Example:
            Exercise MDInputTransformAPI.transform metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param first: Value supplied for first under the utility contract.
        :param rest: Value supplied for rest under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def _true_transform_metadata(self, first: T, /, *rest: T) -> T:
        """
        Mostly needed for typing.

        Example:
            Exercise MDInputTransformAPI. true transform metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param first: Value supplied for first under the utility contract.
        :param rest: Value supplied for rest under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """


class LXMetadataReaderAPI(LiuXinPluginAPI, _MetadataReaderPluginAPI):
    """
    To distinguish the calibre metadata readers from the ones which have been re-written for LiuXin.

    Example:
        Exercise LXMetadataReaderAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    # All file formats this plugin could be used for
    valid_for: Optional[set[str]]

    # The file formats this plugin SHOULD be used for
    priority_for: Optional[set[str]]

    # Costs of actually running the
    run_cost: Literal["low", "medium", "high"]

    quick: bool

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Startup the plugin.

        Example:
            Exercise LXMetadataReaderAPI.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; validated state is stored on the receiving object.
        """
        LiuXinPluginAPI.__init__(self, *args, **kwargs)

    @staticmethod
    @abc.abstractmethod
    def standardize_type(file_type: str) -> str:
        """
        Standardizes a plugin_type so that it can be compared against the known types that the plugin can be run for.

        Example:
            Exercise LXMetadataReaderAPI.standardize type through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param file_type: Value supplied for file type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def get_metadata(self, stream: Union[BinaryIO, str, pathlib.Path], file_type: str):
        """
        Return metadata for the file represented by stream or path.

        Example:
            Exercise LXMetadataReaderAPI.get metadata through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param file_type: Value supplied for file type under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """


@runtime_checkable
class ZipInfoLike(Protocol):
    # --- core identity / metadata ---
    """
    Provide the zipinfolike contract for validated ebook processing.

    Example:
        Exercise ZipInfoLike through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    filename: str
    date_time: Tuple[int, int, int, int, int, int]  # (Y, M, D, h, m, s)
    compress_type: int
    comment: bytes
    extra: bytes

    # --- sizes / CRC ---
    file_size: int
    compress_size: int
    CRC: int

    # --- flags / versions ---
    flag_bits: int
    create_system: int
    create_version: int
    extract_version: int
    reserved: int

    # --- perms / attributes ---
    internal_attr: int
    external_attr: int

    # --- offsets ---
    header_offset: int

    # --- extra fields often present on ZipInfo ---
    volume: int

    # --- methods ZipInfo provides ---
    def is_dir(self) -> bool:
        """
        Return whether is dir holds for the supplied ebook data.

        Example:
            Exercise ZipInfoLike.is dir through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: True when the documented condition holds; otherwise False.
        """
        ...
    def FileHeader(self, zip64: Optional[bool] = None) -> bytes:
        """
        Perform the FileHeader operation under explicit file-format and conversion rules.

        Example:
            Exercise ZipInfoLike.FileHeader through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param zip64: Value supplied for zip64 under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    # Not always needed, but commonly used for display/logging.
    def __repr__(self) -> str:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise ZipInfoLike.  repr   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...



class ArchiveAPI(LiuXinPluginAPI):
    """
    Provides a zipfile like read interface to a compressed file format.

    Example:
        Exercise ArchiveAPI through a consuming regression::

            python -m pytest -q tests/customize/test_customize_base.py
    """
    # This plugin can read from these formats
    read_formats: frozenset[str]

    # This plugin can write to these formats
    write_formats: frozenset[str]

    # If the plugin supports multiple write types, which one should be used by default?
    default_write_type: str

    # Properties of the archive

    # - on disk
    file_path: str
    file_name: str
    file_ext: str
    file_name: str

    # - archive itself
    mode: Literal["a", "w", "r"]
    compression_flags: Any
    write_type: Literal["a", "w", "r"]

    # - Properties of the file in the archive
    compression_type: Optional[Any]
    block_count: Optional[int]
    physical_size: Optional[int]
    final_size: Optional[int]

    multivolume: str
    password: str

    @abc.abstractmethod
    def __init__(self,
                 file_path: Union[pathlib.Path, str],
                 *,
                 mode: Literal["r", "w", "a"],
                 compression_flags=None,
                 write_type: Optional[str] = None,
                 password: str) -> None:
        """
        Initialize an object representing the compressed file.

        Example:
            Exercise ArchiveAPI.  init   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param file_path: Value supplied for file path under the utility contract.
        :param mode: Open or adapter mode controlling read/write behavior.
        :param compression_flags: Value supplied for compression flags under the utility
            contract.
        :param write_type: Value supplied for write type under the utility contract.
        :param password: Value supplied for password under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super().__init__(plugin_path="builtin")

        self.file_path = file_path

        self.mode = mode

        self.compression_flags = compression_flags

        self.write_type = write_type

        self.password = password

    @abc.abstractmethod
    def __str__(self) -> str:
        """
        Returns a string representation of the object

        Example:
            Exercise ArchiveAPI.  str   through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def printdir(self) -> None:
        """
        Prints a contents of the archive to sys.stdout.

        Example:
            Exercise ArchiveAPI.printdir through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @classmethod
    @abc.abstractmethod
    def is_valid(cls, path: Union[str, pathlib.Path]) -> bool:
        """
        Takes a local path - determines if the file can be read by this class.

        Example:
            Exercise ArchiveAPI.is valid through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: True when the documented condition holds; otherwise False.
        """

    @abc.abstractmethod
    def getinfo(self, name: str) -> ZipInfoLike:
        """
        Return info on an element in the archive.

        Example:
            Exercise ArchiveAPI.getinfo through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def infolist(self) -> list[ZipInfoLike]:
        """
        Returns a list containing an info object for every element.

        Example:
            Exercise ArchiveAPI.infolist through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def namelist(self) -> list[str]:
        """
        Returns a list of all members of the archive by name.

        Example:
            Exercise ArchiveAPI.namelist through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @property
    @abc.abstractmethod
    def files(self) -> Iterable[str]:
        """
        Returns all the files in the archive.

        Example:
            Exercise ArchiveAPI.files through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @property
    @abc.abstractmethod
    def folders(self) -> Iterable[str]:
        """
        Returns all the folders in the archive.

        Example:
            Exercise ArchiveAPI.folders through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

    @abc.abstractmethod
    def extract(self,
                path: Union[str, pathlib.Path],
                pwd: str,
                member: Union[str, ZipInfoLike]) -> pathlib.Path:
        """
        Extract a member of the archive.

        Example:
            Exercise ArchiveAPI.extract through a consuming regression::

                python -m pytest -q tests/customize/test_customize_base.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param pwd: Value supplied for pwd under the utility contract.
        :param member: Value supplied for member under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
