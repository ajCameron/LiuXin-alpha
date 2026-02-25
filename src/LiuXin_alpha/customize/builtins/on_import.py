"""
Plugins which are run on certain file types whenever they are imported.

E.g. Used to bundle html files and all their resources into a single zip file.
"""


import re
import os
import glob

from typing import Type

from LiuXin_alpha.customize import FileTypePlugin

from LiuXin_alpha.utils.calibre import guess_type
from LiuXin_alpha.utils.calibre.constants import numeric_version
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logger import default_log


file_type_plugins: list[Type[FileTypePlugin]] = []


try:
    import zipfile
except AttributeError as e:
    debug_str = "Cannot initialize PML2PMLZ - zipfile couldn't be loaded."
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    default_log.info("PML2PMLZ components imported successfully")

    class PML2PMLZ(FileTypePlugin):
        """
        Constructs a PMLZ (a zipped file containing a PML file and all linked resources.
        """

        name = "PML to PMLZ"
        author = "John Schember"
        description = _(
            "Create a PMLZ archive containing the PML file "
            "and all images in the directory pmlname_img or images. "
            "This plugin is run every time you add "
            "a PML file to the library."
        )
        version = numeric_version
        file_types = {"pml"}
        supported_platforms = ["windows", "osx", "linux"]
        on_import = True

        def run(self, pmlfile):
            """
            Returns a zipped PML file containing all the resources of the PML file.

            :param pmlfile:
            :return file_path: ... I think? A path to the temporary file where the processed file is being stored.
            """
            of = self.temporary_file("_plugin_pml2pmlz.pmlz")
            pmlz = zipfile.ZipFile(of.name, "w")
            pmlz.write(pmlfile, os.path.basename(pmlfile), zipfile.ZIP_DEFLATED)

            pml_img = os.path.splitext(pmlfile)[0] + "_img"
            i_img = os.path.join(os.path.dirname(pmlfile), "images")
            img_dir = pml_img if os.path.isdir(pml_img) else i_img if os.path.isdir(i_img) else ""
            if img_dir:
                for image in glob.glob(os.path.join(img_dir, "*.png")):
                    pmlz.write(image, os.path.join("images", (os.path.basename(image))))
            pmlz.close()

            return of.name

    file_type_plugins += [PML2PMLZ]

default_log.info("About to try importing the components for TXT2TXTZ")
# Tests all the required imports - if they work, import the plugin and add the now supported plugins
try:
    from LiuXin_alpha.metadata.file_sources.txt import get_metadata
    from LiuXin_alpha.file_formats.oeb.base import OEB_IMAGES
    from LiuXin_alpha.file_formats.opf.opf2 import metadata_to_opf
except ImportError as e:
    wrn_str = (
        "Couldn't import the required plugins to support TXT2TXZ - Automatic txt & text conversion to txtz is "
        "not enabled"
    )
    default_log.log_exception(wrn_str, e, "DEBUG")
except AttributeError as e:
    # If _icu has not been properly implemented then the module will not function.
    # If this is going to happen better it never be included in the first place.
    wrn_str = (
        "Couldn't import the required plugins to support TXT2TXZ - Automatic txt & text conversion to txtz is "
        "not enabled - Attribute Error"
    )
    default_log.log_exception(wrn_str, e, "DEBUG")
except Exception as e:
    wrn_str = (
        "Couldn't import the required plugins to support TXT2TXZ - Automatic txt & text conversion to txtz is "
        "not enabled - some other kind of error"
    )
    default_log.log_exception(wrn_str, e, "DEBUG")
else:
    default_log.info("TXT2TXTZ components imported successfully")

    class TXT2TXTZ(FileTypePlugin):
        """
        Takes a txt file and zipes it all linked resources.
        """

        name = "TXT to TXTZ"
        author = "John Schember"
        description = _(
            "Create a TXTZ archive when a TXT file is imported "
            "containing Markdown or Textile references to images. The referenced "
            "images as well as the TXT file are added to the archive."
        )
        version = numeric_version
        file_types = {"txt", "text"}
        supported_platforms = ["windows", "osx", "linux"]
        on_import = True

        @staticmethod
        def _get_image_references(txt: str, base_dir: str) -> list[str]:
            """
            Returns a list of all the images linked to in the base txt file.

            :param txt:
            :param base_dir:
            :return:
            """

            images = []

            # Textile
            for m in re.finditer(
                r"(?mu)(?:[\[{])?\!(?:\. )?(?P<path>[^\s(!]+)\s?(?:\(([^\)]+)\))?\!(?::(\S+))?" r"(?:[\]}]|(?=\s|$))",
                txt,
            ):
                path = m.group("path")
                if (
                    path
                    and not os.path.isabs(path)
                    and guess_type(path)[0] in OEB_IMAGES
                    and os.path.exists(os.path.join(base_dir, path))
                ):
                    images.append(path)

            # Markdown inline
            for m in re.finditer(
                r"(?mu)\!\[([^\]\[]*(\[[^\]\[]*(\[[^\]\[]*(\[[^\]\[]*(\[[^\]\[]*(\[[^\]\[]*"
                r"(\[[^\]\[]*\])*[^\]\[]*\])*[^\]\[]*\])*[^\]\[]*\])*[^\]\[]*\])*[^\]\[]*\])*"
                r"[^\]\[]*)\]\s*\((?P<path>[^\)]*)\)",
                txt,
            ):  # noqa - what?
                path = m.group("path")
                if (
                    path
                    and not os.path.isabs(path)
                    and guess_type(path)[0] in OEB_IMAGES
                    and os.path.exists(os.path.join(base_dir, path))
                ):
                    images.append(path)

            # Markdown reference
            refs = {}
            for m in re.finditer(r"(?mu)^(\ ?\ ?\ ?)\[(?P<id>[^\]]*)\]:\s*(?P<path>[^\s]*)$", txt):
                if m.group("id") and m.group("path"):
                    refs[m.group("id")] = m.group("path")
            for m in re.finditer(
                r"(?mu)\!\[([^\]\[]*(\[[^\]\[]*(\[[^\]\[]*(\[[^\]\[]*(\[[^\]\[]*"
                r"(\[[^\]\[]*(\[[^\]\[]*\])*[^\]\[]*\])*[^\]\[]*\])*[^\]\[]*\])*"
                r"[^\]\[]*\])*[^\]\[]*\])*[^\]\[]*)\]\s*\[(?P<id>[^\]]*)\]",
                txt,
            ):  # noqa
                path = refs.get(m.group("id"), None)
                if (
                    path
                    and not os.path.isabs(path)
                    and guess_type(path)[0] in OEB_IMAGES
                    and os.path.exists(os.path.join(base_dir, path))
                ):
                    images.append(path)

            # Remove duplicates
            return list(set(images))

        def run(self, path_to_ebook: str) -> str:
            """
            Preforms a conversion - turning a txt file into a txtz.

            :param path_to_ebook:
            :return:
            """

            with open(path_to_ebook, "rb") as ebf:
                txt = ebf.read()

            base_dir = os.path.dirname(path_to_ebook)
            images = self._get_image_references(txt, base_dir)

            if images:
                # Create TXTZ and put file plus images inside it.
                import zipfile

                of = self.temporary_file("_plugin_txt2txtz.txtz")
                txtz = zipfile.ZipFile(of.name, "w")
                # Add selected TXT file to archive.
                txtz.write(path_to_ebook, os.path.basename(path_to_ebook), zipfile.ZIP_DEFLATED)

                # metadata.opf
                if os.path.exists(os.path.join(base_dir, "metadata.opf")):
                    txtz.write(
                        os.path.join(base_dir, "metadata.opf"),
                        "metadata.opf",
                        zipfile.ZIP_DEFLATED,
                    )
                else:
                    with open(path_to_ebook, "rb") as ebf:
                        mi = get_metadata(ebf)
                    opf = metadata_to_opf(mi)
                    txtz.writestr("metadata.opf", opf, zipfile.ZIP_DEFLATED)

                # images
                for image in images:
                    txtz.write(os.path.join(base_dir, image), image)

                txtz.close()

                return of.name
            else:
                # No images so just import the TXT file.
                return path_to_ebook

    file_type_plugins += [TXT2TXTZ]


def get_file_type_plugins():
    """
    Return all the file type plugins loaded in this context.

    These are plugins intended to be run at the import phase of running a book.
    (Turns a html file into a compressed archive including all the assets referenced by the web page, for example.
    :return:
    """
    return file_type_plugins
