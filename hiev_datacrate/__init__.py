import os
import json
import shutil
from hiev_datacrate.utils import *
from datetime import datetime


BAGIT_VERSION = "0.97"
TAGFILE_CHARACTER_ENCODING = "UTF-8"


class DataCrate:
    """ A DataCrate Class """
    def __init__(self, catalog, files, crate_name=None):
        self.catalog = catalog
        self.files = files
        self.crate_name = None
        self.crate_path = None
        self.datadir_path = None
        self.bagit_version = BAGIT_VERSION
        self.tagfile_char_encoding = TAGFILE_CHARACTER_ENCODING

        # Create the basic structure of a DataCrate
        if crate_name:
            if not os.path.isdir(os.path.join(os.getcwd(), crate_name)):
                self.crate_path = os.path.join(os.getcwd(), crate_name)
                self.datadir_path = os.path.join(self.crate_path, 'data')
                os.makedirs(self.crate_path)
                os.makedirs(os.path.join(self.datadir_path))
            else:
                print('This datacrate already exists')
        else:
            crate_name = datetime.now().strftime('%Y%m%d%H%M%S')
            self.crate_path = os.path.join(os.getcwd(), crate_name)
            self.datadir_path = os.path.join(self.crate_path, 'data')
            os.makedirs(self.crate_path)
            os.makedirs(self.datadir_path)

    def set_catalog(self, catalog):
        self.catalog = catalog

    def set_filegroup(self, filegroup):
        self.filegroup = filegroup

    def validate(self):
        if self.catalog is None:
            return 'Invalid - This datacrate does not contain a Catalogue'
        return True

    def export_bt(self):
        """ export bagit details to bagit.txt file"""
        file_path = os.path.join(self.crate_path, 'bagit.txt')
        with open(file_path, 'w') as file:
            file.write("BagIt-Version: " + self.bagit_version + "\n")
            file.write("Tag-File-Character-Encoding: " + self.tagfile_char_encoding + "\n")

    def ingest_files(self):
        """ Copy files into the data folder"""
        for file in self.filegroup.files:
            shutil.copy(file.orig_path, os.path.join(self.crate_path, 'data'))

    def generate_manifest(self):
        """ Generate manifest of md5 checksums """
        file_path = os.path.join(self.crate_path, 'manifest-md5.txt')
        with open(file_path, 'w') as manifest_file:
            for file in os.listdir(self.datadir_path):
                manifest_file.write(md5(os.path.join(self.datadir_path, file)) + " data/" + file + "\n")

    def populate(self):
        self.export_bt()
        self.ingest_files()
        self.catalog.export(self.crate_path)
        self.generate_manifest()


class Catalog:
    """ A Catalog class """
    def __init__(self):
        self.context = {}
        self.graph = []

    def context_append(self, key, value):
        """ Add a new context pair """
        self.context[key] = value

    def graph_element_append(self, graph_element):
        """ Add a new graph object """
        self.graph.append(graph_element)

    def serialize(self):
        """ Serialize object to string """
        jsonld_flat = {"@context": self.context,
                       "@graph": self.graph}

        return json.dumps(jsonld_flat)

    def export(self, path):
        """ Export to json file at top level of datacrate folder """
        data = {"@context": self.context, "@graph": self.graph}

        with open(os.path.join(path, 'CATALOG.json'), 'w') as jsonfile:
            json.dump(data, jsonfile, indent=4)


class GraphElement:
    """ A graph element class """
    def __init__(self, catalog, id):
        self.content = {}
        self.catalog = catalog # Parent catalog
        # check for ID uniqueness across catalog
        if not any(ge['@id'] == id for ge in self.parent.graph):
            self.id = id
            self.content['@id'] = id
            self.catalog.graph_element_append(self)
        else:
            print('An element in your graph already exists with this ID')

    def add_attribute(self, key, value):
        """
        Add a new context pair
        """
        self.content[key] = value

    def add_link(self, key, value):
        """
        Add a new single link element
        """

        self.content[key] = {"@id": value}

    def add_multilink(self, key, values):
        """
        Add a new multilink element
        """
        links = []
        for value in values:
            links.append({"@id": value})

        self.content[key] = links


class FileGrouping:
    """ A Grouping of Files """
    def __init__(self, **kwargs):
        self.description = kwargs['description']
        self.files = []

    def file_append(self, file_object):
        """ Add a new file object """
        self.files.append(file_object)


class File:
    """ An individual file object """
    def __init__(self, file_grouping, orig_path, name, description, creator):
        self.file_grouping = file_grouping
        self.orig_path = orig_path
        self.name = name
        self.description = description
        self.creator = creator
        self.file_grouping.file_append(self)

