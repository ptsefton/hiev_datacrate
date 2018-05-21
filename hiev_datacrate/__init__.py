import os
import json
import shutil
from hiev_datacrate.utils import *
from datetime import datetime


BAGIT_VERSION = "0.97"
TAGFILE_CHARACTER_ENCODING = "UTF-8"


class DataCrate:
    """ A DataCrate Class """
    def __init__(self, catalog, filelist, crate_name=None):
        self.catalog = catalog
        self.filelist = filelist
        self.crate_name = crate_name
        self.crate_path = None
        self.datadir_path = None
        self.bagit_version = BAGIT_VERSION
        self.tagfile_char_encoding = TAGFILE_CHARACTER_ENCODING

        # Create the basic structure of a DataCrate
        if self.crate_name:
            if not os.path.isdir(os.path.join(os.getcwd(), self.crate_name)):
                self.crate_path = os.path.join(os.getcwd(), self.crate_name)
                self.datadir_path = os.path.join(self.crate_path, 'data')
                os.makedirs(self.crate_path)
                os.makedirs(os.path.join(self.datadir_path))
            else:
                print('This datacrate already exists')
        else:
            self.crate_name = datetime.now().strftime('%Y%m%d%H%M%S')
            self.crate_path = os.path.join(os.getcwd(), self.crate_name)
            self.datadir_path = os.path.join(self.crate_path, 'data')
            os.makedirs(self.crate_path)
            os.makedirs(self.datadir_path)

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
        for file in self.filelist.files:
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
        self.graph_elements = []

    def context_append(self, key, value):
        """ Add a new context pair """
        self.context[key] = value

    def graph_element_append(self, graph_element):
        """ Add a new graph_elements object """
        self.graph_elements.append(graph_element)

    def context_append(self, key, schema_val):
        """ Add a new context entry if it doesn't exist """
        if key in self.context and schema_val == self.context[key]:
            pass
        else:
            self.context[key] = schema_val

    def serialize(self):
        """ Serialize myself to string """
        graph_serial = []
        for ge in self.graph_elements:
            graph_serial.append(ge.content)
        data = {"@context": self.context, "@graph_elements": graph_serial}

        return json.dumps(data)

    def export(self, path):
        """ Export myself to a json file at top level of datacrate folder """
        graph_serial = []
        for ge in self.graph_elements:
            graph_serial.append(ge.content)
        data = {"@context": self.context, "@graph": graph_serial}

        with open(os.path.join(path, 'CATALOG.json'), 'w') as jsonfile:
            json.dump(data, jsonfile, indent=4)


class GraphElement:
    """ An individual graph element (part of catalog) class """
    def __init__(self, catalog, gid):
        self.content = {}
        self.catalog = catalog  # Parent catalog
        # check for ID uniqueness across catalog
        if not any(ge.content['@id'] == gid for ge in self.catalog.graph_elements):
            self.gid = gid
            self.content['@id'] = gid
            self.catalog.graph_element_append(self)
        else:
            print('An element in your graph_elements already exists with this ID')

    def add_attribute(self, key, value, schema_val=None):
        """
        Add a new context pair
        """
        self.content[key] = value

        # Add to context if not already existing
        if schema_val:
            self.catalog.context_append(key, schema_val)

    def add_link(self, key, value, schema_val=None):
        """
        Add a new single link element
        """

        self.content[key] = {"@id": value}

        # Add to context if not already existing
        if schema_val:
            self.catalog.context_append(key, schema_val)

    def add_multilink(self, key, values, schema_val=None):
        """
        Add a new multilink element
        """
        links = []
        for value in values:
            links.append({"@id": value})

        self.content[key] = links

        # Add to context if not already existing
        if schema_val:
            self.catalog.context_append(key, schema_val)


class FileList:
    """ A Grouping of Files """
    def __init__(self):
        self.files = []

    def file_append(self, file_object):
        """ Add a new file object """
        self.files.append(file_object)


class File:
    """ An individual file object """
    def __init__(self, file_list, orig_path, filename):
        self.file_list = file_list
        self.orig_path = orig_path
        self.filename = filename
        self.file_list.file_append(self)
