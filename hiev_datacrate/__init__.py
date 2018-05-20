import os
import json
from datetime import datetime


class DataCrate:
    """ A DataCrate Class """
    def __init__(self):
        self.catalog = None
        self.crate_path = None
        self.bagit_version = "0.97"
        self.tagfile_char_encoding = "UTF-8"

    def set_catalog(self, catalog):
        self.catalog = catalog

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

    def generate(self, crate_name=None):
        if self.validate():
            if crate_name:
                if not os.path.isdir(os.path.join(os.getcwd(), crate_name)):
                    self.crate_path = os.path.join(os.getcwd(), crate_name)
                    os.makedirs(self.crate_path)
                    os.makedirs(os.path.join(self.crate_path, 'data'))
                else:
                    print('This datacrate already exists')
            else:
                crate_name = datetime.now().strftime('%Y%m%d%H%M%S')
                self.crate_path = os.path.join(os.getcwd(), crate_name)
                os.makedirs(self.crate_path)
                os.makedirs(os.path.join(self.crate_path, 'data'))

            self.catalog.export(self.crate_path)
            self.export_bt()
        else:
            return "Unable to generate an Invalid DataCrate"


class Catalog:
    """ A Catalog class """
    def __init__(self):
        self.context = {}
        self.graph = []

    def context_append(self, key, value):
        """ Add a new context pair """
        self.context[key] = value

    def graph_append(self, graph_object):
        """ Add a new graph object """
        self.graph.append(graph_object)

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
    def __init__(self):
        self.content = {}

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
