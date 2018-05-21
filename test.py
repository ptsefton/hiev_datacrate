from hiev_datacrate import *


# Begin by creating a new catalog
c = Catalog()
# Add the schema.org references (technically this should ony be done in response to a schema value being used)
c.context_append("@vocab", "http://schema.org/")
c.context_append("schema", "http://schema.org/")

# 1. Handle files
# 1a. Create and populate a graph element for the dataset
# 1b. For each file in the dataset create a corresponding file object (for handling the file itself) and graph_element
#     in the catalog
dge = GraphElement(c, "myDataset")
dge.add_attribute("@type", "Dataset")
dge.add_attribute("path", "/data", "schema:path")
dge.add_attribute("identifier", "mydataset", "schema:identifier")
dge.add_link("creator", "orcid_id_xxxxx", "schema:creator")

# Create a new File List object to store details of files to be included (currently those in 'uploads' folder)
fl = FileList()
for file in os.listdir(os.path.join(os.getcwd(), 'uploads')):
    # Create a new file object to manage the file itself
    f = File(fl, os.path.join(os.getcwd(), 'uploads', file), file)
    # Create a new file graph element to populate the catalog
    fge = GraphElement(c, "data/"+file)
    fge.add_attribute("@type", "File")
    fge.add_attribute("path", "data/"+file, "schema:path")
    fge.add_attribute("filename", file)

# 2. Update the Dataset graph_elements element to point to the individual file entries
file_paths = []
for entry in fl.files:
    file_paths.append("data/"+entry.filename)
dge.add_multilink("hasPart", file_paths)

# 3. Add other information
funder_ids = ["http://orcid.org/0000-0002-3545-944X",
              "http://orcid.org/0000-fdsfds2-3545-944X",
              "http://orcid.org/fdsfds0-0002-3545-944X",
              "http://orcid.org/0000-0002-3545-9fdsf"]
dge.add_multilink("funder", funder_ids)

# c.graph_append(dge.content)

# Finally generate a new DataCrate instance using the file list and all available information
dc = DataCrate(c, fl, crate_name="ExampleCrate")
dc.populate()
