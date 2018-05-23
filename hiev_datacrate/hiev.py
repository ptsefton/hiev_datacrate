import urllib.request
import urllib.parse
import os
import tqdm
import json
from hiev_datacrate import *


def search(api_token, base_url='https://hiev.westernsydney.edu.au/', **kwargs):
    """ Returns a list of HIEv records (or their IDs) matching a set of input search parameters.

    (see https://github.com/IntersectAustralia/dc21-doc/blob/2.3/Search_API.md)

    Input
    -----
    Required
    - api_token: HIEv API token/key
    - base_url - Base URL of the HIEv/Diver instance

    Optional keyword arguments
    - from_date - This is "Date->From Date" in search box of WEB UI: "from_date"=>"2013-01-01"
    - to_date - This is "Date->To Date" in search box of WEB UI: "to_date"=>"2013-01-02"
    - filename - This is "Filename" in search box of WEB UI: "filename"=>"test"
    - description - This is "Description" in search box of WEB UI: "description"=>"test"
    - file_id - This is "File ID" in search box of WEB UI: "file_id"=>"test"
    - id (here replaced as record_id)- This is "ID" in search box of WEB UI: "id"=>"26"
    - stati - This is "Type" in search box of WEB UI: "stati"=>["RAW", "CLEANSED"]
    - automation_stati - This is "Automation Status" in search box of WEB UI, "automation_stati"=>["COMPLETE",
      "WORKING"]
    - access_rights_types - This is the "Access Rights Type" in the search box of the WEB UI: "access_rights_types"=>
      ["Open", "Conditional", "Restricted"]
    - file_formats - This is "File Formats" in search box of WEB UI, "file_formats"=>["TOA5", "Unknown", "audio/mpeg"]
    - published - This is "Type->PACKAGE->Published" in search box of WEB UI: "stati"=>["PACKAGE"], "published"=>
      ["true"]
    - unpublished - This is "Type->PACKAGE->Published" in search box of WEB UI: "stati"=>["PACKAGE"], "unpublished"=>
      ["true"].
    - published_date - This is "Type->PACKAGE->Published Date" in search box of WEB UI: "stati"=>["PACKAGE"],
      "published_date"=>"2013-01-01"
    - tags - This is "Tags" in search box of WEB UI: "tags"=>["4", "5"]
    - labels - This is "Labels" in search box of WEB UI, "labels"=>["label_name_1", "label_name_2"]
    - grant_numbers - This is the "Grant Numbers" in search box of WEB UI, "grant_numbers"=>["grant_number_1",
      "grant_number_2"]
    - related_websites - This is the "Related Websites" in the search box of WEB UI, "related_websites"=>
      ["http://www.intersect.org.au"]
    - facilities - This is "Facility" in search box of WEB UI, ask system administrator to get facility ids :
      "facilities"=>["27"]
    - experiments - This is "Facility" in search box of WEB UI, when one facility is clicked, experiments of this
      facility are selectable, ask system administrator to get experiment ids: "experiments"=>["58", "54"]
    - variables - This is "Columns" in search box of WEB UI, when one group is clicked, columns of this group are
      selectable: "variables"=>["SoilTempProbe_Avg(1)", "SoilTempProbe_Avg(3)"]
    - uploader_id - This is "Added By" in search box of WEB UI, ask system administrator to get uploader ids:
      "uploader_id"=>"83"
    - upload_from_date - This is "Date Added->From Date" in search box of WEB UI, "upload_from_date"=>"2013-01-01"
    - upload_to_date - This is "Date Added->To Date" in search box of WEB UI, "upload_to_date"=>"2013-01-02"

    Returns
    -------
    List of matching hiev search results (with file download url included)

    Example
    -------
    my_files = hievpy.search('MY_API_TOKEN', experiments=['90'], from_date="2016-02-12")

    """

    request_url = base_url + 'data_files/api_search'

    request_data = kwargs
    # Add Auth/API token to request_data
    request_data['auth_token'] = api_token

    # -- Set up the http request and handle the returned response
    data = urllib.parse.urlencode(request_data, True)
    data = data.encode('ascii')
    req = urllib.request.Request(request_url, data)
    with urllib.request.urlopen(req) as response:
        the_page = response.read()

    encoding = response.info().get_content_charset('utf-8')
    results = json.loads(the_page.decode(encoding))

    return results


def download(api_token, record, path=None):
    """ Downloads a file from HIEv to local computer given the file record (as returned by search)

    Input
    -----
    Required
    - api_token: HIEv API token/key
    - record: record object returned by the search function

    Optional
    - path: Full path of download directory (if path not provided, file will be downloaded to current directory)
    """

    download_url = record['url'] + '?' + 'auth_token=%s' % api_token

    if path:
        download_path = os.path.join(path, record['filename'])
        urllib.request.urlretrieve(download_url, download_path)
    else:
        urllib.request.urlretrieve(download_url, record['filename'])


def datacrate(api_token, records):

    # 1. First generate a new DataCrate shell
    dc = DataCrate()

    # 2. Create a new catalog
    c = Catalog()

    # 3. Add the schema.org references (technically this should ony be done in response to a schema value being used)
    c.context_append("@vocab", "http://schema.org/")
    c.context_append("schema", "http://schema.org/")

    # 4. Create and populate a graph element for the dataset itself
    dge = GraphElement(c, "myDataset")
    dge.add_attribute("@type", "Dataset")
    dge.add_attribute("path", "/data", "schema:path")
    dge.add_attribute("identifier", "mydataset", "schema:identifier")
    dge.add_link("creator", "orcid_id_xxxxx", "schema:creator")

    # 5. Loop over the supplied records that we want to datacrate, download them to the crate, and ingest associated
    # metadata for each file into the catalog
    file_paths = []
    for record in records:
        # Download the file to the datacrate data path and inform the file manager
        download(api_token, record, path=dc.datadir_path)

        # Create a new file graph element to populate the catalog
        fge = GraphElement(c, "data/"+record['filename'])
        fge.add_attribute("@type", "File")
        fge.add_attribute("dateCreated", record['created_at'], "schema:dateCreated")
        fge.add_attribute("contentSize", record['file_size'], "schema:contentSize")
        fge.add_attribute("creator", record['creator'], "schema:creator")
        fge.add_attribute("category", record['file_processing_status'], "schema:category")
        fge.add_attribute("fileFormat", record['format'], "schema:fileFormat")
        fge.add_attribute("dateModified", record['updated_at'], "schema:dateModified")
        fge.add_attribute("startTime", record['start_time'], "schema:startTime")
        fge.add_attribute("endTime", record['end_time'], "schema:endTime")
        fge.add_attribute("description", record['file_processing_description'], "schema:description")

        fge.add_attribute("path", "data/"+record['filename'], "schema:path")
        fge.add_attribute("filename", record['filename'])

        # Also capture the filename into a list of all filenames for later appending to the dataset catalog
        file_paths.append("data/" + record['filename'])

    # Update the parent Dataset graph element to point to its individual file entries
    dge.add_multilink("hasPart", file_paths)

    # TODO Still undecided whether we need a seperate filemanager (....maybe outside of the HIEv possibly)
    # Create a new File Manager to store details of files in the crate's data folder
    fm = FileManager()
    for file in dc.datadir_path:
        # Create a new file object to manage the file itself
        f = File(os.path.join(dc.datadir_path, file), file)
        fm.append_file(f)

    # 3. Add other information
    funder_ids = ["http://orcid.org/0000-0002-3545-944X",
                  "http://orcid.org/0000-fdsfds2-3545-944X",
                  "http://orcid.org/fdsfds0-0002-3545-944X",
                  "http://orcid.org/0000-0002-3545-9fdsf"]
    dge.add_multilink("funder", funder_ids)

    # c.graph_append(dge.content)

    dc.set_catalog(c)
    dc.set_file_manager(fm)
    dc.generate()
