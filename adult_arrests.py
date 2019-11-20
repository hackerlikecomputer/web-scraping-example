"""Python module to scrape all the data from the Chicago Police Adult Arrest
   search page."""

# I use what are called docstrings pretty heavily. It's those triple quotes """
# They can be used after function and class definitions
# Some code editors (I use VSCode, which does this) can use them to give hints
# For example, you'll start to write a function and it will tell you which
# parameters can be passed.

# re is the RegEx python core module (comes with python, no install)
import re

# time allows you to pause a script for a certain time
import time

# handles HTTP requests
import requests

# HTML parser
from bs4 import BeautifulSoup

# Used for data manipulation
import pandas as pd

# Helpful for math, pandas uses it heavily
import numpy as np

# I use custom exceptions to help debug my programs
# when writing web scrapers, I'd recommend you do the same thing.
# You just create a class that inherits from the Exception base class
# Learn more here:
# https://www.geeksforgeeks.org/user-defined-exceptions-python-examples/
class ElementNotFoundException(Exception):
    """custom exception class when an element doesn't exist"""

    # ^^docstring
    # the exception doesn't do anything, so just use pass
    pass


class MissingDataError(Exception):
    """custom exception class for missing results when passed to DataFrame"""

    pass


# Brody: I'm using object-oriented programming
# You might not have learned this yet
# An object is basically a collection of functions and variables
# the __init__ function runs when the object is created, i.e. AdultArrestScraper()
# the __init__ variables are passed when the object is created
# "self" is used when you reference something that belongs to the object
#######################################################
# you do NOT need to learn this right now to use python
#######################################################
# but you can learn more here:
# https://realpython.com/python3-object-oriented-programming/


class AdultArrestScraper:
    def __init__(self):
        self.base_url = "http://publicsearch3.azurewebsites.net"
        self.session = requests.Session()
        # copied directly from Chrome request headers
        # this is not mandatory, but some sites will reject your request
        # if you don't pass them with the request
        self.headers = {
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/78.0.3904.97 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;"
            "v=b3",
            "Referer": "http://publicsearch3.azurewebsites.net/Arrests",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0.9",
        }
        # start the scraper by hitting the home page
        # this'll get the cookies that would be handled by a browser
        initial_response = self.session.get(self.base_url + "/Arrests")
        # assert throws an error unless the condition is met
        # status code has to be 200 (OK)
        assert initial_response.status_code == 200, (
            f"request to url {self.base_url + '/Arrests'} failed with "
            f"code {initial_response.status_code}"
        )

    def get(self, url):
        """Makes an GET request, allowing retries and using headers

        Args:
            url (str): url to GET
        """

        # first set up counters, which will be modified in the while loop
        retry_count = 0
        total_wait_time = 0
        # keep everything in the while loop so it keeps going
        # the retry count <= 10 will keep it from going forever
        while retry_count <= 10:
            # first try the request with no wait time
            try:
                resp = self.session.get(url, headers=self.headers)
                # only return the response if it was successful
                if resp.status_code == 200:
                    return resp
                else:
                    raise ValueError(
                        f"request to {url} failed with error code {resp.status_code}"
                    )
            # Timeout is an exception from the requests module
            except requests.exceptions.Timeout:
                # let the counter know it's going to retry it
                retry_count += 1
                # calculate the wait time (grows exponentially)
                wait_time = retry_count ** 2 * 10
                # keep track of how much time it's waited
                total_wait_time += wait_time
                # time.sleep waits for n seconds
                time.sleep(wait_time)
                # retry the loop
                continue
        # If it's tried more than 10 times, give up
        else:
            raise ValueError(
                f"request to {url} timed out after waiting "
                f"a total of {total_wait_time} seconds"
            )

    # I'm overriding the BeautifulSoup.find() and findAll() methods
    # these custom methods use BeautifulSoup under the hood, but provide additional
    # functionality
    # For example: BeautifulSoup returns None if it doesn't find() something
    # I wanted it to throw an error, so rather than repeatedly testing if the request
    # was successful
    # each time I made a request, I created this method
    def find(self, soup, name, attrs={}):
        """replaces the BeautifulSoup.find() method, throws error if no results

        Args:
            soup (bs4.BeautifulSoup object): soup to search
            name (str): name of tag, passed directly to soup.find()
            attrs (dict): attrs pass to soup.find(), passed directly to soup.find()

        Returns: to_find (bs4.BeautifulSoup object)
        """

        # the arguments from my find() function are passed directly to soup.find()
        to_find = soup.find(name, attrs)
        # soup.find() returns None if it can't find it
        # to tell if something is not None, just write "if *thing*""
        # you don't need to write out "if thing is None"
        if to_find:
            return to_find
        else:
            raise ElementNotFoundException(
                f"{name} element not found with attrs {attrs}"
            )

    # same deal as above, this does the same thing as soup.findAll()
    # with additional functionality
    def find_all(self, soup, name, attrs={}):
        """replaces the BeautifulSoup.findAll() method

        Args:
            name (str): name of tag
            attrs (dict): attrs pass to soup.find()

        Returns: to_find (list of bs4.BeautifulSoup objects)
        """

        # same deal as find(), arguments passed right to soup.find()
        to_find = soup.findAll(name, attrs)
        # in this case, findAll returns an empty list if none are found
        if len(to_find) > 0:
            return to_find
        else:
            raise ElementNotFoundException(
                f"{name} element not found with attrs {attrs}"
            )

    def get_page_num(self, soup):
        """retrieves the number of pages for a search result

        Args:
            soup (BeautifulSoup object): soup from which to get result

        Returns: page number (int), or None
        """

        # first find the paginator div
        try:
            paginator = self.find(soup, "div", {"class": "pagination-container"})
        except ElementNotFoundException:
            # there's no paginator, one page only
            return 1
        # then find the last page button, then get its href
        try:
            last_page_btn = self.find(
                paginator, "li", {"class": "PagedList-skipToLast"}
            )
        except ElementNotFoundException:
            # there's no last page button, one page only
            return 1
        last_page_href = self.find(last_page_btn, "a")["href"]
        # using a RegEx pattern to get the exact page number
        page_no_pat = re.compile(r"(?<=Page\=)\d+(?=\&)")
        match = page_no_pat.search(last_page_href)
        if match:
            return int(match.group())

    def get_detail_hrefs(self, soup):
        """scrapes the detail pages for a set of search results

        Args:
            soup (BeautifulSoup object): soup to search

        Returns: detail_hrefs (list of str)
        """

        results_table = self.find(soup, "table", {"class": "table-striped"})
        tbody = self.find(results_table, "tbody")
        rows = self.find_all(tbody, "tr")

        details_hrefs = []

        for row in rows:
            # details button is 8th in row
            try:
                details_btn = self.find_all(row, "td")[7]
            except IndexError:
                raise ElementNotFoundException("Unable to locate details button")
            else:
                details_href = self.find(details_btn, "a")["href"]
                details_hrefs.append(details_href)

        return details_hrefs

    def scrape_details_page(self, href):
        """scrapes the data from details page

        Args:
            href (str): href to request

        Returns: pandas DataFrame
        """

        url = self.base_url + "/" + href
        resp = self.get(url)
        soup = BeautifulSoup(resp.content, "html.parser")
        body = self.find(soup, "div", {"class": "body-content"})

        # first I need to get the descriptive info
        # that data is held in four <dl> tags
        dls = self.find_all(body, "dl", {"class": "dl-horizontal"})
        # first <dl> is name, age, cb
        details = {}
        for dl in dls:
            # Brody: you might find this confusing
            # Google list comprehensions
            keys = [dt.text.strip() for dt in self.find_all(dl, "dt")]
            vals = [dd.text.strip() for dd in self.find_all(dl, "dd")]
            # zip() combines two iterables
            # dict() turns the zip() into a dictionary
            data = dict(zip(keys, vals))
            # result looks like this:
            # {'NAME': 'MYRO DELON BOOKER', 'AGE': '43', 'CB NUMBER': '19895515'}
            details.update(data)

        # next need to get the charges
        # they're in a <table> in the body
        table = self.find(body, "table")
        rows = self.find_all(table, "tr")

        # set up a dict of lists to store the results
        charges = {"statute": [], "description": [], "inchoate": []}

        # Brody: This might be confusing, so let me know if you want a more
        # detailed explanation

        # I'm also adding the keys for the details
        # I want one row per charge with the details in each row
        # It's a surprise tool that will help us later
        for key in details:
            charges.update({key: []})

        for row in rows:
            # same deal as above
            vals = [td.text.strip() for td in self.find_all(row, "td")]
            data = dict(zip(["statute", "description", "inchoate"], vals))
            # this appends the results to a list inside the dict
            for key in data:
                # if the results is an empty string, I want a "null object"
                # it comes from the numpy library: np.NaN
                # pandas uses it by default
                if data[key] == "":
                    charges[key].append(np.NaN)
                else:
                    charges[key].append(data[key])
            # this does the same thing for the details
            for key in details:
                if details[key] == "":
                    charges[key].append(np.NaN)
                else:
                    charges[key].append(details[key])

        # remember that weird dictionary/list structure?
        # we can pass it right into a dataframe as long as
        # all the lists are the same length
        # I'm using a try/except block to catch an ValueError if
        # the lists are different lengths, which would indicate missing data
        try:
            return pd.DataFrame(charges)
        except ValueError:
            raise MissingDataError(f"one or more columns has missing data")

    def query(
        self,
        first_name="",
        last_name="",
        cb_number="",
        charge="",
        area="",
        district="",
        beat="",
    ):
        """Searches the CPD adult arrest page using any of the search fields

        Args:
            first_name (str, default ""): query parameter
            last_name (str, default ""): query parameter
            cb_number (str, default ""): query parameter
            charge (str, default ""): query parameter
            area (str, default ""): query parameter
            district (str, default ""): query parameter
            beat (str, default ""): query parameter
        """

        params = [first_name, last_name, cb_number, charge, area, district, beat]
        if all(p == "" for p in params):
            raise ValueError("no search parameters passed")

        if first_name == "" and last_name != "":
            raise ValueError("last name cannot be provided without first name")
        elif first_name != "" and last_name == "":
            raise ValueError("first name cannot be provided without last name")

        # the parentheses here is just because the line would be very long
        # keeping with the rule of max line length of 88
        # multiple strings inside parentheses are automatically combined
        query_str = (
            # prefix for URL query
            "/Arrests?"
            # query parameters
            f"FirstName={first_name}"
            f"&LastName={last_name}"
            f"&CbNumber={cb_number}"
            f"&ChargeId={charge}"
            f"&CPDArea={area}"
            f"&District={district}"
            f"&Beat={beat}"
        )

        # merge the base url and query parameters
        query_url = self.base_url + query_str
        # make the request
        resp = self.get(query_url)
        # get the initial result page
        soup = BeautifulSoup(resp.content, "html.parser")
        # get max page number to loop through pages
        max_page_no = self.get_page_num(soup)
        # get_page_num returns None if it doesn't find one
        # self.scrape_details_page returns df, create list for results
        # we'll combine them later
        dfs = []
        if max_page_no:
            # there are multple pages to scrape
            for page_no in range(1, max_page_no + 1):
                # add the page parameter to the end of the url
                url = query_url + f"&page={page_no}"
                resp = self.get(url)
                soup = BeautifulSoup(resp.content, "html.parser")
                hrefs = self.get_detail_hrefs(soup)
                for href in hrefs:
                    dfs.append(self.scrape_details_page(href))
        else:
            # only one page, do it once
            resp = self.get(query_url)
            soup = BeautifulSoup(resp.content, "html.parser")
            hrefs = self.get_detail_hrefs(soup)
            for href in hrefs:
                dfs.append(self.scrape_details_page(href))

        # combine the list we created above into a single DataFrame
        df = pd.concat(dfs)
        return df


# when a python file is run from the command line, python does some stuff
# one of those things is hard-coding a variable called __name__ to a string,
# "__main__". That does not happen when a module is imported.
# all python scripts can be imported to other python scripts
# in this case, you could do "from adult_arrests import AdultArrestScraper"
# again, you do NOT need to know this to use python, but I wanted to explain this

# I do it like this because I might want to use this code in another script
# I might also want to run the whole thing like you can below
# to run something from the command line, you write
# python -m module_name. That'll run everything below this if clause
# more here: https://realpython.com/python-main-function/
if __name__ == "__main__":
    # set up a bin to put all the dfs
    dfs = []
    # initialize the scraper
    # when you do this, __init__ runs behind the scenes
    scraper = AdultArrestScraper()
    # I know there are 25 police districts in Chicago
    # now loop through the districts
    for i in range(1, 26):
        df = scraper.query(district=i)
        dfs.append(df)
    # pd.concat combines a list of dfs
    df = pd.concat(dfs)
    # allows user to choose where to save file
    out_path = input("Path to csv to save results to: ")
    # finally, save them as a single file
    df.to_csv(out_path)
