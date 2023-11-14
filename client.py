#!/usr/bin/env python3

import xmltodict
import json
import requests 
from typing import Dict
import os 
import urllib.parse
from rich.progress import (
    TextColumn, 
    BarColumn, 
    DownloadColumn, 
    TransferSpeedColumn, 
    TimeRemainingColumn, 
    Progress
)
from concurrent.futures import ThreadPoolExecutor
import argparse
from functools import reduce
from tabulate import tabulate
import re


# ?restype=container&comp=lis

output_dir = "./loot_new"

class AzureFile(object):
    Account: str
    Container: str 
    Name: str 
    Url: str 
    Properties: Dict[str,str]

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.filepath = urllib.parse.urlparse(self.Url).path 
        self.ext = self.Name.split('.')[-1] if len(self.Name.split('.')) > 1 else ""

    def __str__(self):
        return f"<AzureFile name={self.Name}>"

    def __repr__(self):
        return f"<AzureFile name={self.Name}>"
    
    def download_url(self):
        return self.Url
    
    def ToTable(self):
        return [
            self.Account,
            self.Container,
            self.Name,
            self.ext,
        ]
    
    def Download(self, client, target = output_dir, block_write_callback = None, get_header_callback = None):
        resp = requests.get(self.Url, allow_redirects = True, stream = True)
        if get_header_callback: get_header_callback(int(resp.headers.get("content-length")))
        target_path = output_dir + "/" + self.Account + self.filepath
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, 'wb') as fd:
            for chunk in resp.iter_content(chunk_size=1024):
                if block_write_callback: block_write_callback(1024)
                if chunk:
                    fd.write(chunk)
                    fd.flush()

class AzureFileBase(object):
    def __init__(self, data) -> None:
        self.data = data 
        self._current = 0

    def __iter__(self):
        return self
    
    def __next__(self):
        try:
            result = self.data[self._current]
        except IndexError:
            raise StopIteration
        self._current += 1
        return result         

    def IncludeExts(self, exts):
        return AzureFileBase(list(filter(lambda x: x.ext in exts, self.data)))

    def ExcludeExts(self, exts):
        return AzureFileBase(list(filter(lambda x: x.ext not in exts, self.data)))
    
    def AllExtensions(self):
        return [[i] for i in set([d.ext for d in self.data])]
    
    def Search(self,search, regex = False):
        if regex:
            fn = lambda x: re.match('^.*(' + search + ').*$', x.Name.lower())
        else:
            fn = lambda x: search.lower() in x.Name.lower()
        return AzureFileBase(list(filter( fn, self.data)))
    
    def ToList(self):
        return self.data
    
    def ToTable(self):
        return ["Account","Container","File","Extension"], [d.ToTable() for d in self.data]
    

class AzureClient(object):

    def __init__(self, client, name: str, container: str ) -> None:
        self.name = name 
        self.container = container 
        self.url = f"https://{self.name}.blob.core.windows.net/{self.container}"

    def List(self):
        data = []
        for b in xmltodict.parse( client.get(f"{self.url}?restype=container&comp=list").content )["EnumerationResults"]["Blobs"]["Blob"]:
            b["Account"] = self.name
            b["Container"] = self.container
            data += [AzureFile(**b)]
        return AzureFileBase(data)

    def IncludeExt(self, exts):
        return filter(lambda x: x.ext in exts, self.List())
    
    def ExcludeExt(self, exts):
        return filter(lambda x: x.ext not in exts, self.List())


def DownloadThreaded(f, progress):
    task_id = progress.add_task(f"Downloading: {f.Account}:{f.filepath}")

    try:
        f.Download(
            client, 
            get_header_callback=lambda x: progress.update(task_id, total=x),
            block_write_callback=lambda x: progress.update(task_id, advance=x))
    except requests.exceptions.ConnectionError as e:
        print(f"Error: {e}")
    
    progress.remove_task(task_id)


if __name__ == "__main__":

    parser = argparse.ArgumentParser("Storage Account Downloader")
    parser.add_argument("--container","-c",help="Container name",required=True)
    parser.add_argument("--account","-a",help="Account name",required=True)
    parser.add_argument("--exclude", type = lambda s: [i for i in s.split(',')],  help="File extensions to exclude")
    parser.add_argument("--include", type = lambda s: [i for i in s.split(',')],  help="File extensions to include")
    parser.add_argument("--download", action='store_true', help = "Download files found")
    parser.add_argument("--show","--list", action='store_true', help = "List files in a table")
    parser.add_argument("--exts", action='store_true', help = "List all file extensions in use")
    parser.add_argument("--output","-o", type = str, help = "Directory to save files to", default = "./loot_new")
    parser.add_argument("--regex", action='store_true', help = "Interpret search string as regex")
    parser.add_argument("search",default='', help="Phrase to search for")
    args = parser.parse_args()

    # account_name = "blobpdtaus2"
    account_name = args.account
    # container_name = "emails"
    container_name = args.container

    client = requests.session()
    c = AzureClient(client, account_name, container_name)

    progress = Progress(
        TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
    )

    with Progress() as progress:
        blobs = c.List()
        if args.exclude: blobs = blobs.ExcludeExts(args.exclude)
        elif args.include: blobs = blobs.IncludeExts(args.include)

        if args.search: blobs = blobs.Search(args.search, regex = args.regex)
        
        if args.download:
            with ThreadPoolExecutor(max_workers=10) as executor:
                for f in blobs:
                    executor.submit(DownloadThreaded, f, progress)
        elif args.exts:
            print(tabulate(blobs.AllExtensions(),headers=["Extensions"]))

        else:
            headers, data = blobs.ToTable()
            print(tabulate(data, headers=headers))