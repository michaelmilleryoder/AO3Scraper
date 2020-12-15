######
#
# This script takes in (a list or csv of) fic IDs and
# writes a csv containing the fic itself, as well as the 
# metadata.
#
# Usage - python ao3_get_fanfics.py ID [--header header] [--csv csvoutfilename] 
#
# ID is a required argument. It is either a single number, 
# multiple numbers seperated by spaces, or a csv filename where
# the IDs are the first column.
# (It is suggested you run ao3_work_ids.py first to get this csv.)
#
# --header is an optional string which specifies your HTTP header
# for ethical scraping. For example, the author's would be 
# 'Chrome/52 (Macintosh; Intel Mac OS X 10_10_5); Jingyi Li/UC Berkeley/email@address.com'
# If left blank, no header will be sent with your GET requests.
# 
# --csv is an optional string which specifies the name of your
# csv output file. If left blank, it will be called "fanfics.csv"
# Note that by default, the script appends to existing csvs instead of overwriting them.
# 
# --restart is an optional string which when used in combination with a csv input will start
# the scraping from the given work_id, skipping all previous rows in the csv
#
# Author: Jingyi Li soundtracknoon [at] gmail
# I wrote this in Python 2.7. 9/23/16
# Updated 2/13/18 (also Python3 compatible)
#
# Reformatted output to be consistent with fanfiction.net scraper
# Sept 2018  Chris Bogart
#######

import requests
from bs4 import BeautifulSoup
import bs4
import json
import gzip
import argparse
import time
import os
import pdb
import re
import csv
import sys
from tqdm import tqdm
#from unidecode import unidecode

# We don't want to convert unicode to ascii particularly
def unidecode(st): return st

def safe(st):
    try: return st.encode("utf-8", "default")
    except: return st

def maybe_json(s):
    if type(s) is list: return json.dumps(s)
    if type(s) is dict: return json.dumps(s)
    else: return s

def consolidate(tags):
    return " ".join([unidecode(t.text if type(t) is bs4.element.Tag else t).strip() for t in tags])

def into_chunks(tag):
    previouschild  = []
    for i, child in enumerate(tag.children):
        if child.name == "p":
            if len(previouschild): 
                yield consolidate(previouschild)
                previouschild = []
            for chunk in into_chunks(child):
                yield chunk
        elif child.name == "div":
            if len(previouschild): 
                yield consolidate(previouschild)
                previouschild = []
            for chunk in into_chunks(child): 
                yield chunk
        elif child.name == "br":
            if len(previouschild): 
                yield consolidate(previouschild)
                previouschild = []
        elif type(child) is bs4.element.Tag:
            previouschild.append(child)
        else:
            previouschild.append(child)
    yield consolidate(previouschild)

def into_text(tag):
    return "\n".join([ch.strip() for ch in into_chunks(tag) if len(ch.strip()) > 0])

def get_tag_info(category, meta):
    '''
    given a category and a 'work meta group, returns a list of tags (eg, 'rating' -> 'explicit')
    '''
    try:
        tag_list = meta.find("dd", class_=str(category) + ' tags').find_all(class_="tag")
    except AttributeError as e:
        return []
    return [unidecode(result.text) for result in tag_list] 

def get_series(meta):
    try:
        seriesregion = meta.find("span",class_="series").find("span",class_="position")
        part_and_name = re.search("Part (\d+) of the (.*) series", seriesregion.text)
        seriespart = part_and_name.group(1)
        series = part_and_name.group(2)
        seriesid = seriesregion.find("a")["href"].split("/")[2]
    except:
        series = ""
        seriespart = ""
        seriesid = ""
    return (series, seriespart, seriesid)
    
def get_stats(meta):
    '''
    returns a dictionary of  
    language, published, status, date status, words, chapters, comments, kudos, bookmarks, hits
    '''
    categories = ['language', 'published', 'status', 'words', 'chapters', 'comments', 'kudos', 'bookmarks', 'hits'] 

    stats = {}
    for category in categories:
        try:
            stat = unidecode(meta.find("dd", class_=category).text )
        except Exception as e:
            stat = "null"
            tqdm.write("Category error:")
            tqdm.write('{} {} {}'.format(type(e), e, category))
        stats[category] = stat

    stats["status date"] = stats.get("status",stats["published"])
    stats["language"] = stats["language"].strip()

    #add a custom completed/updated field
    thestatus  = meta.find("dt", class_="status")
    if not thestatus: status = 'Completed' 
    else: status = thestatus.text.strip(':')
    stats["status"] = status
    
    #print stats
    return stats      

def get_tags(meta):
    '''
    returns a list of lists, of
    rating, category, fandom, pairing, characters, additional_tags
    '''
    tags = ['rating', 'category', 'fandom', 'relationship', 'character', 'freeform']
    return { tag: get_tag_info(tag, meta) for tag in tags }


def access_denied(soup):
    if (soup.find(class_="flash error")):
        return True
    if (not soup.find(class_="work meta group")):
        return True
    return False

def url2cache(url):
    cache = "raw/" + re.sub(r'[^a-zA-Z0-9]', '_', url) + ".gz"
    return cache


def robust_get(url, headers):
    delay = 5
    cache = url2cache(url)
    if os.path.isfile(cache):
        return gzip.open(cache).read().decode("utf-8","default")
    req = None
    req_count = 10
    req_err = None
    while req_count > 0 and req is None:
        try:
            time.sleep(delay)
            req = requests.get(url, headers=headers)
            #with gzip.open(cache,"wb") as f:
            #    f.write(req.text.encode("utf-8"))
        except Exception as e:
            req = None
            req_err = e
            req_count -= 1
            print("ERROR, on ", url, " sleeping 30")
            print(type(e), e)
            time.sleep(30)
    if req_count == 0 and req is None:
        raise req_err
    return req.text

def workdir(output_dirpath, fandom): 
    return os.path.join(output_dirpath, "ao3_" + fandom + "_text")

def storiescsv(output_dirpath, fandom): 
    return os.path.join(output_dirpath, "ao3_" + fandom + "_text/stories.csv")

def errorscsv(output_dirpath, fandom): 
    return os.path.join(output_dirpath, "ao3_" + fandom + "_text/errors.csv")

def chapterscsv(output_dirpath, fandom): 
    return os.path.join(output_dirpath, "ao3_" + fandom + "_text/chapters.csv")

def contentdir(output_dirpath, fandom): 
    return os.path.join(output_dirpath, "ao3_" + fandom + "_text/stories/")

def contentfile(output_dirpath, fandom, workid, chapterid): 
    return contentdir(output_dirpath, fandom) + workid + "_" + str(chapterid).zfill(4) + ".csv"

def write_fic_to_csv(fandom, fic_id, only_first_chap, storywriter, chapterwriter, errorwriter, storycolumns, chaptercolumns, header_info='', output_dirpath=''):
    '''
    fandom is the grouping that determines filenames etc.
    fic_id is the AO3 ID of a fic, found every URL /works/[id].
    writer is a csv writer object
    the output of this program is a row in the CSV file containing all metadata 
    and the fic content itself.
    header_info should be the header info to encourage ethical scraping.
    '''
    tqdm.write('Scraping {}'.format(fic_id))
    get_comments = True
    url = 'http://archiveofourown.org/works/'+str(fic_id)+'?view_adult=true'
    if not only_first_chap:
        url = url + '&view_full_work=true'
    if get_comments:
        url = url + '&show_comments=true'
    headers = {'user-agent' : header_info}
    src = robust_get(url, headers)
    soup = BeautifulSoup(src, 'lxml')
    if (access_denied(soup)):
        print('Access Denied')
        open("err_" + str(fic_id) + ".err.txt", "w").write(src)
        error_row = [fic_id] + ['Access Denied']
        errorwriter.writerow(error_row)
    else:
        meta = soup.find("dl", class_="work meta group")
        (series, seriespart, seriesid) = get_series(meta)
        tags = get_tags(meta)
        stats = get_stats(meta)
        title = unidecode(soup.find("h2", class_="title heading").string).strip()
        author = unidecode(soup.find(class_="byline").text).strip()
        try:
            href = soup.find(class_="byline").find("a")["href"]
            author_key = href.split("/")[2]
            author_pseudo = href.split("/")[4]
        except Exception as e:
            print('Unexpected error getting authorship: ', sys.exc_info()[0])
            error_row = [fic_id] +  [sys.exc_info()[0]]
            errorwriter.writerow(error_row)
            author_key = author
            author_pseudo= author
            
        #get the fic itself
        content = soup.find("div", id= "chapters")
        #chapters = content.findAll("div", id=re.compile('^chapter-'))
        chapnodes = content.findAll("div", id=re.compile('^chapter-'))
        if len(chapnodes) == 0: chapnodes = soup.findAll("div", id="chapters")
        chapter_titles = [t.h3.text.strip()  for t in chapnodes]
        chapters = [ch.find("div", class_="userstuff") for ch in chapnodes]
        #content.findAll("div", class_="userstuff") #id=re.compile('^chapter-'))
        #chapters = content.findAll("div", class_="userstuff") #id=re.compile('^chapter-'))
        #chapter_titles = [unidecode(t.find("h3").text).strip() for t in content.findAll("div", class_="preface")]
        #if len(chapter_titles) == 0:
        #    chapter_titles = [title]


        st_summary = ""
        st_preface_notes = ""
        st_afterword_notes = ""
        for preface in soup.find_all("div", class_="preface"):
            if "afterword" in preface.attrs['class']:
                try:
                    st_afterword_notes = into_text(preface.find("blockquote"))
                except: pass
            elif "chapter" not in preface.attrs['class']:
                try:
                    st_preface_notes = into_text(preface.find("div",class_="notes").find("blockquote"))
                except: pass
                try:
                    st_summary = into_text(preface.find("div",class_="summary").find("blockquote"))
                except: pass

        strow = { "fic_id": fic_id,
                  "title": title.encode("utf-8"),
                  "summary": st_summary.encode("utf-8"),
                  "preface_notes": st_preface_notes.encode("utf-8"),
                  "afterword_notes": st_afterword_notes.encode("utf-8"),
                  "series": series,
                  "seriespart": seriespart,
                  "seriesid": seriesid,
                  "author": author_pseudo.encode("utf-8"),
                  "author_key": author_key.encode("utf-8"),
                  "additional tags": tags["freeform"],
                  "chapter_count": len(chapters) }
        strow = dict(strow, **tags)
        strow = dict(strow, **stats)
        storywriter.writerow([safe(maybe_json(strow.get(k,"null"))) for k in storycolumns])
            

        # get div class=notes under div class=preface, and under div class=afterword; class-level notes
        # get div class=summary under div class=preface
        for ch, chall in enumerate(chapters):
            chapter_title = chapter_titles[ch]
            paras = [t.text if type(t) is bs4.element.Tag else t for t in into_chunks(chall)]
            paras = [unidecode(t).strip() for t in paras if len(t.strip()) > 0 and t.strip() != "Chapter Text"]
             
            ch_preface_notes = ""
            ch_summary = ""
            ch_afterword_notes = ""
            chapnode = chapnodes[ch]
            try:
                ch_summary = into_text(chapnode.find("div", class_="preface").find("div", id="summary").find("blockquote"))
            except: pass
            try:
                ch_preface_notes = into_text(chapnode.find("div", class_="preface").find("div", id="notes").find("blockquote"))
            except: pass
            try:
                ch_afterword_notes = into_text(chapnode.find("div", class_="end").find("blockquote"))
            except: pass
            # div class=end notes --> id=notes
            chrow =  {
                 "fic_id": fic_id,
                 "title": title.encode("utf-8"),
                 "summary": ch_summary.encode("utf-8"),
                 "preface_notes": ch_preface_notes.encode("utf-8"),
                 "afterword_notes": ch_afterword_notes.encode("utf-8"),
                 "chapter_num": str(ch+1),
                 "chapter_title": chapter_title.encode("utf-8"),
                 "paragraph_count": len(paras)}
            chapterwriter.writerow([chrow.get(k,"null") for k in chaptercolumns])
            content_out = csv.writer(open(contentfile(output_dirpath, fandom, fic_id, ch+1), "w"))
            content_out.writerow(['fic_id', 'chapter_id','para_id','text'])
            for pn, para in enumerate(paras):
                try:
                    content_out.writerow([fic_id, ch+1, pn+1, para.encode("utf-8")])
                except:
                    print('Unexpected error: ', sys.exc_info()[0])
                    pdb.set_trace()
                    error_row = [fic_id] +  [sys.exc_info()[0]]
                    errorwriter.writerow(error_row)
            content_out = None
        tqdm.write('Done.')
        tqdm.write(' ')

def get_args(): 
    parser = argparse.ArgumentParser(description='Scrape and save some fanfic, given their AO3 IDs.')
    parser.add_argument(
        'ids', metavar='IDS', nargs='+',
        help='a single id, a space seperated list of ids, or a csv input filename')
    parser.add_argument(
        '--fandom', default='some_fandom',
        help='fandom identifier')
    parser.add_argument(
        '--header', default='',
        help='user http header')
    parser.add_argument(
        '--restart', default='', 
        help='work_id to start at from within a csv')
    parser.add_argument(
        '--firstchap', default='', 
        help='only retrieve first chapter of multichapter fics')
    parser.add_argument(
        '--outputdir', default='',
        help='Path to the output directory. Will create output/ao3_<fandom>_text directory within that directory.')
    args = parser.parse_args()
    fic_ids = args.ids
    idlist_is_csv = (len(fic_ids) == 1 and '.csv' in fic_ids[0]) 
    fandom = str(args.fandom)
    headers = str(args.header)
    if headers == "":
        if os.path.isfile(".browser_header.txt"):
            headers = open(".browser_header.txt", "r").read().strip()
    print("Using", headers, "to self-identify to ArchiveOfOurOwn")
    restart = str(args.restart)
    ofc = str(args.firstchap)
    if ofc != "":
        ofc = True
    else:
        ofc = False
    output_dirpath = args.outputdir
    return fic_ids, fandom, headers, restart, idlist_is_csv, ofc, output_dirpath

'''

'''
def process_id(fic_id, restart, found):
    if found:
        return True
    if fic_id == restart:
        return True
    else:
        return False

def main():
    fic_ids, fandom, headers, restart, idlist_is_csv, only_first_chap, output_dirpath = get_args()
    os.chdir(os.getcwd())
    storycolumns = ['fic_id', 'title', 'author', 'author_key', 'rating', 'category', 'fandom', 'relationship', 'character', 'additional tags', 'language', 'published', 'status', 'status date', 'words', 'comments', 'kudos', 'bookmarks', 'hits', 'chapter_count', 'series','seriespart','seriesid', 'summary', 'preface_notes','afterword_notes']
    chaptercolumns = ['fic_id', 'title', 'summary', 'preface_notes', 'afterword_notes', 'chapter_num', 'chapter_title', 'paragraph_count']
    textcolumns = ['fic_id', 'chapter_id','para_id','text']
    if not os.path.exists(workdir(output_dirpath, fandom)):
        os.mkdir(workdir(output_dirpath, fandom))
    if not os.path.exists(contentdir(output_dirpath, fandom)):
        os.mkdir(contentdir(output_dirpath, fandom))
    with open(storiescsv(output_dirpath, fandom), 'a') as f_out:
      storywriter = csv.writer(f_out)
      with open(chapterscsv(output_dirpath, fandom), 'a') as ch_out:
        chapterwriter = csv.writer(ch_out)
        with open(errorscsv(output_dirpath, fandom), 'a') as e_out:
            errorwriter = csv.writer(e_out)
            #does the csv already exist? if not, let's write a header row.
            if os.stat(storiescsv(output_dirpath, fandom)).st_size == 0:
                print('Writing a header row for the csv.')
                storywriter.writerow(storycolumns)
            if os.stat(chapterscsv(output_dirpath, fandom)).st_size == 0:
                print('Writing a header row for the csv.')
                chapterwriter.writerow(chaptercolumns)
            if idlist_is_csv:
                csv_fname = fic_ids[0]
                total_lines = 0

                # Count fics remaining
                with open(csv_fname, 'r') as f_in:
                    reader = csv.reader(f_in)
                    for row in reader:
                        if not row:
                            continue
                        total_lines += 1

                # Scrape fics
                with open(csv_fname, 'r+') as f_in:
                    reader = csv.reader(f_in)
                    if restart is '':
                        for row in tqdm(reader, total=total_lines, ncols=70):
                            if not row:
                                continue
                            write_fic_to_csv(fandom, row[0], only_first_chap, storywriter, chapterwriter, errorwriter, storycolumns, chaptercolumns, headers, output_dirpath)
                    else: 
                        found_restart = False
                        for row in tqdm(reader, total=total_lines, ncols=70):
                            if not row:
                                continue
                            found_restart = process_id(row[0], restart, found_restart)
                            if found_restart:
                                write_fic_to_csv(fandom, row[0], only_first_chap, storywriter, chapterwriter, errorwriter, storycolumns, chaptercolumns, headers, output_dirpath=output_dirpath)
                            else:
                                print('Skipping already processed fic')

            else:
                for fic_id in fic_ids:
                    write_fic_to_csv(fandom, fic_id, only_first_chap, storywriter, chapterwriter, errorwriter, storycolumns, chaptercolumns, headers, output_dirpath=output_dirpath) 
main()
