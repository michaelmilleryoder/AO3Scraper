#!/usr/bin/env python
# coding: utf-8

"""
    This script has 2 goals:
        1. Combine fic IDs that were separately scraped
        2. Exclude fic IDs that were already scraped (to update a fandom's IDs, e.g.)

    @author Michael Miller Yoder
    @date 2020
"""

import os
import argparse
import pandas as pd


class FicIdManipulator():

    def __init__(self, outpath, sections_dirpath, scraped_path, exclude_path):
        self.outpath = outpath
        self.sections_dirpath = sections_dirpath
        self.scraped_path = scraped_path
        self.exclude_path = exclude_path
        self.scraped_fic_ids = None
        self.output_fic_ids = None

    def manipulate(self):
        if self.sections_dirpath: self.combine_fic_ids()
        if self.exclude_path: self.exclude_fic_ids()

    def combine_fic_ids(self):
        # Load scraped fic IDs
        print("Combining scraped fic IDs...")
        self.scraped_fic_ids = pd.DataFrame()
        for fname in os.listdir(self.sections_dirpath):
            fic_ids_path = os.path.join(self.sections_dirpath, fname)
            fic_ids_data = pd.read_csv(fic_ids_path, index_col=0, header=None)
            self.scraped_fic_ids = pd.concat([self.scraped_fic_ids, fic_ids_data])

    def exclude_fic_ids(self):
        """ Exclude fic_ids that were already scraped """
        print("Removing fic IDs that were already scraped...")

        # Load fic IDs of data already scraped
        if self.scraped_fic_ids is None:
            self.load_scraped_fic_ids()
        print(f"\tNumber of scraped fic IDs: {len(self.scraped_fic_ids)}")
        existing_metadata = pd.read_csv(self.exclude_path)
        new_fic_ids = list(set(self.scraped_fic_ids.index) - set(existing_metadata['fic_id']))
        self.output_fic_ids = self.scraped_fic_ids.loc[new_fic_ids,:]
        print(f"\tNumber of new fic IDs: {len(self.output_fic_ids)}")

    def load_scraped_fic_ids(self):
        self.scraped_fic_ids = pd.read_csv(self.scraped_path, index_col=0, header=None)

    def save_fic_ids(self):
        """ Save out new fic ids """
        self.output_fic_ids.to_csv(self.outpath, header=False)
        print(f"Output fic IDs saved to {self.outpath}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('outpath', nargs='?',
            default=None,
            help='Path where the output CSV of work IDs will be saved. This can then be used to scrape the actual fic texts.')
    parser.add_argument('--sections-dirpath', dest='sections_dirpath', nargs='?',
            default=None,
            help='If combining fic IDs, path to the directory where the fic CSVs are')
    parser.add_argument('--scraped-path', dest='scraped_path', nargs='?',
            default=None,
            help='If not combining fic IDs, path to the 1 scraped fic CSV')
    parser.add_argument('--exclude-path', dest='exclude_path', nargs='?', 
            default=None,
            help='Path to the metadata CSV of stories that are already scraped')
    args = parser.parse_args()

    manipulator = FicIdManipulator(args.outpath, args.sections_dirpath, args.scraped_path, args.exclude_path)
    manipulator.manipulate()
    manipulator.save_fic_ids()

if __name__ == '__main__':
    main()
