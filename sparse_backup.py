#!/usr/bin/env python3

import argparse

from hashlib import md5
import os, json
import shutil

debug = True

def log(message):
    if debug:
        print(message)

class UI:
    '''
    Later...
    '''
    def show(self):
        pass

class Replicator:
    '''
    The class that replicates one folder to another
    '''
    def __init__(self,
        conf_file = False,
        src_root = os.path.join(os.environ['HOME'], 'work', 'hd-mirror', 'tests', 'test-src'),
        dst_root = os.path.join(os.environ['HOME'], 'work', 'hd-mirror', 'tests', 'test-dst'),
        ignore_dotfiles = True,
        clean_src = False,
        ignore_list = [],
        risky=True):
        #TODO - implement a configuration file
        self.conf_file = conf_file

        #Class attributes - important
        self.ignore_dotfiles = ignore_dotfiles
        self.ignore_list = ignore_list
        self.src_root = src_root
        self.dst_root = dst_root
        self.src_tree = []
        self.dst_tree = []
        self.risky = risky

        #Utility variables not important to the class
        self.hashing_iter = 0

    def chunks(self, filename, chunksize):
        '''
        During the binary read, bail out at about 60Mb
        Need to check the math... but the basis is there.
        '''
        f = open(filename, mode='rb')
        buf = "junk"
        #Bail out on large files after 60 Mb or so
        while len(buf) and self.hashing_iter < 14648:
            self.hashing_iter += 1
            buf = f.read(chunksize)
            yield buf

    def md5sum(self, filename):
        '''
        Sum a file.
        '''
        d = md5()
        for buf in self.chunks(filename, 4096):
            d.update(buf)
        return d.hexdigest()


    def listdir(self, uri):
        '''
        Drop through both folders and build the dicts
        '''
        temp_list = []
        unclean = os.listdir(uri)
        for item in unclean:
            save = False
            full_item = os.path.join(uri, item)
            if self.ignore_dotfiles and (item[0] == '.'):
                continue
            elif os.path.islink(full_item):
                continue
            elif full_item in self.ignore_list:
                continue
            elif os.path.isdir(full_item):
                child_list = self.listdir(full_item)
                child_list_hash = ''
                for item_b in child_list:
                    try:
                        for item_a in item_b['dir']:
                            child_list_hash += item_a['md5sum']
                    except:
                        child_list_hash += item_b['md5sum']
                child_list_hash = md5(child_list_hash.encode('utf-8')).hexdigest()
                temp = {'name': full_item, 'dir' : child_list, 'md5sum' : child_list_hash, 'synchronized': False}
            else:
                child_list_hash = self.md5sum(full_item)
                temp = {'name': full_item, 'dir' : False, 'md5sum' : child_list_hash, 'synchronized': False}
            temp_list.append(temp)
        return temp_list

    def flatten_dst(self, tree):
        '''
        Grab all hashes to make a flat list for if in
        '''
        file_hashes = []
        dir_hashes = []
        try:
            for item in tree['dir']:
                file_tmp, dir_tmp = self.flatten_dst(item)
                for item_a in dir_tmp:
                    dir_hashes.append(item_a)
                for item_a in file_tmp:
                    file_hashes.append(item_a)
            dir_hashes.append(tree['md5sum'])
        except:
            file_hashes.append(tree['md5sum'])

        return file_hashes, dir_hashes


    def dedupe(self, hash_list):
        '''
        Bounce the list through a dict and back to dedupe it.
        '''
        dedupe = {}
        for item in hash_list:
            dedupe[item] = 1
        hash_list = []
        for item in dedupe.keys():
            hash_list.append(item)
        return hash_list


    def inventory (self):
        '''
        Build source and destination trees with md5sums of all folders and directories
        '''
        log('Calculating source hashes')
        self.src_tree = self.listdir(self.src_root)
        log(self.src_tree)
        log('Calculating destination hashes')
        self.dst_tree = self.listdir(self.dst_root)
        log(self.dst_tree)
        log('Flattening destination hash tree')
        file_hashes, dir_hashes = [], []
        for item in self.dst_tree:
            temp_file_hashes, temp_folder_hashes = self.flatten_dst(item)
            for item_a in temp_file_hashes:
                file_hashes.append(item_a)
            for item_a in temp_folder_hashes:
                dir_hashes.append(item_a)
        self.dst_flattened_files = self.dedupe(file_hashes)
        self.dst_flattened_folders = self.dedupe(dir_hashes)
        log((self.dst_flattened_files, self.dst_flattened_folders))

    def check_by_hash(self, tree):
        '''
        Grab all hashes to make a flat list for if in
        '''
        try:
            for item in tree['dir']:
                self.check_by_hash(item)
            if tree['md5sum'] in self.dst_flattened_folders:
                tree['synchronized'] = True
        except:
            if tree['md5sum'] in self.dst_flattened_files:
                tree['synchronized'] = True


    def file_folder_check_pass(self):
        '''
        Check if a folder or file hash is in the dst folder
        structure anywhere.
        '''
        log('Testing crossover of files and folders by hash')
        for item in self.src_tree:
            self.check_by_hash(item)
        log(json.dumps(self.src_tree, sort_keys=True, indent=4))

    def reverse_tree_walk(self, tree):
        '''
        Walk the tree from leaf to root
        '''
        synced = False
        synced_folder = 0
        max_width = len(tree)
        try:
            for item in tree['dir']:
                if self.reverse_tree_walk(item):
                    synced_folder += 1
            if synced_folder == max_width:
                tree['synchronized'] = True
                synced = True
        except:
            synced = tree['synchronized']
        return synced


    def directory_check_scatter(self):
        '''
        Check if all contents of a folder are synced.
        If so, then the folder is synced even if the files all
        moved around.

        Bug - chokes on empty folders...
        '''
        log('Testing crossover of folders with scatter')
        for item in self.src_tree:
            self.reverse_tree_walk(item)

    def compare(self):
        '''
        Compare md5sums of files and folders
        '''
        self.file_folder_check_pass()
        self.directory_check_scatter()

    def repli_copy(self, tree):
        '''
        Copies from source to destination maintaining folder structure from origin
        Overwrites destination if exists
        '''
        try:
            for item in tree['dir']:
                self.repli_copy(item)
        except:
            if os.path.isdir(tree['name']):
                return
            if tree['synchronized']:
                return
            head, tail = os.path.split(os.path.join(self.dst_root, tree['name'].split(self.src_root)[1][1:]))
            os.makedirs(head, exist_ok=True)
            if self.risky:
                log("Trying to copy:")
                log(tree['name'])
                log("to:")
                log(os.path.join(head, tail))
                shutil.copy(tree['name'], os.path.join(head, tail))
            else:
                log("Safe copies not yet implemented")

    def replicate(self):
        '''
        Replicate anything not marked as synchronized and start over
        '''
        for item in self.src_tree:
            self.repli_copy(item)

    def run(self):
        '''
        Obvious
        '''
        self.inventory()
        self.compare()
        self.replicate()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='sparse_backup.py',
        description='''A simple utility to replicate a directory tree
            into another folder, omitting files that might have been 
            copied already.''',)
    parser.add_argument('-s', '--source', type=str, help='Source directory for the copy')
    parser.add_argument('-d', '--destination', type=str, help='Destination directory for the copy')
    parser.add_argument('-c', '--conf', type=str, help='Path to a configuration file')
    parser.add_argument('-i', '--no-ignore-dotfiles', dest='ignore_dotfiles', action='store_false', help='Do not ignore dotfiles')
    parser.set_defaults(ignore_dotfiles=True)
    parser.set_defaults(conf='.config/sparse_backup.json')
    parser.set_defaults(source=(os.path.join(os.environ['HOME'], 'work', 'hd-mirror', 'tests', 'test-src')))
    parser.set_defaults(destination=(os.path.join(os.environ['HOME'], 'work', 'hd-mirror', 'tests', 'test-dst')))
    parser.add_argument('OPERATION', metavar='OPERATION', type=str, nargs='1', help='Operation to be performed.  Available options include dedupe and backup.')
    '''
    parser.add_argument('-', '--', type=str, help='')
    parser.add_argument('-', '--', type=str, help='')
    parser.add_argument('-', '--', type=str, help='')
    parser.add_argument('-', '--', type=str, help='')
    parser.add_argument('-', '--', type=str, help='')
    '''
    args = parser.parse_args()
    replicate = Replicator(conf_file=args.conf, 
                            src_root=args.source, 
                            dst_root=args.destination, 
                            ignore_dotfiles=args.ignore_dotfiles)
    replicate.run()
