"""
Various Readers for the Nutch EcoSystem
"""

from __future__ import absolute_import, division, print_function

from .JVM import gateway

class GeneralReader(object):
    gateway = gateway

    def read(self, path=None, auto_convert=True):
        """
        method to read entire contents of a file

        Parameters
        ----------
        path : str

        Returns
        -------
        list of strings
        """

        data = self.reader.read(path)
        if auto_convert:
            data = [dict(d) for d in data]
        return data

    def head(self, nrows=5, path=None, auto_convert=True):
        """
        method to read first n-rows of a file

        Parameters
        ----------
        nrows : int
        path : str

        Returns
        -------
        list of strings
        """

        data = self.reader.head(nrows, path)
        if auto_convert:
            data = [dict(d) for d in data]

        return data

    def slice(self, start, stop, path=None, auto_convert=True):
        """
        method to slice (start, stop) contents of a file

        Parameters
        ----------
        nrows : int
        path : str

        Returns
        -------
        list of strings
        """

        data = self.reader.slice(start, stop, path)
        if auto_convert:
            data = [dict(d) for d in data]

        return data

    def count(self,path=None, auto_convert=True):
        """
        method to count number of rows in a file

        Parameters
        ----------
        path : str

        Returns
        -------
        long

        """

        count = self.reader.count(path)
        if auto_convert:
            data = long(count)

        return data


class SequenceReader(GeneralReader):
    """
    Generalized sequence file reader
    """

    def __init__(self):
        #grab object from jvm
        self.reader = self.gateway.jvm.SequenceReader

    def read(self, path=None, auto_convert=True):
        """
        method to read entire contents of a sequence file

        Parameters
        ----------
        path : str

        Returns
        -------
        list of strings
        """

        data = self.reader.read(path)
        if auto_convert:
            data = [list(d) for d in data]

        return data

    def head(self, nrows=5, path=None, auto_convert=True):
        """
        method to read first n-rows of a sequence file

        Parameters
        ----------
        nrows : int
        path : str

        Returns
        -------
        list of strings
        """

        data = self.reader.head(nrows, path)
        if auto_convert:
            data = [list(d) for d in data]

        return data

    def slice(self, start, stop, path=None, auto_convert=True):
        """
        method to slice (start, stop) contents of a sequence file

        Parameters
        ----------
        nrows : int
        path : str

        Returns
        -------
        list of strings
        """

        data = self.reader.slice(start, stop, path)
        if auto_convert:
            data = [list(d) for d in data]

        return data

    def count(self,path=None, auto_convert=True):
        """
        method to count number of rows in a sequence file

        Parameters
        ----------
        path : str

        Returns
        -------
        long

        """

        count = self.reader.count(path)
        if auto_convert:
            data = long(count)

        return data

class LinkReader(GeneralReader):
    """
    Link Reader
    """

    def __init__(self):
        #grab object from jvm
        self.reader = self.gateway.jvm.LinkReader
        self.schema = '{key_url: string, url: string, anchor: string, score: float64, ' \
                      'timestamp: int64, linktype: string}'


class NodeReader(GeneralReader):
    """
    Link Reader
    """

    def __init__(self):
        #grab object from jvm
        self.reader = self.gateway.jvm.NodeReader
        self.schema = '{key_url: string, num_inlinks: int64, num_outlinks: int64, ' \
                      'inlink_score: float, outlink_score: float64, metadata: string}'


    def hello(self):
        self.reader.hello()

sequence_reader = SequenceReader()
link_reader = LinkReader()
node_reader = NodeReader()
