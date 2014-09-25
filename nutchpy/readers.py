"""
Various Readers for the Nutch EcoSystem
"""

from __future__ import absolute_import, division, print_function

from .JVM import gateway

class GeneralReader(object):
    gateway = gateway

    def read(self, path=None):
        pass

    def head(self, nrows=5, path=None):
        pass

    def slice(self, start, stop, path):
        pass

    def count(self, path):
        pass


class SequenceReader(GeneralReader):
    """
    Generalized sequence file reader
    """

    def __init__(self):
        #grab object from jvm
        self.seq_reader = self.gateway.jvm.SequenceReader

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

        data = self.seq_reader.read(path)
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

        data = self.seq_reader.head(nrows, path)
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

        data = self.seq_reader.slice(start, stop, path)
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

        count = self.seq_reader.count(path)
        if auto_convert:
            data = long(count)

        return data

class OutlinksReader(GeneralReader):
    """
    Generalized sequence file reader
    """

    def __init__(self):
        #grab object from jvm
        self.outlinks_reader = self.gateway.jvm.OutlinkReader
        self.schema = '{timestamp: int64, key_url: string, score: float64, ' \
                      'url: string, linktype: string, anchor: string}'

    def read(self, path=None, auto_convert=True):
        """
        method to read entire contents of an outlinks file

        Parameters
        ----------
        path : str

        Returns
        -------
        list of strings
        """

        data = self.outlinks_reader.read(path)
        if auto_convert:
            data = [dict(d) for d in data]
        return data

    def head(self, nrows=5, path=None, auto_convert=True):
        """
        method to read first n-rows of an outlinks file

        Parameters
        ----------
        nrows : int
        path : str

        Returns
        -------
        list of strings
        """

        data = self.outlinks_reader.head(nrows, path)
        if auto_convert:
            data = [dict(d) for d in data]

        return data

    def slice(self, start, stop, path=None, auto_convert=True):
        """
        method to slice (start, stop) contents of an outlinks file

        Parameters
        ----------
        nrows : int
        path : str

        Returns
        -------
        list of strings
        """

        data = self.outlinks_reader.slice(start, stop, path)
        if auto_convert:
            data = [dict(d) for d in data]

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

        count = self.outlinks_reader.count(path)
        if auto_convert:
            data = long(count)

        return data


sequence_reader = SequenceReader()
outlinks_reader = OutlinksReader()
