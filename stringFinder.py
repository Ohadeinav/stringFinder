

from threading import Thread , Lock
from collections import defaultdict

# Location class to contain the line and char offsets of a specific occurence of a particular string in the  text
# Fields: 1) linOff - line Offset from start of file 2) charOff - char Offset from start of file
class Loc:
    def __init__(self, lin_off, char_off):
        self.linOff = lin_off
        self.charOff = char_off
    def print(self): # not exactly the requested format, but this way is clearer
        print("[lineOffset=" + str(self.linOff) + ",charOffset=" + str(self.charOff) + "]")

# match Class will store the file and strings from the input of the program, and provide all member functions
# necessary to parse, match, aggregate, and print the occurences of strings in the file
class match:
    def __init__(self, inFile, strings):
        self.mapList = []   # a list to store the maps returned by all the threads
                            # map format: Key - string : value - List of Loc objects
        self.mutex = Lock() # Lock for the mapList
        self.inFile = inFile  # the file being processed
        self.strings = strings # the strings to match

    # this internal member function finds all occurences of a given string in a particular line
    # and returns them as a list of indexes (offset from start of the line)
    # used by matcher (also internal)
    def __find_all(self, a_string, sub):
        result = []  # list contaning indexes where occurences start
        k = 0
        while k < len(a_string):
            k = a_string.find(sub, k)
            if k == -1:
                return result
            else:
                result.append(k)
                k += len(sub)  # change to k += 1 to search for overlapping results
        return result

    # __matcher method receives a buffer of lines from the file (in our case 1000) and appends to the mapList a map
    # of occurences of all of the given strings in the file
    # the map has the form, Key - string : Value - List of Loc Objects
    # Note: this is an internal function that will run in parallel in multiple threads
    # @params:buff-buffer of lines to process. linOff, charOff-lin and char Offset of buffer from beginning of text

    def __matcher(self, buff, linOff, charOff):
        # create a new map with a dynamic list as values
        map = defaultdict(list)
        # fill up map by finding occurences
        for line in buff:
            for s in self.strings:
                indexes = self.__find_all(line, s) # list of indexes of occurences
                for i in indexes:
                    loc = Loc(linOff,charOff + i) # create Loc object with relative offset
                    map[s].append(loc) # add to the list for the current string in the map

            linOff += 1
            charOff += len(line)

        self.mutex.acquire() # lock the mapList
        self.mapList.append(map)
        self.mutex.release() # unlock the mapList

    # the runMatch method splits the file into blocks of 1000 lines each and sends them concurrently to the matcher
    # to find occurences of the given strings. At the end, it conjoins the results of the threads and prints
    # all the occurences in the text
    def runMatch(self):
        f = open(self.inFile ,'r')
        linOff, charOff = 0,0 # initialize lineOffset and charOffset to send to the matcher for each string
        lines = f.readlines() # split the file into a list of lines
        f.close()
        linCount = len(lines)
        threadList=[]
        i=0
        while i < linCount:  # generate lists of 1000 lines each and send to the matcher
            if linCount-i < 1000: # end case of approaching the end of the text file
                buff = lines[i:i+linCount-i]
            else:
                buff = lines[i:i + 1000]
            # create a thread to send the buffer to the matcher and start locating occurences
            currThread = Thread(target=self.__matcher(buff,linOff,charOff))
            threadList.append(currThread)
            currThread.start()
            # increment the linOff and charOff
            i += 1000
            linOff = i
            for line in buff:
                charOff += len(line)
        # wait for all threads to finish
            for t in threadList:
                t.join()
        # aggregate the mapList and print the results
        self.__aggregAndPrint()

    def __aggregAndPrint(self):
        dict = defaultdict(list) # temporary map to conjoin all the lists for each string
        for map in self.mapList:
            for k,v in map.items():
                dict[k].extend(v)
        for k,v in dict.items():
            print(k + " --> ")
            for i in v:
                i.print()

if __name__ == "__main__":
    matcher = match("big.txt", ["James","John","Robert","Michael","William",
                                    "David","Richard","Charles","Joseph","Thomas","Christopher","Daniel","Paul","Mark",
                                    "Donald","George","Kenneth","Steven","Edward",
                                    "Brian","Ronald","Anthony","Kevin","Jason","Matthew","Gary","Timothy","Jose",
                                    "Larry","Jeffrey","Frank","Scott","Eric","Stephen","Andrew",
                                    "Raymond","Gregory","Joshua","Jerry","Dennis","Walter","Patrick",
                                    "Peter","Harold","Douglas","Henry","Carl","Arthur","Ryan","Roger"])
    matcher.runMatch()

