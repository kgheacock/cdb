#include "pfm.h"
#include <memory>
#include <fstream>

bool fileExists(const string &fileName) {
    struct stat buf;
    return stat(fileName.c_str(), &buf) == 0 ? true : false;
}

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
    // TODO: fix valgrind errors/warnings.
}


RC PagedFileManager::createFile(const string &fileName)
{
    if (fileExists(fileName) || !ofstream(fileName.c_str(), ofstream::out | ofstream::binary)) {
        return -1;
    }
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if (!fileExists(fileName)) {
        return -1;
    }
    // Remove returns non-zero value on failure.
    return remove(fileName.c_str());
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if (!fileExists(fileName)) {
        return -1;
    }

    auto fsMode = fstream::in | fstream::out | fstream::binary;
    unique_ptr<fstream> fs {new fstream(fileName.c_str(), fsMode)};
    if (!fs) {
        return -1;
    }

    fileHandle.fs = std::move(fs);
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if (!fileHandle.isHandling()) {
        return -1;
    }

    fileHandle.fs->close();
    fileHandle.fs = nullptr;
    return 0;
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    return -1;
}


RC FileHandle::appendPage(const void *data)
{
    return -1;
}


unsigned FileHandle::getNumberOfPages()
{
    if (!isHandling()) {
        return 0;
    }

    // Used for restoring our position so we don't have a side effect.
    auto initialPosition = fs->tellg();
    if (initialPosition == -1) {
        return 0;
    }

    // Get file length.
    fs->seekg(0, fs->end); // Seek to the end.
    auto len = fs->tellg();
    if (len == -1) {
        return 0;
    }

    fs->seekg(initialPosition); 
    
    return len / PAGE_SIZE;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
	return 0;
}

