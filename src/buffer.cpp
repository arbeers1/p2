/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 * @author Alex Beers, MOHAMED ALMARZOOQI, MOHAMED ALREMEITHI
 *
 * File: Buffer.cpp
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

void BufMgr::advanceClock() {
  // increments clockHand to next position
  if (clockHand == numBufs - 1) {
    clockHand = 0;
  } else {
    clockHand++;
  }
}

void BufMgr::allocBuf(FrameId& frame) {
  // Go through all frames twice(in the case that no frame has ref=0 on first
  // cycle)
  for (unsigned int i = 0; i < numBufs * 2; i++) {
    // Case if frame is not valid
    if (bufDescTable[clockHand].valid == false) {
      frame = clockHand;
      return;
      // Case if a frame is available to pick and valid
    } else if (bufDescTable[clockHand].refbit == false &&
               bufDescTable[clockHand].pinCnt == 0) {
      frame = clockHand;
      // Writes to file in case that the frame is dirty
      if (bufDescTable[clockHand].dirty == true) {
        bufDescTable[clockHand].file.writePage(bufPool[clockHand]);
      }
      // Remove current hashtable entry at frame
      try {
        hashTable.remove(bufDescTable[clockHand].file,
                         bufDescTable[clockHand].pageNo);
      } catch (HashNotFoundException const&) {
      }
      return;
      // case if ref needs to be reset
    } else if (bufDescTable[clockHand].refbit == true &&
               bufDescTable[clockHand].pinCnt == 0) {
      bufDescTable[clockHand].refbit = false;
      advanceClock();
    } else {  // march forwards
      advanceClock();
    }
  }
  // Case if no frames are available
  throw BufferExceededException();
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  // Checks if the page is in the pool.
  // If it is, the page is updated and returned via pointer
  // If it isn't, the page is added via allocatePage
  FrameId fId;
  try {
    hashTable.lookup(file, pageNo, fId);
    // Update the frame when it is found
    bufDescTable[fId].refbit = true;
    bufDescTable[fId].pinCnt++;
    page = &bufPool[fId];
  } catch (HashNotFoundException const&) {
    // The frame was not found so it is allocated
    allocBuf(fId);
    Page p = file.readPage(pageNo);
    // New frame inserted and values are updated
    bufPool[fId] = p;
    hashTable.insert(file, pageNo, fId);
    bufDescTable[fId].Set(file, pageNo);
    page = &bufPool[fId];
  }
}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
  FrameId fId;
  // Checks if the page exists and unpins it if so
  try {
    hashTable.lookup(file, pageNo, fId);
    // case if page is not pinned
    if (bufDescTable[fId].pinCnt == 0) {
      throw PageNotPinnedException(file.filename(), pageNo, fId);
    }
    // update pin
    bufDescTable[fId].pinCnt--;
    if (dirty) {
      bufDescTable[fId].dirty = true;
    }
  } catch (HashNotFoundException const&) {
  }
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
  Page p = file.allocatePage();
  FrameId fId;
  allocBuf(fId);  // allocate the frame
  // Assign page its frame
  bufPool[fId] = p;
  pageNo = p.page_number();
  // std::cout <<  pageNo << std::flush;
  // insert to hash
  hashTable.insert(file, pageNo, fId);
  // Set up the frame
  bufDescTable[fId].Set(file, pageNo);
  // Pointer to bufferframe page
  page = &bufPool[fId];
}

void BufMgr::flushFile(File& file) {
  for (unsigned int i = 0; i < numBufs; i++) {
    if (bufDescTable[i].file == file) {
      // fails if page is invalid
      if (bufDescTable[i].valid == false) {
        FrameId fId = i;
        throw BadBufferException(fId, bufDescTable[i].dirty,
                                 bufDescTable[i].valid, bufDescTable[i].refbit);
      }
      // fails if page is pinned
      if (bufDescTable[i].pinCnt > 0) {
        FrameId fId = i;
        throw PagePinnedException(file.filename(), bufDescTable[i].pageNo, fId);
      }
      // write if dirty
      if (bufDescTable[i].dirty == true) {
        file.writePage(bufPool[i]);
      }
      hashTable.remove(file, bufDescTable[i].pageNo);
      bufDescTable[i].clear();
    }
  }
}

void BufMgr::disposePage(File& file, const PageId PageNo) {
  // Checks if page is in the pool and removes it if so
  try {
    FrameId fId;
    hashTable.lookup(file, PageNo, fId);
    hashTable.remove(file, PageNo);
    bufDescTable[fId].clear();
  } catch (HashNotFoundException const&) {
  }
  file.deletePage(PageNo);
}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
