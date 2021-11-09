/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
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
  //increments clockHand to next position
  if(clockHand == numBufs-1){
      clockHand = 0;
  }else{
      clockHand++;
  }
}

void BufMgr::allocBuf(FrameId& frame) {
   //TODO: 
   //-Consider case where dirty bit page needs to be written to disk
   //Go through all frames twice(in the case that no frame has ref=0 on first cycle)
   for(unsigned int i = 0; i < numBufs*2; i++){
      //Case if frame is not valid
      if(bufDescTable[clockHand].valid == false){
	 frame = clockHand;
	 bufDescTable[clockHand].pinCnt++;
	 bufDescTable[clockHand].valid = true;
	 return;
      //Case if a frame is available to pick
      }else if(bufDescTable[clockHand].refbit == false && bufDescTable[clockHand].pinCnt == 0){
         frame = clockHand;
	 //Writes to file in case that the frame is dirty
	 if(bufDescTable[clockHand].dirty == true){
	    //write to disk and set dirty to false
	 }
	 //Remove current hashtable entry at frame
	 try{
	   hashTable.remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
	 }catch(HashNotFoundException const&){}
	 //Increment pin count
	 bufDescTable[clockHand].pinCnt++;
	 return;
      //case if ref needs to be reset
      }else if(bufDescTable[clockHand].refbit == true){
         bufDescTable[clockHand].refbit = false;
	 advanceClock();
      }else{ //march forwards
         advanceClock();
      }
   }
   //Case if no frames are available
   throw BufferExceededException();
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
   *page = file.allocatePage();
   FrameId fId = -1;
   allocBuf(fId); //allocate the frame
   std::cout << fId << std::flush; //delete this
   //TODO:
   //insert to hash
   //Call set
}

void BufMgr::flushFile(File& file) {}

void BufMgr::disposePage(File& file, const PageId PageNo) {}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
