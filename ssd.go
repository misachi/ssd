package main

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type RequestT byte

const (
	READ  RequestT = 'R'
	WRITE RequestT = 'W'
)

const (
	PAGE_SIZE        = 4096
	BLOCK_SIZE       = 1 << 20
	PLANE_SIZE       = 1 << 23
	CHIP_SIZE        = 1 << 25
	MAX_INFLIGHT_REQ = 24
)

type page struct {
	data   []byte
	offset uint64 // Meh!
}

func newPage() *page {
	return &page{data: make([]byte, PAGE_SIZE), offset: 0}
}

type physPageAddr struct {
	chip uint16
	addr uint64
}

type block struct {
	pages         []*page
	lastWritePage int32
	nrWrites      int32 // Keep track of writes to block to help with wear leveling
	nrFreePages   uint32
}

func newBlock(nrPages uint32) *block {
	blk := &block{pages: make([]*page, nrPages), nrFreePages: nrPages}

	var i uint32 = 0
	for i < nrPages {
		blk.pages[i] = newPage()
		i += 1
	}
	return blk
}

type plane struct {
	blocks      []*block
	nrFreePages uint32
}

func newPlane(nrBlocks uint16, pagePerBlock uint32) *plane {
	plane := &plane{blocks: make([]*block, nrBlocks), nrFreePages: pagePerBlock * uint32(nrBlocks)}

	var i uint16 = 0
	for i < nrBlocks {
		plane.blocks[i] = newBlock(pagePerBlock)
		i += 1
	}
	return plane
}

type chip struct {
	planes      []*plane
	nrFreePages uint32
}

func newChip(nrPlanes, blockPerPlane uint16, pagePerBlock uint32) *chip {
	chip := &chip{
		planes:      make([]*plane, nrPlanes),
		nrFreePages: pagePerBlock * uint32(blockPerPlane) * uint32(nrPlanes),
	}

	var i uint16 = 0
	for i < nrPlanes {
		chip.planes[i] = newPlane(blockPerPlane, pagePerBlock)
		i += 1
	}
	return chip
}

type channel struct {
	chips []*chip     // Single channel / chip
	mu    *sync.Mutex // Channel lock
}

func newChannel(nrChips, planePerChip, blockPerPlane uint16, pagePerBlock uint32) *channel {
	channel := &channel{chips: make([]*chip, nrChips), mu: &sync.Mutex{}}

	var i uint16 = 0
	for i < nrChips {
		channel.chips[i] = newChip(planePerChip, blockPerPlane, pagePerBlock)
		i += 1
	}
	return channel
}

type request struct {
	rType          RequestT // request type
	offset         uint64 // Where to start writing data
	completionChan chan int // Channel for communicating completion of requests(both read/write)
	data           []byte
}

type SSD struct {
	qHead         atomic.Int32
	blockSize     uint32
	planeSize     uint32
	chipSize      uint32
	pagePerBlock  uint32
	blockPerPlane uint16
	planePerChip  uint16
	nrChips       uint16
	nrPlanes      uint16
	nrPages       uint64
	nrBlocks      uint64
	size          uint64
	freeSpace     uint64
	channels      []*channel
	reqQueue      []*request
	qTail         atomic.Int32
	mu            *sync.Mutex
	addressMap    map[uint64]*physPageAddr
}

func NewSSD(devSize uint64) *SSD {
	nr_channels := devSize / CHIP_SIZE
	var blockSize uint32 = BLOCK_SIZE
	var planeSize uint32 = PLANE_SIZE
	var chipSize uint32 = CHIP_SIZE
	var i uint16 = 0

	SSD := &SSD{
		blockSize:     blockSize,
		planeSize:     planeSize,
		chipSize:      chipSize,
		pagePerBlock:  blockSize / PAGE_SIZE,
		blockPerPlane: uint16(planeSize / blockSize),
		planePerChip:  uint16(chipSize / planeSize),
		nrChips:       uint16(nr_channels),
		size:          devSize,
		freeSpace:     devSize,
		channels:      make([]*channel, nr_channels),
		reqQueue:      make([]*request, MAX_INFLIGHT_REQ),
		mu:            &sync.Mutex{},
	}

	SSD.nrChips = uint16(nr_channels)
	SSD.nrPlanes = SSD.nrChips * SSD.planePerChip
	SSD.nrBlocks = uint64(SSD.nrPlanes * SSD.blockPerPlane)
	SSD.nrPages = SSD.nrBlocks * uint64(SSD.pagePerBlock)
	SSD.addressMap = make(map[uint64]*physPageAddr)

	SSD.qHead.Store(0)
	SSD.qTail.Store(0)

	for i < uint16(nr_channels) {
		SSD.channels[i] = newChannel(SSD.nrChips, SSD.planePerChip, SSD.blockPerPlane, SSD.pagePerBlock)
		i += 1
	}

	return SSD
}

func physicalPageAddr(dev *SSD, logAddr uint64) (*physPageAddr, error) {
	physAddr, ok := dev.addressMap[logAddr]
	if !ok {
		return nil, fmt.Errorf("physicalPageAddr address not found: %d", logAddr)
	}
	return physAddr, nil
}

func mapPhysPage(dev *SSD, addr uint64) *physPageAddr {
	for i := 0; i < int(dev.nrChips); i++ {
		ch := dev.channels[i]
		chip := ch.chips[i]
		if chip.nrFreePages > 0 {
			for j := 0; j < int(dev.planePerChip); j++ {
				plane := chip.planes[j]
				if plane.nrFreePages > 0 {
					for k := 0; k < int(dev.blockPerPlane); k++ {
						block := plane.blocks[k]
						if block.nrFreePages > 0 {
							for z := 0; z < int(dev.pagePerBlock); z++ {
								page := block.pages[z]
								if page.offset < 1 {
									physAddr := new(physPageAddr)
									physAddr.addr = uint64(j*int(dev.planeSize) + k*int(dev.blockSize) + z*PAGE_SIZE)
									physAddr.chip = uint16(i)
									dev.addressMap[addr] = physAddr

									page.offset += PAGE_SIZE

									chip.nrFreePages--
									plane.nrFreePages--
									block.nrFreePages--
									dev.freeSpace -= PAGE_SIZE
									return physAddr
								}
							}
						}
					}
				}
			}
		}
	}
	return nil
}

func mapPhysPages(dev *SSD, addr, nrPages uint64) []*physPageAddr {
	addrList := make([]*physPageAddr, nrPages)
	for i := 0; i < int(dev.nrChips); i++ {
		ch := dev.channels[i]
		chip := ch.chips[i]
		if chip.nrFreePages > 0 {
			for j := 0; j < int(dev.planePerChip); j++ {

			recheck_plane:
				plane := chip.planes[j]
				if plane.nrFreePages > 0 {
					for k := 0; k < int(dev.planePerChip); k++ {
						block := plane.blocks[k]
						if block.nrFreePages > 0 {
							physAddr := new(physPageAddr)
							physAddr.addr = uint64(j * k * PAGE_SIZE)
							physAddr.chip = uint16(i)
							dev.addressMap[addr] = physAddr
							addrList = append(addrList, physAddr)

							chip.nrFreePages--
							plane.nrFreePages--
							block.nrFreePages--
							dev.freeSpace -= PAGE_SIZE

							if uint64(len(addrList)) < nrPages && chip.nrFreePages > 0 && (j+1) < int(dev.planePerChip) {
								j++
								goto recheck_plane
							}

							if uint64(len(addrList)) >= nrPages {
								return addrList
							}
						}
					}
				}
			}
		}
	}
	return nil
}

func getPage(dev *SSD, physAddr *physPageAddr) *page {
	channel := dev.channels[physAddr.chip]
	chip := channel.chips[physAddr.chip]
	planeN := physAddr.addr / uint64(dev.planeSize)
	plane := chip.planes[planeN]
	remainderByte := physAddr.addr - (planeN * uint64(uint32(dev.blockPerPlane)*dev.blockSize))
	blockN := uint64(math.Ceil(float64(remainderByte) / float64(dev.blockSize)))
	block := plane.blocks[blockN]
	pageN := remainderByte / PAGE_SIZE
	return block.pages[pageN]
}

func (ssd *SSD) reqSize(tail, head int32) int32 {
	qSize := len(ssd.reqQueue)
	if tail < head {
		return (int32(qSize) - head) + tail
	}

	return tail - head
}

func (ssd *SSD) addRequest(req *request) error {
retry:
	curPos := ssd.qTail.Load()

	if curPos < int32(len(ssd.reqQueue)) {
		nextPos := curPos + 1

		if !ssd.qTail.CompareAndSwap(curPos, nextPos) {
			goto retry // Something changed
		}
		ssd.reqQueue[curPos] = req

		return nil
	}
	// fmt.Printf("Type: %q, Data: %d\n", req.rType, req.data[0])
	return fmt.Errorf("Request queue is full")
}

func (ssd *SSD) nextRequest() *request {
retry:
	curPos := ssd.qHead.Load()
	tail := ssd.qTail.Load()
	if curPos != tail { // Empty?
		nextPos := curPos + 1
		if nextPos >= int32(len(ssd.reqQueue)) {
			nextPos = 0
		}

		if !ssd.qHead.CompareAndSwap(curPos, nextPos) {
			goto retry // Something changed
		}

		tail := ssd.qTail.Load()

		head := ssd.qHead.Load()
		if tail >= int32(len(ssd.reqQueue)) && head != 0 {
			ssd.qTail.CompareAndSwap(tail, 0)
		}

		return ssd.reqQueue[curPos]
	}

	// log.Println("Request queue is empty")
	return nil
}

func (ssd *SSD) SubmitIO(req *request) error {
	return ssd.addRequest(req)
}

func (ssd *SSD) handleIO(ctx *context.Context) error {
	req := ssd.nextRequest()
	if req != nil {
		err := ssd.doIO(req)
		if err != nil {
			return fmt.Errorf("handleIO: Error when handling IO request: %v", err)
		}
	}
	return nil
}

func testrun() {
	devSize := 1 << 30 // 1 GB
	ssd := NewSSD(uint64(devSize))

	ctx := context.Background()
	cChan := make(chan int)
	sChan := make(chan int)

	/* Runs the main loop that services incoming request */
	go func(ctx2 *context.Context, ssd *SSD, ch chan int) {
		for {
			ssd.handleIO(ctx2)

			select {
			case <-ch:
				break
			default:
				time.Sleep(2 * time.Microsecond)
			}
		}
	}(&ctx, ssd, sChan)

	/* Sample client/user program writing sending commands to SSD device
	 * Send N write requests and a single read request which will be displayed
	 * in the `print` function below
	 */
	go func(ctx2 *context.Context, ssd *SSD, ch chan int) {
		for j := 0; j < 100; j++ {
			data := make([]byte, PAGE_SIZE)
			data[0] = byte(j)
			data[PAGE_SIZE-1] = byte(j)
			req := request{rType: WRITE, data: data, offset: uint64(j * PAGE_SIZE), completionChan: make(chan int)}

			for {
				err := ssd.SubmitIO(&req)
				if err == nil {
					break
				}
			}
		}

		data := make([]byte, PAGE_SIZE)
		req := request{rType: READ, data: data, offset: uint64(98 * PAGE_SIZE), completionChan: make(chan int)}

		/* Retry read request: Request queue is full */
		for {
			err := ssd.SubmitIO(&req)
			if err == nil {
				break
			}
		}

		<-req.completionChan // Block on read
		fmt.Printf("Read: %d\n", data[0])

		close(ch)

	}(&ctx, ssd, cChan)

	<-cChan

	close(sChan)

}

func (ssd *SSD) doIO(req *request) error {
	// TODO: Submit IO exceeding PAGE_SIZE bytes; Use mapPhysPages for writes
	switch req.rType {
	case READ:
		physAddr, err := physicalPageAddr(ssd, req.offset)
		if err != nil {
			return fmt.Errorf("submitIO: read to addr=%d error: %v", req.offset, err)
		}

		page := getPage(ssd, physAddr)
		if page == nil {
			return fmt.Errorf("submitIO: unable to get page with addr=%d", req.offset)
		}
		copy(req.data, page.data)
		req.completionChan <- 1
		return nil
	case WRITE:
		physAddr, err := physicalPageAddr(ssd, req.offset)
		if err != nil {
			physAddr = mapPhysPage(ssd, req.offset)
			if physAddr == nil {
				return fmt.Errorf("submitIO: unable to write to addr=%d: %v", req.offset, err)
			}
		}

		page := getPage(ssd, physAddr)
		if page == nil {
			return fmt.Errorf("submitIO: unable to get page with addr=%d", req.offset)
		}
		copy(page.data, req.data)
		return nil
	default:
		return fmt.Errorf("submitIO: Unrecognized IO command")
	}
}
