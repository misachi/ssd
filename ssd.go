package main

import (
	"sync"
)

type RequestT byte

const (
	READ  RequestT = 'R'
	WRITE RequestT = 'W'
)

const PAGE_SIZE = 4096

type page struct {
	data   []byte
	offset uint64 // Meh!
}

type block struct {
	pages         []*page
	lastWritePage int32
	nrWrites      int32 // Keep track of writes to block to help with wear leveling
	nrFreePages   uint64
}

type plane struct {
	blocks        []*block
	nrEmptyBlocks uint
}

type chip struct {
	blocks       []*block
	nrFreeBlocks uint
}

type channel struct {
	chips []*chip       // Single channel / chip
	mu    *sync.RWMutex // Channel lock
}

type request struct {
	rType  RequestT // request type
	offset uint64
	data   []byte
}

type ssd struct {
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
	channels      []*channel
	reqQueue      []*request
}
