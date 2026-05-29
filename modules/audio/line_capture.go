package audio

import (
	"bufio"
	"context"
	"io"
	"os"
	"strings"
	"time"
)

type LineCapture struct {
	name   string
	path   string
	read   io.Reader
	logf   func(string, ...any)
	format Format
}

func NewLineCapture(name string, reader io.Reader, logf func(string, ...any)) *LineCapture {
	return &LineCapture{
		name:   name,
		read:   reader,
		logf:   logf,
		format: Format{Encoding: "text/line"},
	}
}

func NewLineCaptureFromPath(path string, logf func(string, ...any)) *LineCapture {
	name := strings.TrimSpace(path)
	if name == "" {
		name = "fifo"
	}
	return &LineCapture{
		name:   name,
		path:   path,
		logf:   logf,
		format: Format{Encoding: "text/line"},
	}
}

func (c *LineCapture) Name() string {
	if strings.TrimSpace(c.name) == "" {
		return "line"
	}
	return c.name
}

func (c *LineCapture) Start(ctx context.Context) (<-chan Frame, error) {
	out := make(chan Frame, 32)
	if c.path != "" {
		go c.readPathLoop(ctx, out)
	} else {
		go c.readReader(ctx, c.read, out)
	}
	return out, nil
}

func (c *LineCapture) Close() error { return nil }

func (c *LineCapture) readReader(ctx context.Context, r io.Reader, out chan<- Frame) {
	defer close(out)
	c.scanReader(ctx, r, out)
}

func (c *LineCapture) scanReader(ctx context.Context, r io.Reader, out chan<- Frame) {
	if r == nil {
		return
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		frame := Frame{Data: []byte(line), Format: c.format, Timestamp: time.Now()}
		select {
		case out <- frame:
		case <-ctx.Done():
			return
		}
	}
	if err := scanner.Err(); err != nil && c.logf != nil {
		c.logf("capture read error: %v", err)
	}
}

func (c *LineCapture) readPathLoop(ctx context.Context, out chan<- Frame) {
	defer close(out)
	path := strings.TrimSpace(c.path)
	if path == "" {
		return
	}

	var lastInfo os.FileInfo
	var lastModTime time.Time
	var lastOffset int64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		info, err := os.Stat(path)
		if err != nil {
			if c.logf != nil {
				c.logf("capture stat failed: %v", err)
			}
			if !sleepWithContext(ctx, 500*time.Millisecond) {
				return
			}
			continue
		}

		isPipe := info.Mode()&os.ModeNamedPipe != 0
		sameFile := !isPipe && lastInfo != nil && os.SameFile(info, lastInfo)
		if sameFile && !lastModTime.IsZero() && info.ModTime().Equal(lastModTime) && info.Size() == lastOffset {
			if !sleepWithContext(ctx, 200*time.Millisecond) {
				return
			}
			continue
		}

		readOffset := int64(0)
		if sameFile && info.Size() > lastOffset {
			readOffset = lastOffset
		}

		f, err := os.Open(path)
		if err != nil {
			if c.logf != nil {
				c.logf("capture open failed: %v", err)
			}
			if !sleepWithContext(ctx, 500*time.Millisecond) {
				return
			}
			continue
		}
		if readOffset > 0 {
			if _, err := f.Seek(readOffset, io.SeekStart); err != nil {
				if c.logf != nil {
					c.logf("capture seek failed: %v", err)
				}
				_ = f.Close()
				if !sleepWithContext(ctx, 500*time.Millisecond) {
					return
				}
				continue
			}
		}

		c.scanReader(ctx, f, out)
		consumedOffset := info.Size()
		if !isPipe {
			if offset, err := f.Seek(0, io.SeekCurrent); err == nil {
				consumedOffset = offset
			}
		}
		finalInfo, _ := f.Stat()
		_ = f.Close()
		if !isPipe {
			lastInfo = info
			lastOffset = consumedOffset
			lastModTime = time.Time{}
			if finalInfo != nil && finalInfo.Size() == consumedOffset {
				lastModTime = finalInfo.ModTime()
			} else if info.Size() == consumedOffset {
				lastModTime = info.ModTime()
			}
		}

		if !sleepWithContext(ctx, 200*time.Millisecond) {
			return
		}
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
