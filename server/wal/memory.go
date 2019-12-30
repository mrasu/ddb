package wal

import "io"

type Memory struct {
	Logs [][]byte

	pos int
}

func (m *Memory) Write(p []byte) (n int, err error) {
	m.Logs = append(m.Logs, p)

	return len(p), nil
}

func (m *Memory) Read(p []byte) (n int, err error) {
	pos := 0
	pNum := 0
	for _, log := range m.Logs {
		if pos+len(log) > m.pos {
			for _, b := range log[m.pos-pos:] {
				p[pNum] = b
				pNum += 1
				m.pos += 1
				if pNum == len(p) {
					return pNum, nil
				}
			}
		}
		pos += len(log)
	}

	if pNum == 0 {
		return 0, io.EOF
	} else {
		return pNum, nil
	}
}

func (m *Memory) Close() error {
	return nil
}

func (m *Memory) Clear() {
	m.Logs = [][]byte{}
}
