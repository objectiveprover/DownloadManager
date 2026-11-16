// Package tui is the terminal interface for the download manager.
package tui

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"dlm/internal/engine"
)

// Run shows the TUI until the user quits or ctx is cancelled.
// webInfo is shown in the header (e.g. the browser UI address).
func Run(ctx context.Context, m *engine.Manager, webInfo string) error {
	updates, unsubscribe := m.Subscribe()
	defer unsubscribe()
	p := tea.NewProgram(newModel(m, updates, webInfo), tea.WithAltScreen(), tea.WithContext(ctx))
	_, err := p.Run()
	if errors.Is(err, tea.ErrProgramKilled) && ctx.Err() != nil {
		return nil
	}
	return err
}

var (
	titleStyle   = lipgloss.NewStyle().Bold(true)
	mutedStyle   = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "245", Dark: "243"})
	nameStyle    = lipgloss.NewStyle().Bold(true)
	okStyle      = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "28", Dark: "42"}).Bold(true)
	badStyle     = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "160", Dark: "203"}).Bold(true)
	waitStyle    = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "136", Dark: "221"})
	cursorStyle  = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "27", Dark: "75"}).Bold(true)
	labelStyle   = lipgloss.NewStyle().Bold(true)
	formBoxStyle = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1).
			BorderForeground(lipgloss.AdaptiveColor{Light: "27", Dark: "75"})
)

type updateMsg []engine.Snapshot
type closedMsg struct{}

func waitForUpdate(ch <-chan []engine.Snapshot) tea.Cmd {
	return func() tea.Msg {
		list, ok := <-ch
		if !ok {
			return closedMsg{}
		}
		return updateMsg(list)
	}
}

type mode int

const (
	modeList mode = iota
	modeAdd
	modeConfirmDelete
)

type model struct {
	mgr     *engine.Manager
	updates <-chan []engine.Snapshot
	webInfo string

	list   []engine.Snapshot
	cursor int
	width  int
	height int
	flash  string // one-line message (errors from actions)

	mode     mode
	urlIn    textinput.Model
	sumIn    textinput.Model
	formErr  string
	dlBar    progress.Model
	checkBar progress.Model
}

func newModel(mgr *engine.Manager, updates <-chan []engine.Snapshot, webInfo string) model {
	urlIn := textinput.New()
	urlIn.Placeholder = "https://example.com/file.iso"
	urlIn.Prompt = "› "
	urlIn.CharLimit = 4096

	sumIn := textinput.New()
	sumIn.Placeholder = "sha256:e3b0c44298fc1c149afbf4c8…  (or a bare md5/sha1/sha256/sha512 hash)"
	sumIn.Prompt = "› "
	sumIn.CharLimit = 300

	return model{
		mgr:      mgr,
		updates:  updates,
		webInfo:  webInfo,
		width:    80,
		height:   24,
		urlIn:    urlIn,
		sumIn:    sumIn,
		dlBar:    progress.New(progress.WithDefaultGradient(), progress.WithoutPercentage()),
		checkBar: progress.New(progress.WithSolidFill("#E0A526"), progress.WithoutPercentage()),
	}
}

func (m model) Init() tea.Cmd { return waitForUpdate(m.updates) }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		barWidth := max(10, min(50, m.width-30))
		m.dlBar.Width, m.checkBar.Width = barWidth, barWidth
		m.urlIn.Width = max(20, m.width-8)
		m.sumIn.Width = max(20, m.width-8)
		return m, nil

	case updateMsg:
		m.list = msg
		m.cursor = max(0, min(m.cursor, len(m.list)-1))
		return m, waitForUpdate(m.updates)

	case closedMsg:
		return m, tea.Quit

	case tea.KeyMsg:
		switch m.mode {
		case modeAdd:
			return m.updateForm(msg)
		case modeConfirmDelete:
			return m.updateConfirm(msg)
		}
		return m.updateList(msg)
	}

	if m.mode == modeAdd {
		return m.updateInputs(msg)
	}
	return m, nil
}

func (m model) selected() (engine.Snapshot, bool) {
	if m.cursor < 0 || m.cursor >= len(m.list) {
		return engine.Snapshot{}, false
	}
	return m.list[m.cursor], true
}

func (m model) updateList(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	m.flash = ""
	sel, ok := m.selected()
	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "up", "k":
		m.cursor = max(0, m.cursor-1)
	case "down", "j":
		m.cursor = min(len(m.list)-1, m.cursor+1)
	case "a":
		m.mode = modeAdd
		m.formErr = ""
		m.urlIn.Reset()
		m.sumIn.Reset()
		m.sumIn.Blur()
		return m, m.urlIn.Focus()
	case "p", " ":
		if ok {
			var err error
			if sel.Status == engine.StatusPaused || sel.Status == engine.StatusFailed {
				err = m.mgr.Resume(sel.ID)
			} else {
				err = m.mgr.Pause(sel.ID)
			}
			m.setFlash(err)
		}
	case "r":
		if ok {
			m.setFlash(m.mgr.Resume(sel.ID))
		}
	case "d":
		if ok {
			m.setFlash(m.mgr.Remove(sel.ID, false))
		}
	case "x":
		if ok && (sel.Status == engine.StatusDone || sel.Checksum.Status == engine.ChecksumMismatch) {
			m.mode = modeConfirmDelete
		} else if ok {
			m.flash = "x deletes a finished file; use d to remove an unfinished download"
		}
	}
	return m, nil
}

func (m *model) setFlash(err error) {
	if err != nil {
		m.flash = err.Error()
	}
}

func (m model) updateConfirm(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if sel, ok := m.selected(); ok && (msg.String() == "y" || msg.String() == "Y") {
		m.setFlash(m.mgr.Remove(sel.ID, true))
	}
	m.mode = modeList
	return m, nil
}

func (m model) updateForm(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit
	case "esc":
		m.mode = modeList
		return m, nil
	case "tab", "shift+tab", "up", "down":
		return m, m.toggleFocus()
	case "enter":
		if m.urlIn.Focused() {
			if strings.TrimSpace(m.urlIn.Value()) == "" {
				m.formErr = "Enter a URL first."
				return m, nil
			}
			// Move on to the checksum field so it's never skipped by accident;
			// pressing enter there (even when empty) adds the download.
			return m, m.toggleFocus()
		}
		snap, err := m.mgr.Add(m.urlIn.Value(), m.sumIn.Value())
		if err != nil {
			m.formErr = err.Error()
			return m, nil
		}
		m.mode = modeList
		m.list = append(m.list, snap)
		m.cursor = len(m.list) - 1
		return m, nil
	}
	return m.updateInputs(msg)
}

func (m *model) toggleFocus() tea.Cmd {
	if m.urlIn.Focused() {
		m.urlIn.Blur()
		return m.sumIn.Focus()
	}
	m.sumIn.Blur()
	return m.urlIn.Focus()
}

func (m model) updateInputs(msg tea.Msg) (tea.Model, tea.Cmd) {
	var c1, c2 tea.Cmd
	m.urlIn, c1 = m.urlIn.Update(msg)
	m.sumIn, c2 = m.sumIn.Update(msg)
	if _, isKey := msg.(tea.KeyMsg); isKey {
		m.formErr = ""
	}
	return m, tea.Batch(c1, c2)
}

// ---------- view ----------

func (m model) View() string {
	var b strings.Builder

	count := fmt.Sprintf("%d download", len(m.list))
	if len(m.list) != 1 {
		count += "s"
	}
	b.WriteString(titleStyle.Render("dlm") + mutedStyle.Render(" · "+count+" · "+m.webInfo) + "\n\n")

	var footer string
	switch m.mode {
	case modeAdd:
		b.WriteString(m.formView() + "\n")
	case modeConfirmDelete:
		sel, _ := m.selected()
		footer = badStyle.Render(fmt.Sprintf("Delete %s from disk? y/N", sel.Filename))
	}
	if footer == "" {
		footer = m.flashOrHelp()
	}

	used := lipgloss.Height(b.String()) + 2
	b.WriteString(m.listView(m.height - used))
	body := b.String()
	// Keep the help line pinned to the bottom of the screen.
	pad := max(1, m.height-lipgloss.Height(body)-lipgloss.Height(footer)+1)
	return body + strings.Repeat("\n", pad) + footer
}

func (m model) flashOrHelp() string {
	if m.flash != "" {
		return badStyle.Render(m.flash)
	}
	if m.mode == modeAdd {
		return mutedStyle.Render("enter next field / add · tab switch field · esc cancel")
	}
	return mutedStyle.Render("a add · p pause/resume · r retry · d remove · x delete file · ↑↓ select · q quit")
}

func (m model) formView() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("Add download") + "\n\n")
	b.WriteString(labelStyle.Render("URL") + "\n" + m.urlIn.View() + "\n\n")
	b.WriteString(labelStyle.Render("Checksum") + mutedStyle.Render(" (optional; verified automatically when the download finishes)") + "\n")
	b.WriteString(m.sumIn.View() + "\n")
	b.WriteString(checksumHint(m.sumIn.Value()))
	if m.formErr != "" {
		b.WriteString("\n" + badStyle.Render(m.formErr))
	}
	return formBoxStyle.Width(max(40, m.width-2)).Render(b.String())
}

// checksumHint says what will happen with what's typed in the Checksum field.
func checksumHint(v string) string {
	if strings.TrimSpace(v) == "" {
		return mutedStyle.Render("No checksum: the download won't be verified.")
	}
	cs, err := engine.ParseChecksum(v)
	if err != nil {
		return badStyle.Render(err.Error())
	}
	return okStyle.Render("✓ " + cs.Algo + " checksum: will be verified when the download finishes.")
}

func (m model) listView(avail int) string {
	if len(m.list) == 0 {
		if m.mode == modeAdd {
			return ""
		}
		return mutedStyle.Render("No downloads yet. Press a to add one.")
	}

	blocks := make([]string, len(m.list))
	for i, s := range m.list {
		blocks[i] = m.itemView(s, i == m.cursor)
	}

	// Scroll just far enough that the selected download is visible.
	offset := 0
	for offset < m.cursor {
		h := 0
		for _, blk := range blocks[offset : m.cursor+1] {
			h += lipgloss.Height(blk) + 1
		}
		if h <= avail {
			break
		}
		offset++
	}

	var out []string
	h := 0
	for _, blk := range blocks[offset:] {
		bh := lipgloss.Height(blk) + 1
		if h+bh > avail && len(out) > 0 {
			break
		}
		out = append(out, blk)
		h += bh
	}
	return strings.Join(out, "\n\n")
}

func (m model) itemView(s engine.Snapshot, selected bool) string {
	marker := "  "
	if selected {
		marker = cursorStyle.Render("▸ ")
	}
	const indent = "  "

	// Line 1: name and status.
	lines := []string{marker + nameStyle.Render(s.Filename) + "  " + statusLabel(s.Status)}

	// Line 2: progress bar and numbers (download, or hashing while verifying).
	if s.Status == engine.StatusVerifying {
		p := s.VerifyProgress()
		lines = append(lines, indent+m.checkBar.ViewAs(p)+waitStyle.Render(fmt.Sprintf("  Verifying %3.0f%%", p*100)))
	} else {
		p := s.Progress()
		var bar string
		if p < 0 {
			bar = m.dlBar.ViewAs(0)
		} else {
			bar = m.dlBar.ViewAs(p)
		}
		lines = append(lines, indent+bar+"  "+mutedStyle.Render(progressText(s)))
	}

	// Line 3: the checksum result, always shown so it's clear whether the file was checked.
	lines = append(lines, indent+checksumBadge(s.Checksum))
	if s.Checksum.Status == engine.ChecksumMismatch {
		lines = append(lines,
			indent+mutedStyle.Render("  expected ")+s.Checksum.Expected,
			indent+mutedStyle.Render("  actual   ")+s.Checksum.Actual,
			indent+mutedStyle.Render("  r re-download · x delete file"))
	} else if s.Error != "" {
		lines = append(lines, indent+badStyle.Render(truncate(s.Error, m.width-4)))
	}
	return strings.Join(lines, "\n")
}

func progressText(s engine.Snapshot) string {
	var parts []string
	if s.Size >= 0 {
		parts = append(parts, fmt.Sprintf("%s / %s", humanBytes(s.Downloaded), humanBytes(s.Size)))
		if s.Size > 0 {
			parts = append(parts, fmt.Sprintf("%.0f%%", s.Progress()*100))
		}
	} else {
		parts = append(parts, humanBytes(s.Downloaded))
	}
	if s.Status == engine.StatusDownloading {
		if s.Speed > 0 {
			parts = append(parts, humanBytes(int64(s.Speed))+"/s")
		}
		if eta := humanDuration(s.ETA); eta != "" {
			parts = append(parts, "ETA "+eta)
		}
	}
	return strings.Join(parts, " · ")
}

func statusLabel(st engine.Status) string {
	switch st {
	case engine.StatusDone:
		return okStyle.Render("done")
	case engine.StatusFailed:
		return badStyle.Render("failed")
	case engine.StatusVerifying:
		return waitStyle.Render("verifying")
	case engine.StatusDownloading:
		return cursorStyle.Render("downloading")
	default:
		return mutedStyle.Render(string(st))
	}
}

func checksumBadge(cs engine.Checksum) string {
	switch cs.Status {
	case engine.ChecksumVerified:
		return okStyle.Render("✓ Verified") + mutedStyle.Render(" ("+cs.Algo+")")
	case engine.ChecksumMismatch:
		return badStyle.Render("✗ Checksum mismatch") + mutedStyle.Render(" ("+cs.Algo+")")
	case engine.ChecksumVerifying:
		return waitStyle.Render("… Verifying " + cs.Algo)
	case engine.ChecksumPending:
		return waitStyle.Render("⧗ " + cs.Algo + " checksum: will verify when finished")
	default:
		return mutedStyle.Render("– No checksum")
	}
}

func truncate(s string, n int) string {
	r := []rune(s)
	if n < 2 || len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}
