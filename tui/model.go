package tui

import (
	"database/sql"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const (
	InitialTextareaHeight = 3
	MaxTextareaHeight     = 8
	LayoutGap             = 1
	MinTableHeight        = 5
	DefaultTerminalWidth  = 80
)

type Model struct {
	db         *sql.DB
	format     string
	input      textarea.Model
	results    viewport.Model
	width      int
	height     int
	history    []string
	historyPos int
}

func Run(db *sql.DB, format string) error {
	m := initialModel(db, format)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func initialModel(db *sql.DB, format string) Model {
	ta := textarea.New()
	ta.Placeholder = "Enter SQL query... (press Enter to execute, Ctrl+C to quit)"
	ta.Focus()
	ta.ShowLineNumbers = true
	ta.SetWidth(DefaultTerminalWidth)
	ta.SetHeight(InitialTextareaHeight)
	ta.FocusedStyle = textarea.Style{Base: lipgloss.NewStyle()}
	ta.BlurredStyle = textarea.Style{Base: lipgloss.NewStyle()}

	history := loadHistory()

	welcomeRows := []table.Row{
		{"Welcome to pbql-go"},
		{""},
		{"Type a SQL query and press Enter"},
		{"Ctrl+J - Insert newline"},
		{"Ctrl+C or .quit - Exit"},
		{".help, .tables, .schema - More commands"},
	}
	welcomeCols := []table.Column{{Title: "Getting Started", Width: 40}}

	t := table.New(
		table.WithColumns(welcomeCols),
		table.WithRows(welcomeRows),
		table.WithFocused(false),
	)

	vp := viewport.New(DefaultTerminalWidth, 0)
	vp.SetContent(t.View())

	return Model{
		db:         db,
		format:     format,
		input:      ta,
		results:    vp,
		width:      80,
		height:     24,
		history:    history,
		historyPos: -1,
	}
}

func (m Model) Init() tea.Cmd {
	return textarea.Blink
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "up", "ctrl+p":
			if m.historyPos < len(m.history)-1 {
				m.historyPos++
				m.input.SetValue(m.history[m.historyPos])
			}
			return m, nil
		case "down", "ctrl+n":
			if m.historyPos > 0 {
				m.historyPos--
				m.input.SetValue(m.history[m.historyPos])
			} else if m.historyPos == 0 {
				m.historyPos = -1
				m.input.SetValue("")
			}
			return m, nil
		case "ctrl+j":
			// Insert newline
			m.input.InsertString("\n")
			m.input.CursorDown()
			m.recalculateLayout()
			return m, nil
		case "enter":
			query := strings.TrimSpace(m.input.Value())
			if query == "" {
				return m, nil
			}
			if strings.HasPrefix(query, ".") {
				return m.handleCommand(query)
			}
			results, cols := executeQuery(m.db, query)
			m.results.SetContent(buildTable(results, cols, m.width))
			appendToHistory(query)

			m.history = loadHistory()
			m.historyPos = -1
			m.input.Reset()
			return m, nil
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	}

	m.input, cmd = m.input.Update(msg)
	cmds = append(cmds, cmd)

	// Recalculate heights
	m.recalculateLayout()
	return m, tea.Batch(cmds...)
}

func (m *Model) recalculateLayout() {
	// Calculate textarea height based on content
	textLines := strings.Count(m.input.Value(), "\n") + 1
	textareaHeight := textLines + 2 // Add padding
	if textareaHeight > MaxTextareaHeight {
		textareaHeight = MaxTextareaHeight
	}
	if textareaHeight < InitialTextareaHeight {
		textareaHeight = InitialTextareaHeight
	}
	m.input.SetHeight(textareaHeight)
	m.input.SetWidth(m.width)

	// Calculate table height
	tableHeight := m.height - textareaHeight - LayoutGap
	if tableHeight < MinTableHeight {
		tableHeight = MinTableHeight
	}
	m.results.Height = tableHeight
	m.results.Width = m.width
}

func (m Model) View() string {
	m.recalculateLayout()
	return m.results.View() + strings.Repeat("\n", LayoutGap) + m.input.View()
}

func (m Model) handleCommand(cmd string) (Model, tea.Cmd) {
	switch strings.ToLower(cmd) {
	case ".quit", ".exit", ".q":
		return m, tea.Quit
	case ".help", ".h", ".?":
		rows := []table.Row{
			{".help, .h, .?", "Show this help"},
			{".tables", "List all tables"},
			{".schema", "Show detailed schema"},
			{".format <fmt>", "Set output format"},
			{".quit, .exit", "Exit interactive mode"},
			{"Enter", "Execute query"},
			{"Ctrl+C, q", "Quit"},
		}
		m.results.SetContent(buildTable(rows, []string{"Command", "Description"}, m.width))
	case ".tables":
		rows, _ := m.db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
		var tables []table.Row
		for rows.Next() {
			var name string
			rows.Scan(&name)
			tables = append(tables, table.Row{name})
		}
		m.results.SetContent(buildTable(tables, []string{"Tables"}, m.width))
	case ".schema":
		rows := []table.Row{
			{"files", "name, package, syntax, options"},
			{"messages", "full_name, name, file, parent_message..."},
			{"fields", "id, name, number, message, type, type_name..."},
			{"enums", "full_name, name, file, parent_message..."},
			{"enum_values", "id, name, number, enum, options"},
			{"services", "full_name, name, file, options"},
			{"methods", "full_name, name, service, input_type..."},
			{"extensions", "full_name, name, number, file..."},
			{"oneofs", "id, name, message, options"},
			{"oneof_fields", "oneof_id, field_id"},
			{"dependencies", "file, dependency, is_public..."},
		}
		m.results.SetContent(buildTable(rows, []string{"Table", "Columns"}, m.width))
	default:
		if strings.HasPrefix(cmd, ".format ") {
			newFmt := strings.TrimSpace(strings.TrimPrefix(cmd, ".format "))
			if newFmt == "table" || newFmt == "json" || newFmt == "csv" {
				m.format = newFmt
				rows := []table.Row{{fmt.Sprintf("Format set to %s", newFmt)}}
				m.results.SetContent(buildTable(rows, []string{"Status"}, m.width))
			} else {
				rows := []table.Row{{fmt.Sprintf("Invalid format: %s", newFmt)}}
				m.results.SetContent(buildTable(rows, []string{"Error"}, m.width))
			}
		} else {
			rows := []table.Row{{fmt.Sprintf("Unknown command: %s", cmd)}}
			m.results.SetContent(buildTable(rows, []string{"Error"}, m.width))
		}
	}
	m.recalculateLayout()
	return m, nil
}

func buildTable(rows []table.Row, headers []string, terminalWidth int) string {
	if len(rows) == 0 {
		return "No results"
	}

	colWidth := (terminalWidth - 4) / len(headers)
	if colWidth < 10 {
		colWidth = 10
	}

	cols := make([]table.Column, len(headers))
	for i, h := range headers {
		cols[i] = table.Column{Title: h, Width: colWidth}
	}

	t := table.New(
		table.WithColumns(cols),
		table.WithRows(rows),
		table.WithFocused(false),
		table.WithStyles(table.Styles{
			Selected: lipgloss.Style{},
			Header:   lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#00FF00")),
		}),
	)

	return t.View()
}

func executeQuery(db *sql.DB, query string) ([]table.Row, []string) {
	rows, err := db.Query(query)
	if err != nil {
		return []table.Row{{fmt.Sprintf("Error: %v", err)}}, []string{"Error"}
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return []table.Row{{fmt.Sprintf("Error: %v", err)}}, []string{"Error"}
	}

	var results []table.Row
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		row := make([]string, len(cols))
		for i, val := range values {
			row[i] = fmt.Sprintf("%v", val)
		}
		results = append(results, row)
	}
	return results, cols
}

func appendToHistory(query string) error {
	usr, err := user.Current()
	if err != nil {
		return err
	}
	path := filepath.Join(usr.HomeDir, ".pbql_history")

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	timestamp := time.Now().Format(time.RFC3339)
	_, err = f.WriteString(fmt.Sprintf("# %s\n%s\n\n", timestamp, query))
	return err
}

func loadHistory() []string {
	usr, err := user.Current()
	if err != nil {
		return nil
	}
	path := filepath.Join(usr.HomeDir, ".pbql_history")

	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	lines := strings.Split(string(data), "\n")
	var history []string
	var current []string
	for _, line := range lines {
		if strings.HasPrefix(line, "# ") {
			if len(current) > 0 {
				history = append([]string{strings.Join(current, "\n")}, history...)
				current = nil
			}
		} else if line == "" {
			if len(current) > 0 {
				history = append([]string{strings.Join(current, "\n")}, history...)
				current = nil
			}
		} else {
			current = append(current, line)
		}
	}
	if len(current) > 0 {
		history = append([]string{strings.Join(current, "\n")}, history...)
	}

	return history
}
