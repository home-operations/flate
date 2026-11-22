package resourcesetinput

import (
	"bytes"
	"text/template/parse"
)

const (
	draftValue    = "<<value>>"
	draftDocument = "\n<<template>>\n"
)

// Draft preserves literal YAML outside template control flow and marks unknown output.
func (pt *Template) Draft() []byte {
	var out bytes.Buffer
	writeDraft(&out, pt.tmpl.Tree.Root)
	return out.Bytes()
}

func writeDraft(out *bytes.Buffer, list *parse.ListNode) {
	if list == nil {
		return
	}
	for _, node := range list.Nodes {
		switch node := node.(type) {
		case *parse.TextNode:
			out.Write(node.Text)
		case *parse.ActionNode:
			if len(node.Pipe.Decl) == 0 {
				out.WriteString(draftValue)
			}
		case *parse.IfNode:
			writeDraftBranch(out, &node.BranchNode)
		case *parse.RangeNode:
			writeDraftBranch(out, &node.BranchNode)
		case *parse.WithNode:
			writeDraftBranch(out, &node.BranchNode)
		case *parse.TemplateNode:
			out.WriteString(draftDocument)
		}
	}
}

func writeDraftBranch(out *bytes.Buffer, branch *parse.BranchNode) {
	var body bytes.Buffer
	for _, list := range []*parse.ListNode{branch.List, branch.ElseList} {
		body.Reset()
		writeDraft(&body, list)
		data := body.Bytes()
		value := bytes.TrimSpace(data)
		if len(value) == 0 {
			continue
		}
		// A branch can emit another document even when it starts in a payload field.
		if bytes.HasPrefix(value, []byte("---")) || bytes.Contains(value, []byte("\n---")) ||
			bytes.Contains(value, []byte("<<template>>")) {
			out.WriteString(draftDocument)
			return
		}
		offset := bytes.Index(data, value)
		out.Write(data[:offset])
		out.WriteString(draftValue)
		out.Write(data[offset+len(value):])
	}
}
