all: docs

DOCS   = README README.md
SCRIPT = jabberd2-export.pl

docs: $(DOCS)

README: ${SCRIPT}
	podselect $(<) > $@

README.md: $(SCRIPT)
	pod2markdown $(<) > $@

clean:
	rm -f $(DOCS)
