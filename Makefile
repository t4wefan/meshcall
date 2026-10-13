.PHONY: check router regenerate drift
check:       ## verify everything (what CI runs)
	scripts/check.sh
router:      ## build the Go Router into meshcall-router/bin
	scripts/check.sh go
regenerate:  ## rewrite all checked-in generated clients
	scripts/regenerate.sh
drift:       ## fail if any generated client is stale
	scripts/check.sh drift
