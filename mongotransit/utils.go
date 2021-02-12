package mongotransit

type Phase string

const (
	PHASE_EXPORT = Phase("export")
	PHASE_IMPORT = Phase("import")
)
