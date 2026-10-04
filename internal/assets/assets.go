package assets

import _ "embed"

//go:embed browsers.list
var Browsers string

//go:embed goaccess.conf
var Config string
