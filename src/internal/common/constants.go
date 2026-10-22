package common

import (
	"os"
	"path"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

// GetHomePath returns the directory holding the peer database: <datadir>/ocfcore
// when datadir is set (--datadir / OF_DATADIR), otherwise ~/.ocfcore.
func GetHomePath() string {
	var ocfcorePath string
	if dataDir := viper.GetString("datadir"); dataDir != "" {
		ocfcorePath = path.Join(dataDir, "ocfcore")
	} else {
		home, err := homedir.Dir()
		if err != nil {
			Logger.Error("Could not get home directory", "error", err)
			home = "."
		}
		ocfcorePath = path.Join(home, ".ocfcore")
	}
	if _, err := os.Stat(ocfcorePath); os.IsNotExist(err) {
		err := os.MkdirAll(ocfcorePath, 0755)
		if err != nil {
			Logger.Error("Could not create ocfcore directory", "error", err)
			return "."
		}
	}
	return ocfcorePath
}

func GetDBPath(nodeid string) string {
	return path.Join(GetHomePath(), "ocfcore."+nodeid+".db")
}
