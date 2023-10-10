package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/dominant-strategies/go-quai/cmd/utils"
	"github.com/dominant-strategies/go-quai/common/constants"
	"github.com/dominant-strategies/go-quai/log"
)

var rootCmd = &cobra.Command{
	PersistentPreRunE: rootCmdPreRun,
}

func Execute() error {
	err := rootCmd.Execute()
	if err != nil {
		return err
	}
	return nil
}

func init() {
	for _, flag := range utils.GlobalFlags {
		utils.CreateAndBindFlag(flag, rootCmd)
	}
}

func rootCmdPreRun(cmd *cobra.Command, args []string) error {
	// set config path to read config file
	configDir := cmd.Flag(utils.ConfigDirFlag.Name).Value.String()
	viper.SetConfigFile(configDir + constants.CONFIG_FILE_NAME)
	viper.SetConfigType(constants.CONFIG_FILE_TYPE)

	// Write default config file if it does not exist
	if _, err := os.Stat(configDir + constants.CONFIG_FILE_NAME); os.IsNotExist(err) {
		err := utils.WriteDefaultConfigFile(configDir+constants.CONFIG_FILE_NAME, constants.CONFIG_FILE_TYPE)
		if err != nil {
			return err
		}
		log.WithField("path", configDir+constants.CONFIG_FILE_NAME).Info("Default config file created")
	}

	// load config from file and environment variables
	utils.InitConfig()

	// set logger inmediately after parsing cobra flags
	logLevel := viper.GetString(utils.LogLevelFlag.Name)
	log.WithField("logLevel", logLevel).Info("setting global logger")
	log.SetGlobalLogger("", logLevel)

	// bind cobra flags to viper instance
	err := viper.BindPFlags(cmd.Flags())
	if err != nil {
		return fmt.Errorf("error binding flags: %s", err)
	}

	// Make sure data dir and config dir exist
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		if err := os.MkdirAll(configDir, 0755); err != nil {
			return err
		}
	}

	// Check that environment is local, colosseum, garden, lighthouse, dev, or orchard
	environment := viper.GetString(utils.EnvironmentFlag.Name)
	if !utils.IsValidEnvironment(environment) {
		log.Fatalf("invalid environment: %s", environment)
	}

	log.WithField("options", viper.AllSettings()).Debug("config options loaded")
	return nil
}