package main

type Config struct {
	MongoDBConnectionURI string `env:"MONGO_URI" envDefault:"mongodb://127.0.0.1:27018/?connect=direct"`
	OutputDirectory string `env:"OUTPUT_DIR" envDefault:"./dump"`
	MaxCollections int `env:"MAX_COLLECTIONS" envDefault:"100"`
	MaxEvents int64 `env:"MAX_EVENTS" envDefault:"1000"`
	MaxRepetitions int64 `env:"MAX_REPETITIONS" envDefault:"1000"`
}
