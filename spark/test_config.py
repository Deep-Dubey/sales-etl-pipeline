from spark.common import load_config


def main():
    config = load_config()

    print("=" * 60)
    print("Configuration Loaded Successfully")
    print("=" * 60)

    for key, value in config.items():
        print(f"{key}:")
        print(value)
        print()


if __name__ == "__main__":
    main()