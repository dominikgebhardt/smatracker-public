import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Test logging messages

logging.info("This is an info message")
logging.warning("This is a warning message")
logging.error("This is an error message")

logging.info("Trying to import mylib")
try:
    from mylib import hello

    logging.info(hello("World"))
    logging.info("Successfully imported mylib")
except ImportError:
    logging.error("Failed to import mylib")
