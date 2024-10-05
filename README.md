# NASA Space Photo ETL Project

This project fetches, processes, and stores daily space photos using NASA's Astronomy Picture of the Day (APOD) API.

## Features

- Fetches daily space photos and information from NASA's APOD API
- Processes and structures the retrieved data
- Stores processed data in a PostgreSQL database
- Includes error handling and logging

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/ramazanusen/nasa-space-photo-etl.git
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Create a `.env` file and set the necessary environment variables:
   ```
   NASA_API_KEY=your_api_key_here
   DB_HOST=your_database_host
   DB_NAME=your_database_name
   DB_USER=your_database_user
   DB_PASSWORD=your_database_password
   ```


## Contributing

1. Fork the repository
2. Create a new feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the [MIT License](LICENSE).

## Contact

Project Owner: [Ramazan Usen](https://github.com/ramazanusen)

Project Link: [https://github.com/ramazanusen/nasa-space-photo-etl](https://github.com/ramazanusen/nasa-space-photo-etl)
