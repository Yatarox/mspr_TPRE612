# FastAPI Dashboard API

This project is a FastAPI application that serves as a dashboard API. It provides endpoints for retrieving and updating dashboard metrics using an existing database and ETL processes.

## Project Structure

```
fastapi-dashboard-api
├── src
│   ├── main.py                # Entry point of the FastAPI application
│   ├── api                    # Contains API routes
│   │   ├── __init__.py
│   │   └── routes
│   │       ├── __init__.py
│   │       └── dashboard.py   # Dashboard API routes
│   ├── models                 # Contains database models
│   │   ├── __init__.py
│   │   └── database.py        # Database connection and ORM models
│   ├── schemas                # Contains Pydantic schemas
│   │   ├── __init__.py
│   │   └── dashboard.py       # Request and response models for the dashboard API
│   └── services               # Contains business logic
│       ├── __init__.py
│       └── dashboard_service.py # Logic for interacting with dashboard data
├── requirements.txt           # Project dependencies
└── README.md                  # Project documentation
```

## Setup Instructions

1. Clone the repository:
   ```
   git clone <repository-url>
   cd fastapi-dashboard-api
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Run the FastAPI application:
   ```
   uvicorn src.main:app --reload
   ```

4. Access the API documentation at `http://127.0.0.1:8000/docs`.

## Usage Examples

- **Get Dashboard Metrics**
  - Endpoint: `GET /dashboard/metrics`
  - Description: Retrieves the current metrics for the dashboard.

- **Update Dashboard Metrics**
  - Endpoint: `PUT /dashboard/metrics`
  - Description: Updates the metrics for the dashboard with the provided data.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License

This project is licensed under the MIT License.