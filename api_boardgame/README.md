# Flask API w/ Spark

```http
POST http://localhost:5000/submit HTTP/1.1
Content-Type: application/json

{
    "app": "mysql_bowls_app.py"
}
```

```http
POST http://localhost:5000/submit HTTP/1.1
Content-Type: application/json

{
    "app": "first_run.py",
    "master": "spark://leader:7077"
}
```

```http
POST http://localhost:5000/submit HTTP/1.1
Content-Type: application/json

{
    "app": "color_app.py",
    "master": "spark://leader:7077"
}
```