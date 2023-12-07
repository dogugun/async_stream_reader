import os
filename = ".env"
with open(".env", 'w') as file:
    file.write("STREAM_ENDPOINT=http://localhost:8080/\n")
    file.write("STREAM_USER=sytac\n")
    file.write("STREAM_PASSWORD=<password>\n")
