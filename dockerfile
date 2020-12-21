# Use the official openjdk image as a parent image.
FROM openjdk:8

# Set the working directory.
WORKDIR /titanoboa/

COPY titanoboa-0.9.0_gui.zip /titanoboa/titanoboa.zip

# Unzip the release and delete the zip
RUN unzip titanoboa.zip && rm titanoboa.zip

# Titanoboa uses port 3000 by default
EXPOSE 3000

# Start Titanoboa server
CMD [ "./start.sh" ]
