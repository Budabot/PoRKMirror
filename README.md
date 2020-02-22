# PoRKMirror
Scrapes People of Rubi-Ka (people.anarchy-online.com) to record character history

# To build
From the PoRKMirror directory, run this command:
```
docker run -it --rm --name pork-mirror-build -v ~/.m2:/root/.m2 -v "$(pwd)":/usr/src/mymaven -w /usr/src/mymaven maven:3.6.3-jdk-11-slim mvn -e clean package
```

If you get an error about an unresolved scala-util dependency, you should follow the instructions on the https://github.com/bigwheels16/scala-util project for installing it locally first.
