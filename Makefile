

dirs = XiProxy http2xic DbMan

all:
	for x in $(dirs); do (cd $$x; make); done

clean:
	for x in $(dirs); do (cd $$x; make clean); done

