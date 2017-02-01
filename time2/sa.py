import arrow
import pprint
import json

start    = int(0)
#stop     = int(100000)
stop     = int(1474925221)

running = start

file_ = open("file", 'wb')

while running < stop:

    list = []
    running = running + 1
    arrow_date = arrow.get(running)
    list.append(arrow_date.format('YYYY'))
    list.append(arrow_date.format('MM'))
    list.append(arrow_date.format('DD'))
    list.append(arrow_date.format('HH'))
    # create a new file handle when hour is zero 
    if list[3] == "00":
	filename = list[0] + "-" + list[1] + "-" + list[2]
        file_ = open(filename, 'wb')
    list.append(arrow_date.format('mm'))
    list.append(arrow_date.format('ss'))
    list.append(arrow_date.format('ZZ'))
    # create a new file handle when hour is zero 
    # pprint.pprint(json.dumps(list))
    # pprint.pprint(file)
    file_.write("%s\n" % json.dumps(list))

exit()
