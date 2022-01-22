import re
import os


def replaceOld(s1, excpt):
    if excpt:
        rgx = re.search(r'\=(\s*)', s1)[0]
        before = s1[:s1.find(rgx)+len(rgx)]+'int(int('
        s1 = before+s1[len(before)-8:]+'*10.764))'
        return s1
    else:
        rgx = re.search(r'\,(\s*)', s1)[0]
        before = s1[:s1.find(rgx)+len(rgx)]+'int(int('
        s1 = before+s1[len(before)-8:s1.rfind(')')+1]+'*10.764))'
        return s1


currentSpider = ""
codeLins = []


def getlineNumber(pattern, string, rex):

    matches = list(re.finditer(pattern, string, 0))
    if not matches:
        return

    end = matches[-1].start()
    newline_table = {-1: 0}
    for i, m in enumerate(re.finditer(r'\n', string), 1):
        offset = m.start()
        if offset > end:
            break
        newline_table[offset] = i

    for m in matches:
        newline_offset = string.rfind('\n', 0, m.start())
        line_number = newline_table[newline_offset]

        oldLine = codeLins[line_number]
        print(oldLine)
        if 'item' in codeLins[line_number] and ':' not in codeLins[line_number]:
            try:
                newline = replaceOld(codeLins[line_number], False)

                print(newline)
            except:
                try:
                    newline = replaceOld(codeLins[line_number], True)
                    print(newline)
                except:
                    with open('notEditedSpiders.txt', 'a') as file:
                        file.write(codeLins[line_number] +
                                   " - " + currentSpider + '\n')

            Nstring = string.replace(oldLine, newline)
            with open(currentSpider, 'w') as file:
                file.write(Nstring)


with open('spidersPaths', 'r') as pyNames:
    filedata = pyNames.readlines()


for file in filedata:

    print(currentSpider)
    print('*'*50)
    x = str("".join(os.path.dirname(os.getcwd())))
    currentSpider = x+'/'+file.replace('\n', '')
    with open(currentSpider) as spiderCode:
        pattern = r'\"square_meters\"|\'square_meters\''
        string = spiderCode.read()
        codeLins = string.split('\n')
        rex = re.findall(pattern, string)
        getlineNumber(pattern, string, rex)

    print('='*100)
