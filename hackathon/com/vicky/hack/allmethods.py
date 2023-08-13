import re
def converttoupper(str):
    if str == None:
        return None
    else:
        return str.upper()

def remspecialchar(str):
    if str == None:
        return None
    else:
        return re.sub("[;\\@%-/:~,*?\"<>|&'0-9]",'',str)


x="Pathway - 2X (with dental)"
print(remspecialchar(x))

