from nio import *



nio = NetWorkIO('localhost', 1984)

req = OODict()
req.method = 'meta.ls'
req.args = {'file': '/aabdf'}
if nio.request(req):
    print req.return_value
else:
    print req.error

