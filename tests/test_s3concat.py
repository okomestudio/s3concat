import hashlib
import json

import faker


fake = faker.Factory.create()


def generate_file(size):
    content = ''
    while 1:
        s = json.dumps({'first_name': fake.name(),
                        'address': fake.address()})
        content += s + '\n'
        if len(content) > size:
            break
    return content


def test_generate_file():
    content = generate_file(1024**2)
    assert 1024**2 < len(content)

    m = hashlib.md5()
    m.update(content)
    md5 = m.hexdigest()

    print content
    print md5
