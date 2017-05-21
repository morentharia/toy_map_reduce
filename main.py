import os
import tempfile
import shutil
from collections import defaultdict
from multiprocessing import Pool, cpu_count

import click
from more_itertools import chunked, flatten, divide

from readerwriter import ReaderWriter

ID_LEVEL_FILENAME = "id_level.csv"
ID_OBJECT_FILENAME = "id_object.csv"


@click.command()
@click.argument('action', type=click.Choice(['generate', 'process']))
@click.option('--files-path', default="/tmp/data", help='default files path')
@click.option('--num-zip', default=50, help='The number of zip files')
@click.option('--num-xml', default=10, help='The number of xml files')
def cli(action, *args, **kwargs):
    try:
        globals()[action](*args, **kwargs)
    except Exception as e:
        click.secho(str(e), fg='red')


def generate(files_path, num_zip, num_xml):
    os.makedirs(files_path, exist_ok=True)
    for doc in ReaderWriter(files_path, num_zip, num_xml):
        doc.write()


def process(files_path, num_zip, num_xml):
    pool_size = cpu_count() * 2
    doc_per_process = 4
    reduce_tree_depth = 4

    with Pool(pool_size) as pool:
        gen = ReaderWriter(files_path, num_zip, num_xml)

        gen = chunked(gen, doc_per_process)
        gen = pool.imap_unordered(mapper_process, gen)

        gen = divide(1 << reduce_tree_depth, gen)
        gen = flatten(map(partition, gen))
        gen = pool.imap_unordered(reducer_process, gen)

        for _ in range(reduce_tree_depth - 1):
            gen = chunked(gen, 2)
            gen = flatten(map(partition, gen))
            gen = pool.imap_unordered(reducer_process, gen)

        gen = partition(gen)
        gen = pool.imap_unordered(reducer_process, gen)

        # DEBUG:
        # result = list(gen); print(result); return;

        gen = list(gen)
        if not gen:
            click.secho('Empty output', fg='red')

        for res in gen:
            for k, v in res.items():
                click.secho('Ok %s' % k, fg='green')
                shutil.move(v, k)


def partition(mapped_values):
    partitioned_data = defaultdict(list)
    for val in mapped_values:
        for key, value in val.items():
            partitioned_data[key].append(value)
    return partitioned_data.items()


def write_tmpfile(lines):
    with tempfile.NamedTemporaryFile('w', delete=False) as out:
        out.writelines(lines)
    return out.name


def mapper_process(doc_list):
    try:
        return {k: write_tmpfile(flatten(v))
                for k, v in partition(map(mapper, doc_list))}
    except Exception as e:
        click.secho(str(e), fg='red')
        return {}


def mapper(doc):
    one_lines = []
    two_lines = []
    for data in doc.read():
        one_lines.append(
            "{id},{level}\n".format(**data)
        )
        two_lines.extend([
            "{id},{name}\n".format(name=name, **data)
            for name in data['objects']
        ])
    return {ID_LEVEL_FILENAME: one_lines, ID_OBJECT_FILENAME: two_lines}


def reducer_process(args):
    try:
        key, values = args
        with tempfile.NamedTemporaryFile('wb', delete=False) as wfd:
            for filename in values:
                with open(filename, 'rb') as fd:
                    shutil.copyfileobj(fd, wfd)
                os.remove(filename)
        return {key: wfd.name}
    except Exception as e:
        click.secho(str(e), fg='red')
        return {}


if __name__ == '__main__':
    cli()
