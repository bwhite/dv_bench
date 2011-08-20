import time
import numpy as np


def generate_data(size):
    rnd = np.array(np.random.randint(0, 256, size), dtype=np.uint8).tostring()
    return rnd


def gauntlet(multi_write_func, sizes, num_rounds):
    times = {}
    for size in sizes:
        datas = [generate_data(size) for x in range(num_rounds)]
        times[size] = [0, 0, 0]  # Write, read, delete
        for cur_round in range(num_rounds):
            # Write
            st = time.time()
            multi_read_func, multi_delete_func = multi_write_func(datas)
            times[size][0] = (time.time() - st) / num_rounds
            # Read
            st = time.time()
            new_datas = multi_read_func()
            times[size][1] = (time.time() - st) / num_rounds
            assert new_datas == datas
            # Delete
            st = time.time()
            multi_delete_func()
            times[size][2] = (time.time() - st) / num_rounds
    return times


def local_multi_write_func(datas):
    import tempfile
    temp_dir = tempfile.mkdtemp()
    for num, data in enumerate(datas):
        with open('%s/%.8d' % (temp_dir, num), 'w') as fp:
            fp.write(data)
    datas_len = len(datas)
    
    def multi_read_func():
        return [open('%s/%.8d' % (temp_dir, num)).read() for num in range(datas_len)]

    def multi_delete_func():
        import shutil
        shutil.rmtree(temp_dir)

    return multi_read_func, multi_delete_func


def cass_multi_write_func(datas):
    import cass_bench
    col_fam = cass_bench.connect()
    k = 'bench_db-%f' % np.random.random()
    for x, y in enumerate(datas):
        col_fam.insert(k, {str(x): y})
    datas_len = len(datas)

    def multi_read_func():
        return [col_fam.get(k, [str(x)])[str(x)] for x in range(datas_len)]

    def multi_delete_func():
        col_fam.remove(k)

    return multi_read_func, multi_delete_func


def format_table(table_name, times):
    times_sorted = sorted(times)
    
    def mk_size(x):
        s = '%.0e' % x
        return ['$10^%s$' % int(s[3:])]
    table_body = '\n'.join([r'%s\\ \hline' % ('&'.join(mk_size(t) + ['%.5f' % x for x in times[t]])) for t in times_sorted])
    
    table = r"""\documentclass[11pt]{article}
    \usepackage{fullpage}

    \title{%s}

    \begin{document}
    \maketitle
    \begin{tabular}{|l||c|c|c|} \hline
    Size & Write & Read & Delete \\ \hline\hline
    %s
    \end{tabular}
    \end{document}""" % (table_name, table_body)
    return table


latex_out = format_table('Cassandra', gauntlet(cass_multi_write_func, [10 ** x for x in range(0, 7)], 10))
print(latex_out)
with open('cass.tex', 'w') as fp:
    fp.write(latex_out)

latex_out = format_table('Local Disk', gauntlet(local_multi_write_func, [10 ** x for x in range(0, 7)], 10))
print(latex_out)
with open('local.tex', 'w') as fp:
    fp.write(latex_out)
