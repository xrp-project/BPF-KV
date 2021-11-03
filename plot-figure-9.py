import sys
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator

thread = [i for i in range(1, 13)]

def get_data(file_name, prefix, index):
    data = []
    with open(file_name, 'r') as f:
        line = f.readline()
        while line:
            if line.startswith(prefix):
                data.append(float(line.split()[index]))
            line = f.readline()
    return data

c = {'spdk':'blue', 'xrp':'red', 'io_uring':'orange', 'read':'green'}
m = {'spdk':'.', 'xrp':'v', 'io_uring':'+', 'read':'d'}

# figure a
plt.rcParams.update({'font.size': 14})
plt.figure()
for i in ['spdk', 'io_uring', 'read', 'xrp']:
    lat_3 = get_data('./data/' + i + '-3.txt', '99%   latency:', 2)
    lat_6 = get_data('./data/' + i + '-6.txt', '99%   latency:', 2)
    plt.plot(thread, lat_3, label=i, color=c[i], marker=m[i], markersize=14)
    #plt.plot(thread, lat_6, label=i+' (6 lvls)')
x_loc = MultipleLocator(1)
ax = plt.gca()
ax.xaxis.set_major_locator(x_loc)
plt.xlim(0.5, 12.5)
plt.yscale('log')
plt.xlabel('Threads', fontsize=20)
plt.ylabel('99th Latency (μs)', fontsize=20)
plt.legend()
plt.tight_layout()
plt.savefig('figure-9-a.pdf')
plt.close()

# figure b
plt.figure()
for i in ['spdk', 'xrp', 'read', 'io_uring']:
    thru_3 = get_data('./data/' + i + '-3.txt', 'Average throughput:', 2)
    print(i, thru_3)
    plt.plot(thread, thru_3, label=i, color=c[i], marker=m[i], markersize=14)
x_loc = MultipleLocator(1)
ax = plt.gca()
ax.xaxis.set_major_locator(x_loc)
# plt.xlim(0.5, 12.5)
# plt.yscale('log')
plt.ylim(ymin=0)
plt.xlabel('Threads', fontsize=20)
plt.ylabel('Throughput (ops/sec)', fontsize=20)
plt.legend()
plt.tight_layout()
plt.savefig('figure-9-b.pdf')
plt.close()

# figure c
plt.figure()
for i in ['spdk', 'xrp', 'read', 'io_uring']:
    thru_6 = get_data('./data/' + i + '-6.txt', 'Average throughput:', 2)
    plt.plot(thread, thru_6, label=i, color=c[i], marker=m[i], markersize=14)
x_loc = MultipleLocator(1)
ax = plt.gca()
ax.xaxis.set_major_locator(x_loc)
# plt.xlim(0.5, 12.5)
# plt.yscale('log')
plt.ylim(ymin=0) 
plt.xlabel('Threads', fontsize=20)
plt.ylabel('Throughput (ops/sec)', fontsize=20)
plt.legend()
plt.tight_layout()
plt.savefig('figure-9-c.pdf')
plt.close()

# figure d
plt.figure()
layer = [i for i in range(1, 7)]
for i in ['spdk', 'xrp', 'read', 'io_uring']:
    thru = get_data('./data/' + i + '-chain.txt', 'Average throughput:', 2)
    plt.plot(layer, thru, label=i, color=c[i], marker=m[i], markersize=14)
x_loc = MultipleLocator(1)
ax = plt.gca()
ax.xaxis.set_major_locator(x_loc)
# plt.xlim(0.5, 12.5)
# plt.yscale('log')
plt.ylim(ymin=0) 
plt.xlabel('I/O Chain Length', fontsize=20)
plt.ylabel('Throughput (ops/sec)', fontsize=20)
plt.legend()
plt.tight_layout()
plt.savefig('figure-9-d.pdf')
plt.close()
