import sys
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator

plt.rcParams.update({'font.size': 12})
plt.figure()
thread = [i for i in range(6, 25)]
spdk = [697046.9758,665379.6229,625176.7638,604199.9895,551595.1093,506739.3245,461518.7572,441649.0087,405917.9065,359946.3352,387235.9762,315002.0367,312038.8661,285033.2001,263546.7527,258897.8283,223314.2115,200517.0509,190620.779]
ebpf = [708964.9917,706577.358,703521.6036,700908.1714,698775.5837,698436.3271,699695.8742,695898.0707,698472.3006,696774.4961,696896.8216,697747.3257,699138.5369,696613.8805,697182.465,700964.0176,702165.3548,698442.4334,700138.5618]
plt.plot(thread, spdk, label='SPDK', marker='.', markersize=10)
plt.plot(thread, ebpf, label='XRP', marker='^', markersize=10)
plt.axhline(y=714285.714, xmin=0, xmax=25, linestyle='--', color='r')
plt.gca().xaxis.set_major_locator(MultipleLocator(1))
plt.xlim(5.1, 24.9)
plt.ylim(0, 750000)
plt.xlabel('Thread Number', fontsize=20)
plt.ylabel('Throughput (op/s)', fontsize=20)
plt.legend(prop={'size': 14})
plt.tight_layout()
plt.savefig('spdk-scalability.pdf')
plt.close()

thru_spdk = []
lat_spdk = []
lat95_spdk = []
lat99_spdk = []
lat999_spdk = []
with open('./data/spdk-new-lat-thru.out', 'r') as f:
    line = f.readline()
    while line:
        if line.startswith('Average throughput:'):
            nums = line.split()
            thru_spdk.append(float(nums[2]))
            lat_spdk.append(float(nums[5]))
        elif line.startswith('95%   latency:'):
            lat95_spdk.append(float(line.split()[2]))
        elif line.startswith('99%   latency:'):
            lat99_spdk.append(float(line.split()[2]))
        elif line.startswith('99.9% latency:'):
            lat999_spdk.append(float(line.split()[2]))
        line = f.readline()

thru_ebpf = []
lat_ebpf = []
lat95_ebpf = []
lat99_ebpf = []
lat999_ebpf = []
with open('./data/ebpf-lat-thru.out', 'r') as f:
    line = f.readline()
    while line:
        if line.startswith('Average throughput:'):
            nums = line.split()
            thru_ebpf.append(float(nums[2]))
            lat_ebpf.append(float(nums[5]))
        elif line.startswith('95%   latency:'):
            lat95_ebpf.append(float(line.split()[2]))
        elif line.startswith('99%   latency:'):
            lat99_ebpf.append(float(line.split()[2]))
        elif line.startswith('99.9% latency:'):
            lat999_ebpf.append(float(line.split()[2]))
        line = f.readline()

plt.figure()
plt.plot(thru_spdk, lat99_spdk, label='99% latency (SPDK)', linestyle='dashed', color='red', marker='s', markersize=10)
plt.plot(thru_spdk, lat_spdk, label='avg latency (SPDK)', linestyle='solid', color='red', marker='.', markersize=10)
plt.plot(thru_ebpf, lat99_ebpf, label='99% latency (XRP)', linestyle='dashed', color='blue', marker='d', markersize=10)
plt.plot(thru_ebpf, lat_ebpf, label='avg latency (XRP)', linestyle='solid', color='blue', marker='^', markersize=10)
# plt.plot(thru, lat95, label='95% latency', marker='v')
# plt.plot(thru, lat999, label='99.9% latency', marker='d')

plt.xlabel('Throughput (op/s)', fontsize=20)
plt.ylabel('Latency (usec)', fontsize=20)
plt.legend(prop={'size': 14})
plt.tight_layout()
plt.savefig('spdk-lat-thru.pdf')
plt.close()
