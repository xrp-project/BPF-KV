# disable hyper threading
echo off > /sys/devices/system/cpu/smt/control
# set to performance mode
for CPUID in {0..5}
do
    cd /sys/devices/system/cpu/cpu$CPUID/
    cd cpufreq/
    echo "performance" > scaling_governor
    echo $(cat cpuinfo_max_freq) > scaling_max_freq
    echo $(cat cpuinfo_max_freq) > scaling_min_freq
    cd ../cpuidle
    cd state0
    echo 0 > disable
    cd ..
    for STATE in {1..3}
    do
        cd state$STATE
        echo 1 > disable
        cd ..
    done
done
cd /sys/devices/system/cpu/intel_pstate
echo 1 > no_turbo
echo 100 > max_perf_pct
echo 100 > min_perf_pct
