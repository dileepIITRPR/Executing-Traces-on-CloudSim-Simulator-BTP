package org.cloudbus.cloudsim.examples;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;


import java.text.DecimalFormat;
import java.util.*;

public class DynamicResourceAllocation {
    private static List<Cloudlet> cloudletList;
    private static List<Vm> vmList;

    // Parameters for VMs, Hosts, and Cloudlets
    private static final int HOST_MIPS = 1000;
    private static final int HOST_RAM = 2048;
    private static final long HOST_STORAGE = 1000000;
    private static final int HOST_BW = 10000;
    private static final int VM_PES_NUMBER = 1;
    private static final int VM_RAM = 2048;
    private static final long VM_SIZE = 10000;
    private static final long VM_BW = 1000;
    private static final String VM_VMM = "Xen";
    private static final long CLOUDLET_LENGTH = 40000;
    private static final long CLOUDLET_FILE_SIZE = 300;
    private static final long CLOUDLET_OUTPUT_SIZE = 300;
    private static final double CPU_THRESHOLD = 0.8; // CPU utilization threshold for load balancing
    private static final double CPU_HISTORY_WINDOW = 10; // Window size to track CPU usage history (in seconds)
    private static Map<Vm, List<Double>> cpuUsageHistory = new HashMap<>();
    public static void main(String[] args) {
        Log.printLine("Starting Dynamic Resource Allocation Simulation");

        try {
            int numUser = 1;
            Calendar calendar = Calendar.getInstance();
            boolean traceFlag = false;
            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter NO. of Vm's: ");
            int NUM_VMS = scanner.nextInt();
            System.out.print("Enter NO. of Cloudlets: ");
            int NUM_CLOUDLETS= scanner.nextInt();
            
            CloudSim.init(numUser, calendar, traceFlag);

            Datacenter datacenter = createDatacenter("Datacenter_0");

            DatacenterBroker broker = createBroker();
            int brokerId = broker.getId();

            vmList = new ArrayList<>();

            // Create VMs
            for (int i = 0; i < NUM_VMS; i++) {
                Vm vm = new Vm(i, brokerId, HOST_MIPS, VM_PES_NUMBER, VM_RAM, VM_BW, VM_SIZE, VM_VMM, new CloudletSchedulerTimeShared());
                vmList.add(vm);
            }

            broker.submitVmList(vmList);

            cloudletList = new ArrayList<>();

            // Create Cloudlets
            for (int i = 0; i < NUM_CLOUDLETS; i++) {
                Cloudlet cloudlet = new Cloudlet(i, CLOUDLET_LENGTH, VM_PES_NUMBER, CLOUDLET_FILE_SIZE, CLOUDLET_OUTPUT_SIZE,
                        new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull());
                cloudlet.setUserId(brokerId);
                cloudletList.add(cloudlet);
            }

            broker.submitCloudletList(cloudletList);
            throttledVmAllocation(broker);

            bindCloudletsToVmsInDynamicRR(broker);

            CloudSim.startSimulation();

            List<Cloudlet> newList = broker.getCloudletReceivedList();

            CloudSim.stopSimulation();

            // Calculate data forwarded, execution time, and communication latency
            calculateMetrics(newList);
            Log.printLine("Dynamic Resource Allocation Simulation finished");
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }

    private static Datacenter createDatacenter(String name) {
        List<Host> hostList = new ArrayList<>();
        List<Pe> peList = new ArrayList<>();
        peList.add(new Pe(0, new PeProvisionerSimple(HOST_MIPS)));
        hostList.add(new Host(0, new RamProvisionerSimple(HOST_RAM), new BwProvisionerSimple(HOST_BW),
                HOST_STORAGE, peList, new VmSchedulerTimeShared(peList)));
        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double timeZone = 10.0;
        double cost = 3.0;
        double costPerMem = 0.05;
        double costPerStorage = 0.001;
        double costPerBw = 0.0;
        LinkedList<Storage> storageList = new LinkedList<>();
        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, timeZone, cost, costPerMem, costPerStorage, costPerBw);
        Datacenter datacenter = null;
        try {
            datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }

    private static DatacenterBroker createBroker() {
        DatacenterBroker broker = null;
        try {
            broker = new DatacenterBroker("Broker");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return broker;
    }
    private static void throttledVmAllocation(DatacenterBroker broker) {
        vmList.sort(Comparator.comparingDouble(vm -> estimateCpuUtilization(vm)));

        for (Vm vm : vmList) {
            double estimatedCpuUtilization = estimateCpuUtilization(vm);
            if (estimatedCpuUtilization < CPU_THRESHOLD) {
                List<Cloudlet> cloudletsToAllocate = findCloudletsToAllocate(vm);
                for (Cloudlet cloudlet : cloudletsToAllocate) {
                    broker.bindCloudletToVm(cloudlet.getCloudletId(), vm.getId());
                    updateCpuUsageHistory(vm, 1); // Set CPU usage of VM to 100%
                }
            } else {
                break;
            }
        }
    }
    private static void updateCpuUsageHistory(Vm vm, double cpuUsage) {
        if (!cpuUsageHistory.containsKey(vm)) {
            cpuUsageHistory.put(vm, new ArrayList<>());
        }
        List<Double> history = cpuUsageHistory.get(vm);
        history.add(cpuUsage);
        // Remove old entries to maintain the window size
        if (history.size() > CPU_HISTORY_WINDOW) {
            history.remove(0);
        }
    }

    // Method to estimate current CPU utilization based on historical CPU usage
    private static double estimateCpuUtilization(Vm vm) {
        if (!cpuUsageHistory.containsKey(vm) || cpuUsageHistory.get(vm).isEmpty()) {
            return 0.0; // Return 0 if no history available
        }
        List<Double> history = cpuUsageHistory.get(vm);
        double sum = 0.0;
        for (double usage : history) {
            sum += usage;
        }
        return sum / history.size(); // Return average CPU usage
    }

    private static List<Cloudlet> findCloudletsToAllocate(Vm underutilizedVm) {
        List<Cloudlet> cloudletsToAllocate = new ArrayList<>();
        for (Cloudlet cloudlet : cloudletList) {
            if (cloudlet.getVmId() == -1) { // Cloudlet not yet assigned to any VM
                cloudletsToAllocate.add(cloudlet);
            }
        }
        return cloudletsToAllocate;
    }

    private static void bindCloudletsToVmsInSJF(DatacenterBroker broker) {
        // Sort the cloudlet list based on their length (execution time)
        cloudletList.sort(Comparator.comparingLong(Cloudlet::getCloudletLength));

        // Assign each cloudlet to the VM with the shortest completion time
        for (Cloudlet cloudlet : cloudletList) {
            Vm minExecutionTimeVm = findVmWithMinExecutionTime(cloudlet, broker);
            broker.bindCloudletToVm(cloudlet.getCloudletId(), minExecutionTimeVm.getId());
        }
    }

    private static Vm findVmWithMinExecutionTime(Cloudlet cloudlet, DatacenterBroker broker) {
        double minExecutionTime = Double.MAX_VALUE;
        Vm minExecutionTimeVm = null;

        for (Vm vm : vmList) {
            double executionTime = calculateEstimatedExecutionTime(cloudlet, vm);
            if (executionTime < minExecutionTime) {
                minExecutionTime = executionTime;
                minExecutionTimeVm = vm;
            }
        }

        return minExecutionTimeVm;
    }

    private static double calculateEstimatedExecutionTime(Cloudlet cloudlet, Vm vm) {
        // Estimate execution time based on Cloudlet length and VM MIPS
        return cloudlet.getCloudletLength() / (double) vm.getMips();
    }
    private static void bindCloudletsToVmsInDynamicRR(DatacenterBroker broker) {
        int currentIndex = 0; // Current index to keep track of VM selection

        // Track data transfer status
        double totalDataTransferred = 0;
        double maxDataTransferThreshold = 1000; // Adjust this threshold as needed

        for (Cloudlet cloudlet : cloudletList) {
            // Check data transfer status before scheduling the cloudlet
            if (totalDataTransferred < maxDataTransferThreshold) {
                Vm vm = vmList.get(currentIndex); // Get the VM based on currentIndex
                broker.bindCloudletToVm(cloudlet.getCloudletId(), vm.getId());
                currentIndex = (currentIndex + 1) % vmList.size(); // Move to the next VM in circular order

                // Update totalDataTransferred
                totalDataTransferred += cloudlet.getCloudletFileSize() / 1024.0; // Convert to MB
            } else {
                // If data transfer threshold exceeded, reset currentIndex to 0 and continue scheduling
                currentIndex = 0;
                Vm vm = vmList.get(currentIndex);
                broker.bindCloudletToVm(cloudlet.getCloudletId(), vm.getId());
                currentIndex++; // Move to the next VM
                totalDataTransferred = cloudlet.getCloudletFileSize() / 1024.0; // Reset totalDataTransferred
            }
        }
    }



    private static void  calculateMetrics(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
				+ "Data center ID" + indent + "VM ID" + indent + "Time" + indent
				+ "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
				Log.print("SUCCESS");

				Log.printLine(indent + indent + cloudlet.getResourceId()
						+ indent + indent + indent + cloudlet.getVmId()
						+ indent + indent
						+ dft.format(cloudlet.getActualCPUTime()) + indent
						+ indent + dft.format(cloudlet.getExecStartTime())
						+ indent + indent
						+ dft.format(cloudlet.getFinishTime()));
			}
		}
	}
}
