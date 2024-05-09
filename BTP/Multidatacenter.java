package org.cloudbus.cloudsim.examples;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;


import java.text.DecimalFormat;
import java.util.*;


public class Multidatacenter{
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
    private static final double CPU_THRESHOLD = 0.8;
    private static final double CPU_HISTORY_WINDOW = 10;
    private static final int NUM_DATACENTERS = 4;
    private static Map<Vm, List<Double>> cpuUsageHistory = new HashMap<>();
    private static List<Datacenter> datacenterList;
    private static List<Cloudlet> cloudletList;
    private static List<Vm> vmList;

    public static void main(String[] args) {
        Log.printLine("Starting Dynamic Resource Allocation Simulation");

        try {
            int numUser = 1;
            boolean traceFlag = false;
            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter NO. of Vm's: ");
            int NUM_VMS = scanner.nextInt();
            System.out.print("Enter NO. of Cloudlets: ");
            int NUM_CLOUDLETS= scanner.nextInt();
            
            CloudSim.init(numUser, null, traceFlag);
            Random rand = new Random();
            datacenterList = new ArrayList<>();
            for (int i = 0; i < NUM_DATACENTERS; i++) {
                Datacenter datacenter = createDatacenter("Datacenter_" + i);
                datacenterList.add(datacenter);
            }
            List<Integer> Cloundletlength = new ArrayList<>();;
            List<Integer> CloundletFileSize =new ArrayList<>();;
            List<Integer> CloundletoutputSize= new ArrayList<>();;
            for(int i=1;i<=NUM_CLOUDLETS;i++) {
            	Cloundletlength.add((i%38)*850  + 1000);
            	CloundletFileSize.add((i*450)%600);
            	CloundletoutputSize.add((i*450)%600);
            }
            DatacenterBroker broker = createBroker();
            int brokerId = broker.getId();

            vmList = new ArrayList<>();
            for (int i = 0; i < NUM_VMS; i++) {
                Vm vm = new Vm(i, brokerId, HOST_MIPS, VM_PES_NUMBER, VM_RAM, VM_BW, VM_SIZE, VM_VMM, new CloudletSchedulerTimeShared());
                vmList.add(vm);
            }

            broker.submitVmList(vmList);

            cloudletList = new ArrayList<>();
            for (int i = 0; i < NUM_CLOUDLETS; i++) {
            	double arrivalTime = rand.nextInt(24);
            	 Cloudlet cloudlet = new Cloudlet(i, Cloundletlength.get(i), VM_PES_NUMBER, CloundletFileSize.get(i), CloundletoutputSize.get(i),
                        new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull());
                
            	cloudlet.setUserId(brokerId);
                cloudletList.add(cloudlet);
                
            }

            broker.submitCloudletList(cloudletList);

            for (Datacenter dc : datacenterList) {
               throttledVmAllocation(broker, dc);
               bindCloudletsToVmsInDynamicRR(broker, dc);
            }

            CloudSim.startSimulation();

            List<Cloudlet> newList = broker.getCloudletReceivedList();

            CloudSim.stopSimulation();

            calculateMetrics(newList);
            calculateMetrics1(newList);
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

    private static void throttledVmAllocation(DatacenterBroker broker, Datacenter datacenter) {
        List<Vm> vmList = datacenter.getVmList();
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
    private static void bindCloudletsToVmsInSJF(DatacenterBroker broker, Datacenter datacenter) {
        List<Cloudlet> cloudletList = broker.getCloudletSubmittedList();
        cloudletList.sort(Comparator.comparingLong(Cloudlet::getCloudletLength));

        for (Cloudlet cloudlet : cloudletList) {
            Vm minExecutionTimeVm = null;
            double minExecutionTime = Double.MAX_VALUE;

            for (Vm vm : datacenter.getVmList()) {
                double executionTime = calculateEstimatedExecutionTime(cloudlet, vm);
                if (executionTime < minExecutionTime) {
                    minExecutionTime = executionTime;
                    minExecutionTimeVm = vm;
                }
            }

            if (minExecutionTimeVm != null) {
                broker.bindCloudletToVm(cloudlet.getCloudletId(), minExecutionTimeVm.getId());
            }
        }
    }

    private static double calculateEstimatedExecutionTime(Cloudlet cloudlet, Vm vm) {
        return cloudlet.getCloudletLength() / (double) vm.getMips();
    }


    private static void bindCloudletsToVmsInDynamicRR(DatacenterBroker broker, Datacenter datacenter) {
        List<Cloudlet> cloudletList = broker.getCloudletSubmittedList();

        int currentIndex = 0; // Current index to keep track of VM selection

        // Track data transfer status
        double totalDataTransferred = 0;
        double maxDataTransferThreshold = 1000; // Adjust this threshold as needed

        for (Cloudlet cloudlet : cloudletList) {
            // Check data transfer status before scheduling the cloudlet
            if (totalDataTransferred < maxDataTransferThreshold) {
                Vm vm = datacenter.getVmList().get(currentIndex); // Get the VM based on currentIndex
                broker.bindCloudletToVm(cloudlet.getCloudletId(), vm.getId());
                currentIndex = (currentIndex + 1) % datacenter.getVmList().size(); // Move to the next VM in circular order

                // Update totalDataTransferred
                totalDataTransferred += cloudlet.getCloudletFileSize() / 1024.0; // Convert to MB
            } else {
                // If data transfer threshold exceeded, reset currentIndex to 0 and continue scheduling
                currentIndex = 0;
                Vm vm = datacenter.getVmList().get(currentIndex);
                broker.bindCloudletToVm(cloudlet.getCloudletId(), vm.getId());
                currentIndex++; // Move to the next VM
                totalDataTransferred = cloudlet.getCloudletFileSize() / 1024.0; // Reset totalDataTransferred
            }
        }
    }

    private static void calculateMetrics(List<Cloudlet> list) {
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
   

    
    private static void calculateMetrics1(List<Cloudlet> cloudletList) {
        double totalDataTransferred = 0;
        double totalExecutionTime = 0;

        for (Cloudlet cloudlet : cloudletList) {
            totalDataTransferred += cloudlet.getCloudletFileSize() / 1024.0; // Convert to MB
            totalExecutionTime += cloudlet.getActualCPUTime();
        }

        Log.printLine();
        Log.printLine("========== METRICS ==========");
        Log.printLine("Total amount of data forwarded: " + totalDataTransferred + " MB");
        Log.printLine("Total execution time: " + totalExecutionTime + " seconds");
    }
    
}

