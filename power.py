HOST = "127.0.0.1"
PORT = 4223
UID_DC = "23iX" # Change XYZ to the UID of your Voltage/Current Bricklet 2.0
UID_AC = "Uos"

from tinkerforge.ip_connection import IPConnection
from tinkerforge.bricklet_voltage_current_v2 import BrickletVoltageCurrentV2
from tinkerforge.bricklet_energy_monitor import BrickletEnergyMonitor
import time

ipcon = IPConnection() # Create IP connection
ipcon.connect(HOST, PORT) # Connect to brickd
# ipcon.set_timeout(10)
pw = BrickletVoltageCurrentV2(UID_DC, ipcon) # Create device object
# Callback function for current callback
em = BrickletEnergyMonitor(UID_AC, ipcon) # Create device object

def cb_current(current):
    print("Current: " + str(current/1000.0) + " A")

# Callback function for power callback
def cb_power(power):
    print("power: " + str(power/1000.0) + " W")

    
if __name__ == "__main__":
    print("Testing of the Tinkerforge is working correctly")
    # vc = BrickletVoltageCurrentV2(UID, ipcon) # Create device object

    # ipcon.connect(HOST, PORT) # Connect to brickd
    # Don't use device before ipcon is connected

    # Register current callback to function cb_current
    # vc.register_callback(vc.CALLBACK_CURRENT, cb_current)

    # Set period for current callback to 1s (1000ms) without a threshold
    # vc.set_current_callback_configuration(500, False, "x", 0, 0)

 # Register power callback to function cb_power
    # pw.register_callback(pw.CALLBACK_POWER, cb_power)

    # Configure threshold for power "greater than 10 W"
    # with a debounce period of 1s (1000ms)
    # pw.set_power_callback_configuration(1000, False, ">", 1*1000, 0)
    em.reset_energy()
    while True:
        print("Jetson power: " + str(pw.get_power()/1000.0) + " W")
        voltage, current, energy, real_power, apparent_power, reactive_power, power_factor, frequency = em.get_energy_data()
        print("MEC Power: " + str(real_power/100.0) + " W")
        print("MEC Energy: " + str(energy/100.0) + " Wh")
        print("MEC Energy in J: " + str(energy*36) + " J")
        
        time.sleep(0.5)
    
    input("Press key to exit\n") # Use raw_input() in Python 2
    ipcon.disconnect()
