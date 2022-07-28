package com.pwb.pulsar.functions;

public class deviceHealthRecord {
	private String deviceID;
	private String batteryState;

	public deviceHealthRecord(String deviceID, String batteryState) {
		super();
		this.deviceID = deviceID;
		this.batteryState = batteryState;
	}
	public String getDeviceID() {
		return deviceID;
	}
	public void setDeviceID(String deviceID) {
		this.deviceID = deviceID;
	}
	public String getBatteryState() {
		return batteryState;
	}
	public void setBatteryState(String batteryState) {
		this.batteryState = batteryState;
	}

}
