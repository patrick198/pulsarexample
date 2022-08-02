package com.pwb.pulsar.client;

public class deviceHealthRecord {
	private String deviceID;
	private String batteryState;
	private String deviceTemp;
	private String deviceLocation;

	public deviceHealthRecord(String deviceID, String deviceTemp, String batteryState, String deviceLocation) {
		super();
		this.deviceID = deviceID;
		this.deviceTemp = deviceTemp;
		this.batteryState = batteryState;
		this.deviceLocation = deviceLocation;
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
	public String getDeviceTemp() {
		return deviceTemp;
	}
	public void setDeviceTemp(String deviceTemp) {
		this.deviceTemp = deviceTemp;
	}
	public String getDeviceLocation() {
		return deviceLocation;
	}
	public void setDeviceLocation(String deviceLocation) {
		this.deviceLocation = deviceLocation;
	}

}
