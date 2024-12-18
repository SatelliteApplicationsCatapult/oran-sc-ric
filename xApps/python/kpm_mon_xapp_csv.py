#!/usr/bin/env python3

import argparse
import signal
import csv
import logging
from datetime import datetime
from lib.xAppBase import xAppBase


class MyXapp(xAppBase):
    def __init__(self, config, http_server_port, rmr_port):
        super(MyXapp, self).__init__(config, http_server_port, rmr_port, output_csv)
        self.output_csv = output_csv
        self.csv_headers = ["Timestamp", "UE_ID"]
        self.init_csv_file()

    def init_csv_file(self):
        """Initialize the CSV file with headers."""
        try:
            with open(self.output_csv, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(self.csv_headers)
        except IOError as e:
            logging.error("Failed to initialize CSV file: %s", e)

    def update_csv_headers(self, new_metrics):
        """Update the CSV headers if new metrics are found."""
        try:
            with open(self.output_csv, mode='r', newline='') as file:
                reader = csv.reader(file)
                existing_headers = next(reader)

            additional_headers = [metric for metric in new_metrics if metric not in existing_headers]

            if additional_headers:
                self.csv_headers.extend(additional_headers)
                with open(self.output_csv, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(self.csv_headers)
        except FileNotFoundError:
            logging.warning("CSV file not found. Initializing a new one.")
            self.init_csv_file()
        except IOError as e:
            logging.error("Failed to update CSV headers: %s", e)

    def save_to_csv(self, timestamp, ue_id, metrics):
        """Append a row of measurement data to the CSV file."""
        row = [timestamp, ue_id]
        for header in self.csv_headers[2:]:  # Skip "Timestamp" and "UE_ID"
            value = metrics.get(header, [None])
            row.append(value[0] if isinstance(value, list) else value)

        try:
            with open(self.output_csv, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(row)
        except IOError as e:
            logging.error("Failed to write to CSV file: %s", e)

    def my_subscription_callback(self, e2_agent_id, subscription_id, indication_hdr, indication_msg, kpm_report_style, ue_id):
        if kpm_report_style == 2:
            print("\nRIC Indication Received from {} for Subscription ID: {}, KPM Report Style: {}, UE ID: {}".format(e2_agent_id, subscription_id, kpm_report_style, ue_id))
        else:
            print("\nRIC Indication Received from {} for Subscription ID: {}, KPM Report Style: {}".format(e2_agent_id, subscription_id, kpm_report_style))

        indication_hdr = self.e2sm_kpm.extract_hdr_info(indication_hdr)
        meas_data = self.e2sm_kpm.extract_meas_data(indication_msg)
        timestamp = indication_hdr['colletStartTime']

        print("E2SM_KPM RIC Indication Content:")
        print("-ColletStartTime: ", timestamp)
        print("-Measurements Data:")

        granulPeriod = meas_data.get("granulPeriod", None)
        if granulPeriod is not None:
            print("-granulPeriod: {}".format(granulPeriod))

        if kpm_report_style in [1,2]:
            for metric_name, value in meas_data["measData"].items():
                print("--Metric: {}, Value: {}".format(metric_name, value))

                # Update CSV headers and save data
                self.update_csv_headers(meas_data["measData"].keys())
                self.save_to_csv(timestamp, meas_data["measData"])

        else:
            for ue_id, ue_meas_data in meas_data["ueMeasData"].items():
                print("--UE_id: {}".format(ue_id))
                granulPeriod = ue_meas_data.get("granulPeriod", None)
                if granulPeriod is not None:
                    print("---granulPeriod: {}".format(granulPeriod))

                for metric_name, value in ue_meas_data["measData"].items():
                    print("---Metric: {}, Value: {}".format(metric_name, value))

                # Update CSV headers and save data
                self.update_csv_headers(ue_meas_data["measData"].keys())
                self.save_to_csv(timestamp, ue_id, ue_meas_data["measData"])


    # Mark the function as xApp start function using xAppBase.start_function decorator.
    # It is required to start the internal msg receive loop.
    @xAppBase.start_function
    def start(self, e2_node_id, kpm_report_style, ue_ids, metric_names):
        report_period = 1000
        granul_period = 1000

        # use always the same subscription callback, but bind kpm_report_style parameter
        subscription_callback = lambda agent, sub, hdr, msg: self.my_subscription_callback(agent, sub, hdr, msg, kpm_report_style, None)

        if (kpm_report_style == 1):
            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, metrics: {}".format(e2_node_id, kpm_report_style, metric_names))
            self.e2sm_kpm.subscribe_report_service_style_1(e2_node_id, report_period, metric_names, granul_period, subscription_callback)

        elif (kpm_report_style == 2):
            # need to bind also UE_ID to callback as it is not present in the RIC indication in the case of E2SM KPM Report Style 2
            subscription_callback = lambda agent, sub, hdr, msg: self.my_subscription_callback(agent, sub, hdr, msg, kpm_report_style, ue_ids[0])
            
            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, UE_id: {}, metrics: {}".format(e2_node_id, kpm_report_style, ue_ids[0], metric_names))
            self.e2sm_kpm.subscribe_report_service_style_2(e2_node_id, report_period, ue_ids[0], metric_names, granul_period, subscription_callback)

        elif (kpm_report_style == 3):
            if (len(metric_names) > 1):
                metric_names = metric_names[0]
                print("INFO: Currently only 1 metric can be requested in E2SM-KPM Report Style 3, selected metric: {}".format(metric_names))
            # TODO: currently only dummy condition that is always satisfied, useful to get IDs of all connected UEs
            # example matching UE condition: ul-rSRP < 1000
            matchingConds = [{'matchingCondChoice': ('testCondInfo', {'testType': ('ul-rSRP', 'true'), 'testExpr': 'lessthan', 'testValue': ('valueInt', 1000)})}]

            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, metrics: {}".format(e2_node_id, kpm_report_style, metric_names))
            self.e2sm_kpm.subscribe_report_service_style_3(e2_node_id, report_period, matchingConds, metric_names, granul_period, subscription_callback)

        elif (kpm_report_style == 4):
            # TODO: currently only dummy condition that is always satisfied, useful to get IDs of all connected UEs
            # example matching UE condition: ul-rSRP < 1000
            matchingUeConds = [{'testCondInfo': {'testType': ('ul-rSRP', 'true'), 'testExpr': 'lessthan', 'testValue': ('valueInt', 1000)}}]
            
            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, metrics: {}".format(e2_node_id, kpm_report_style, metric_names))
            self.e2sm_kpm.subscribe_report_service_style_4(e2_node_id, report_period, matchingUeConds, metric_names, granul_period, subscription_callback)

        elif (kpm_report_style == 5):
            if (len(ue_ids) < 2):
                dummyUeId = ue_ids[0] + 1
                ue_ids.append(dummyUeId)
                print("INFO: Subscription for E2SM_KPM Report Service Style 5 requires at least two UE IDs -> add dummy UeID: {}".format(dummyUeId))

            print("Subscribe to E2 node ID: {}, RAN func: e2sm_kpm, Report Style: {}, UE_ids: {}, metrics: {}".format(e2_node_id, kpm_report_style, ue_ids, metric_names))
            self.e2sm_kpm.subscribe_report_service_style_5(e2_node_id, report_period, ue_ids, metric_names, granul_period, subscription_callback)

        else:
            print("INFO: Subscription for E2SM_KPM Report Service Style {} is not supported".format(kpm_report_style))
            exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='My example xApp')
    parser.add_argument("--config", type=str, default='', help="xApp config file path")
    parser.add_argument("--http_server_port", type=int, default=8092, help="HTTP server listen port")
    parser.add_argument("--rmr_port", type=int, default=4562, help="RMR port")
    parser.add_argument("--e2_node_id", type=str, default='gnbd_001_001_00019b_0', help="E2 Node ID")
    parser.add_argument("--ran_func_id", type=int, default=2, help="RAN function ID")
    parser.add_argument("--kpm_report_style", type=int, default=1, help="xApp config file path")
    parser.add_argument("--ue_ids", type=str, default='0', help="UE ID")
    parser.add_argument("--metrics", type=str, default='DRB.UEThpUl,DRB.UEThpDl', help="Metrics name as comma-separated string")
    parser.add_argument("--output_csv", type=str, default=f"measurement_data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S'}.csv", help="Output path for CSV file")

    args = parser.parse_args()
    config = args.config
    e2_node_id = args.e2_node_id # TODO: get available E2 nodes from SubMgr, now the id has to be given.
    ran_func_id = args.ran_func_id # TODO: get available E2 nodes from SubMgr, now the id has to be given.
    ue_ids = list(map(int, args.ue_ids.split(","))) # Note: the UE id has to exist at E2 node!
    kpm_report_style = args.kpm_report_style
    metrics = args.metrics.split(",")

    # Create MyXapp.
    myXapp = MyXapp(config, args.http_server_port, args.rmr_port, args.output_csv)
    myXapp.e2sm_kpm.set_ran_func_id(ran_func_id)

    # Connect exit signals.
    signal.signal(signal.SIGQUIT, myXapp.signal_handler)
    signal.signal(signal.SIGTERM, myXapp.signal_handler)
    signal.signal(signal.SIGINT, myXapp.signal_handler)

    # Start xApp.
    myXapp.start(e2_node_id, kpm_report_style, ue_ids, metrics)
    # Note: xApp will unsubscribe all active subscriptions at exit.
