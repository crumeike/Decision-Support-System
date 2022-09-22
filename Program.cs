/*In order for this program to run, ensure you use the same syncMeasurement.json format 
that is indexer this program directory as your publish BOD message. If you are using 
the windows form, you can copy the json content from the file and paste in the BOD text box.

*/

using System;
using System.Collections.Generic;
using System.Text;
using System.Globalization;
using ISBM20ClientAdapter;
using ISBM20ClientAdapter.ResponseType;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading;

namespace Decision_Support_System
{
    class Program
    {
        static string _hostName = "";
        static string _channelId = "";
        static string _subscribeTopic = "";
        static string _publishTopic = "";
        static string _SubscribeSessionId = "";
        static string _PublishSessionId = "";

        static Boolean _authentication;
        static string _username = "";
        static string _password = "";

        static string _value;
        static string _measurementLocation = "";
        static string _unitOfmeasure = "";

        static ProviderPublicationService _myProviderPublicationService = new ProviderPublicationService();

        static ConsumerPublicationServices _myConsumerPublicationService = new ConsumerPublicationServices();
        static void Main(string[] args)
        {
            //Read application configurations from Configs.json 
            string filename = "Configs_subscribe.json";
            SetConfigurations(filename);

            //Open a Consumer Publication Session
            OpenSubscriptionSessionResponse myOpenSubscriptionSessionResponse = _myConsumerPublicationService.OpenSubscriptionSession(_hostName, _channelId, _subscribeTopic);
            Console.WriteLine("Host Address " + _hostName);
            Console.WriteLine("Channel Id " + _channelId);
            if (myOpenSubscriptionSessionResponse.StatusCode == 201)
            {
                //SessionID is stored in a class level valuable for repeatedly used in every BPD subscription operation.
                _SubscribeSessionId = myOpenSubscriptionSessionResponse.SessionID;
                Console.WriteLine("Subcription Session " + _SubscribeSessionId + "\n");
            }
            else
            {
                Console.WriteLine(myOpenSubscriptionSessionResponse.StatusCode + " " + myOpenSubscriptionSessionResponse.ISBMHTTPResponse);
                Console.WriteLine("Please check configurations!!");
            }

            Thread.Sleep(1000);

            filename = "Configs_publish.json";
            SetConfigurations(filename);
            //Open a Provider Publication Session
            OpenPublicationSessionResponse myOpenPublicationSessionResponse = _myProviderPublicationService.OpenPublicationSession(_hostName, _channelId);
            Console.WriteLine("\nHost Address " + _hostName);
            Console.WriteLine("Channel Id " + _channelId);
            if (myOpenPublicationSessionResponse.StatusCode == 201)
            {
                //SessionID is stored in a class level valuable for repeatedly used in every BPD post publication.
                _PublishSessionId = myOpenPublicationSessionResponse.SessionID;
                Console.WriteLine("Publication Session " + _PublishSessionId);
                Console.WriteLine("Decision Support System is running!!\n");
            }
            else
            {
                Console.WriteLine(myOpenPublicationSessionResponse.StatusCode + " " + myOpenPublicationSessionResponse.ISBMHTTPResponse);
                Console.WriteLine("Please check configurations!!");
            }

            Thread.Sleep(1000);

            int count = 0;
            while (true)
            {
                /*Read publication in queue and remove after reading it.
                At the same time, get sensor data: water level, unit and location information.*/
                GetSensorData();

                //Apply Analytics
                float WaterLevel = float.Parse(_value, CultureInfo.InvariantCulture.NumberFormat);// convert string to float value
                float Threshold_1 = 12, Threshold_2 = 36, Threshold_3 = 72;
                if ((WaterLevel >= Threshold_1) && (WaterLevel < Threshold_2))
                {
                    //publish advisory 1 : WARNING
                    string Warning = "SyncAssessments.json";
                    string Advisory = System.IO.File.ReadAllText(Warning);
                    PublishBOD(Advisory);
                }
                else if ((WaterLevel >= Threshold_2) && (WaterLevel < Threshold_3))
                {
                    // publish advisory 2 : ALERT
                    string Alert = "SyncAdvisories.json";
                    string Advisory = System.IO.File.ReadAllText(Alert);
                    PublishBOD(Advisory);
                }
                else if (WaterLevel >= Threshold_3)
                {
                    // publish advisory 3: REQUEST FOR WORK
                    string RequestforWork = "ProcessRequestsForWork.json";
                    string Advisory = System.IO.File.ReadAllText(RequestforWork);
                    PublishBOD(Advisory);
                }
                count++;
                //Console.WriteLine("count: " + count);
                if (count == 50)
                {
                    break; //End program after 50 messages have been published.
                }
            }

        }
        private static void SetConfigurations(string filename)
        {
            string JsonFromFile = System.IO.File.ReadAllText(filename);

            JObject JObjectConfigs = JObject.Parse(JsonFromFile);
            _hostName = JObjectConfigs["hostName"].ToString();
            _channelId = JObjectConfigs["channelId"].ToString();
            if (filename == "Configs_publish.json")
            {
                _publishTopic = JObjectConfigs["topic"].ToString();
            }
            if (filename == "Configs_subscribe.json")
            {
                _subscribeTopic = JObjectConfigs["topic"].ToString();
            }

            _authentication = (Boolean)JObjectConfigs["authentication"];
            if (_authentication == true)
            {
                _username = JObjectConfigs["userName"].ToString();
                _password = JObjectConfigs["password"].ToString();
            }

        }
        private static void GetSensorData()
        {
            //Read a Publication 
            ReadPublicationResponse myReadPublicationResponse = _myConsumerPublicationService.ReadPublication(_hostName, _SubscribeSessionId);

            // check if there is a message in the queue
            if (myReadPublicationResponse.StatusCode == 200)
            {
                //Read sensor data from syncMeasurements.json 
                JObject JObjectConfigs = JObject.Parse(myReadPublicationResponse.MessageContent);
                _value = JObjectConfigs["syncMeasurements"]["dataArea"]["measurements"][0]["measurement"][0]["data"]["measure"]["value"].ToString();
                _unitOfmeasure = JObjectConfigs["syncMeasurements"]["dataArea"]["measurements"][0]["measurement"][0]["data"]["measure"]["unitOfMeasure"]["shortName"].ToString();
                _measurementLocation = JObjectConfigs["syncMeasurements"]["dataArea"]["measurements"][0]["measurementLocation"]["shortName"].ToString();

                Console.WriteLine("\nThe water level is: " + _value + " " + _unitOfmeasure);
                Console.WriteLine("Flood location: " + _measurementLocation);
                Console.WriteLine("Time: " + DateTime.Now.ToString("HH:mm:ss tt"));

                //Remove publication from queue
                RemovePublicationResponse myRemovePublicationResponse = _myConsumerPublicationService.RemovePublication(_hostName, _SubscribeSessionId);
            }

            else
            {
                Console.WriteLine(myReadPublicationResponse.ISBMHTTPResponse);

                Thread.Sleep(2000); // Sleep for 2 seconds

                GetSensorData();
            }

        }
        private static void PublishBOD(string bodMessage)
        {
            //Post Publication - BOD message
            PostPublicationResponse myPostPublicationResponse = _myProviderPublicationService.PostPublication(_hostName, _PublishSessionId, _publishTopic, bodMessage);

            string MessageId = "";
            if (myPostPublicationResponse.StatusCode == 201)
            {
                MessageId = myPostPublicationResponse.MessageID;
                Console.WriteLine("Message " + MessageId + " has been pusblished!!\n");
                //Console.WriteLine("\nAdvisory: \n" + bodMessage);
            }
            else
            {
                Console.WriteLine(myPostPublicationResponse.StatusCode + " " + myPostPublicationResponse.ISBMHTTPResponse);
            }

        }
    }
}
